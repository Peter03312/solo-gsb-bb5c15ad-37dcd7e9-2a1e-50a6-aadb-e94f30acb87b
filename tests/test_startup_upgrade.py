"""端到端自测：启动初始化的一次性可恢复升级（建表 / 旧库补列 / 索引 / 版本 0 基线）。

覆盖：
  * 旧库（缺列、无版本表、无升级标记）一次性升级：补列、建表、索引、每份旧样本
    恰好一个 baseline 版本 0，基线仅反映升级时有效链与核定状态；
  * 升级幂等：重跑零变化；
  * 任一步失败整体回滚：不留部分升级结果（无新表/新列/基线/标记），原始数据不变；
    恢复后重跑成功（可恢复）；
  * 升级失败时 lifespan 启动失败：失败进程不对外服务；
  * 并发启动同一持久库：每份旧样本只构建一次基线，后到进程沿用已完成状态；
  * 已有版本、案卷、复核记录不被重写（模拟上一版代码的库重跑完整升级）；
  * 升级完成后样本 / 版本 / 案卷 HTTP 查询可调用且响应含义不变。

运行：python -m tests.test_startup_upgrade
"""
import asyncio
import json
import os
import sqlite3
import tempfile
import threading
from pathlib import Path

_tmp = tempfile.mkdtemp()
os.environ["DB_PATH"] = os.path.join(_tmp, "main.db")
os.environ["SEED_ON_START"] = "false"

from fastapi.testclient import TestClient  # noqa: E402

import app.versions as versions_mod  # noqa: E402
from app import config  # noqa: E402
from app.db import SCHEMA_VERSION, db_session, init_db  # noqa: E402
from app.main import app, lifespan  # noqa: E402

API = "/api/v1"
PASS, FAIL = 0, 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name} {detail}")


def use_db(path: str) -> None:
    config.DB_PATH = Path(path)


# 旧库（版本化之前）：custody_event 无 superseded_by/replaces_event_id/batch_id，
# 无 sample_version / schema_meta 等后续各表
LEGACY_SCHEMA = """
CREATE TABLE sample (
    id INTEGER PRIMARY KEY AUTOINCREMENT, sample_code TEXT NOT NULL UNIQUE,
    collected_at TEXT NOT NULL, collector TEXT NOT NULL, seal_no TEXT NOT NULL,
    content_hash TEXT NOT NULL, approved_storage_at TEXT, approved_by TEXT,
    approve_note TEXT, created_at TEXT NOT NULL);
CREATE TABLE custody_event (
    id INTEGER PRIMARY KEY AUTOINCREMENT, sample_id INTEGER NOT NULL,
    event_id TEXT NOT NULL UNIQUE, event_type TEXT NOT NULL, occurred_at TEXT NOT NULL,
    payload_json TEXT NOT NULL, content_hash TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE event_correction (
    id INTEGER PRIMARY KEY AUTOINCREMENT, correction_id TEXT NOT NULL UNIQUE,
    sample_id INTEGER NOT NULL, target_event_id TEXT NOT NULL, action TEXT NOT NULL,
    operator TEXT NOT NULL, reason TEXT NOT NULL, new_event_id TEXT,
    new_event_row_id INTEGER, request_hash TEXT NOT NULL, created_at TEXT NOT NULL);
"""


def _insert_sample(con, code, collector, seal, approved=None):
    con.execute(
        "INSERT INTO sample (sample_code, collected_at, collector, seal_no, content_hash,"
        " approved_storage_at, approved_by, approve_note, created_at)"
        " VALUES (?,?,?,?,?,?,?,?,?)",
        (code, "2026-01-01T00:00:00Z", collector, seal, "h-" + code,
         approved[0] if approved else None, approved[1] if approved else None,
         approved[2] if approved else None, "2026-01-01T00:00:00Z"),
    )
    return con.execute("SELECT id FROM sample WHERE sample_code = ?", (code,)).fetchone()[0]


def _insert_event(con, sample_id, event_id, etype, occurred_at, payload):
    con.execute(
        "INSERT INTO custody_event (sample_id, event_id, event_type, occurred_at,"
        " payload_json, content_hash, created_at) VALUES (?,?,?,?,?,?,?)",
        (sample_id, event_id, etype, occurred_at,
         json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
         "h-" + event_id, occurred_at),
    )


def _valid_chain(con, sid, prefix, collector, receiver, seal):
    """写入一条完整有效链：移交 → 签收 → 封签检查 → 入库。"""
    _insert_event(con, sid, f"{prefix}-T1", "transfer", "2026-01-01T02:00:00Z",
                  {"transfer_no": f"TR-{prefix}", "from_custodian": collector, "to_custodian": receiver})
    _insert_event(con, sid, f"{prefix}-R1", "receive", "2026-01-01T04:00:00Z",
                  {"transfer_no": f"TR-{prefix}", "receiver": receiver, "seal_no": seal})
    _insert_event(con, sid, f"{prefix}-C1", "seal_check", "2026-01-01T04:30:00Z",
                  {"checked_seal_no": seal, "seal_intact": True})
    _insert_event(con, sid, f"{prefix}-S1", "storage", "2026-01-01T05:00:00Z",
                  {"custodian": "实验室", "storage_location": "冷库A-01"})


def build_legacy_db(path: str) -> set:
    """三份旧样本：OLD-A 有效链未核定、OLD-B 有效链已核定、OLD-C 仅登记。返回原始表集合。"""
    con = sqlite3.connect(path)
    con.executescript(LEGACY_SCHEMA)
    a = _insert_sample(con, "OLD-A", "甲", "SL-A")
    _valid_chain(con, a, "A", "甲", "乙", "SL-A")
    b = _insert_sample(con, "OLD-B", "乙", "SL-B", approved=("2026-03-01T00:00:00Z", "主管", "ok"))
    _valid_chain(con, b, "B", "乙", "丁", "SL-B")
    _insert_sample(con, "OLD-C", "丙", "SL-C")
    con.commit()
    tables = table_names(con)
    con.close()
    return tables


def table_names(con) -> set:
    return {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def column_names(con, table) -> set:
    return {r[1] for r in con.execute(f"PRAGMA table_info({table})")}


def index_names(con) -> set:
    return {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='index'")}


# ---------------------------------------------------------------- 1. 旧库一次性升级
print("1) 旧库（缺列、无版本表）一次性升级：补列 / 建表 / 索引 / 版本 0 基线")
legacy_db = os.path.join(_tmp, "legacy.db")
build_legacy_db(legacy_db)
use_db(legacy_db)
init_db()
with db_session() as conn:
    cols = column_names(conn, "custody_event")
    check("旧库补列完成（superseded_by/replaces_event_id/batch_id）",
          {"superseded_by", "replaces_event_id", "batch_id"} <= cols, str(cols))
    check("旧意见身份列补齐（review.reviewer_person_id）",
          "reviewer_person_id" in column_names(conn, "review"))
    tables = table_names(conn)
    check("版本表与升级标记表已建", {"sample_version", "schema_meta"} <= tables, str(tables))
    check("后续各业务表已建（案卷/复核/人员/测温/校准/处置）",
          {"dossier", "dossier_item", "review_case", "review", "review_opinion",
           "person", "person_alias", "sample_temp_config", "temp_reading",
           "device_calibration", "temp_anomaly_disposition"} <= tables, str(tables))
    idx = index_names(conn)
    check("迁移后索引已建",
          {"idx_event_effective", "idx_correction_sample", "idx_event_batch",
           "idx_version_sample"} <= idx, str(idx))
    marker = conn.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()
    check("升级标记已写入且为当前结构版本",
          marker is not None and marker[0] == SCHEMA_VERSION, str(marker))
    rows = conn.execute(
        "SELECT s.sample_code, v.version, v.change_type FROM sample_version v"
        " JOIN sample s ON s.id = v.sample_id ORDER BY s.id"
    ).fetchall()
    check("每份旧样本恰好一个版本 0 基线",
          [(r[0], r[1], r[2]) for r in rows]
          == [("OLD-A", 0, "baseline"), ("OLD-B", 0, "baseline"), ("OLD-C", 0, "baseline")],
          str(rows))

    from app.versions import get_version

    snap_a = get_version(conn, "OLD-A", 0)
    check("OLD-A 基线反映升级时有效链（valid / 保管人实验室 / 4 事件）",
          snap_a["status"] == "valid" and snap_a["current_custodian"] == "实验室"
          and snap_a["event_count"] == 4, str(snap_a["status"]))
    check("OLD-A 基线未核定", snap_a["approval"] is None and snap_a["approved_storage_at"] is None)
    snap_b = get_version(conn, "OLD-B", 0)
    check("OLD-B 基线保留升级时核定状态",
          snap_b["approved_by"] == "主管" and snap_b["approved_storage_at"] == "2026-03-01T00:00:00Z"
          and snap_b["approval"] == {"approved_at": "2026-03-01T00:00:00Z",
                                     "approver": "主管", "note": "ok"}, str(snap_b["approval"]))
    snap_c = get_version(conn, "OLD-C", 0)
    check("OLD-C 基线为登记后待补证（不推断更早历史）",
          snap_c["status"] == "pending" and snap_c["event_count"] == 0
          and snap_c["timeline"] == [], str(snap_c["status"]))

# ---------------------------------------------------------------- 2. 升级幂等
print("2) 升级幂等：重跑零变化")
_IDEMPOTENT_TABLES = ("sample", "custody_event", "sample_version", "schema_meta")


def dump_by_rowid(conn, tables):
    return {t: [tuple(r) for r in conn.execute(f"SELECT * FROM {t} ORDER BY rowid").fetchall()]
            for t in tables}


with db_session() as conn:
    before = dump_by_rowid(conn, _IDEMPOTENT_TABLES)
init_db()
init_db()
with db_session() as conn:
    after = dump_by_rowid(conn, _IDEMPOTENT_TABLES)
check("重跑后样本/事件/版本/标记逐行一致", before == after)
with db_session() as conn:
    n = conn.execute("SELECT COUNT(*) FROM sample_version").fetchone()[0]
check("重跑不再构建基线（仍 3 条）", n == 3, str(n))

# ---------------------------------------------------------------- 3. 失败原子性与可恢复
print("3) 升级任一步失败：整体回滚不留部分结果，恢复后重跑成功")
fail_db = os.path.join(_tmp, "fail.db")
legacy_tables = build_legacy_db(fail_db)
use_db(fail_db)
orig_ensure = versions_mod.ensure_version_baselines


def _boom(conn):
    raise RuntimeError("模拟基线构建中途失败")


versions_mod.ensure_version_baselines = _boom
try:
    init_db()
    check("升级失败时 init_db 抛出异常", False, "未抛出")
except RuntimeError as exc:
    check("升级失败时 init_db 抛出异常", "模拟基线构建中途失败" in str(exc), str(exc))
finally:
    versions_mod.ensure_version_baselines = orig_ensure

con = sqlite3.connect(fail_db)
con.row_factory = sqlite3.Row
tables_after_fail = table_names(con)
check("失败后不留新表（sample_version/schema_meta 等不存在）",
      tables_after_fail == legacy_tables, str(tables_after_fail))
check("失败后不留补列",
      column_names(con, "custody_event")
      == {"id", "sample_id", "event_id", "event_type", "occurred_at",
          "payload_json", "content_hash", "created_at"},
      str(column_names(con, "custody_event")))
check("失败后原始样本与事件行不变",
      con.execute("SELECT COUNT(*) FROM sample").fetchone()[0] == 3
      and con.execute("SELECT COUNT(*) FROM custody_event").fetchone()[0] == 8)
con.close()

init_db()  # 可恢复：故障排除后重跑即从头再升级
with db_session() as conn:
    n = conn.execute("SELECT COUNT(*) FROM sample_version").fetchone()[0]
    marker = conn.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()
check("恢复后重跑成功：基线补齐且标记写入",
      n == 3 and marker is not None and marker[0] == SCHEMA_VERSION)
with db_session() as conn:
    check("恢复后补列完成", "superseded_by" in column_names(conn, "custody_event"))

# ---------------------------------------------------------------- 4. 失败进程不对外服务
print("4) 升级失败时 lifespan 启动失败（失败进程不对外服务）")
fail2_db = os.path.join(_tmp, "fail2.db")
build_legacy_db(fail2_db)
use_db(fail2_db)
versions_mod.ensure_version_baselines = _boom
lifespan_failed = False
try:

    async def _run_lifespan():
        async with lifespan(app):
            pass

    asyncio.run(_run_lifespan())
except RuntimeError:
    lifespan_failed = True
finally:
    versions_mod.ensure_version_baselines = orig_ensure
check("lifespan 随升级失败而失败（启动不进入服务）", lifespan_failed)
con = sqlite3.connect(fail2_db)
check("lifespan 失败后库中同样无部分升级结果",
      table_names(con) == legacy_tables, str(table_names(con)))
con.close()
init_db()  # 恢复后该库可正常升级
with db_session() as conn:
    check("恢复后该库升级成功", conn.execute("SELECT COUNT(*) FROM sample_version").fetchone()[0] == 3)

# ---------------------------------------------------------------- 5. 并发启动
print("5) 并发启动同一持久库：每份旧样本只构建一次基线，后到进程沿用已完成状态")
race_db = os.path.join(_tmp, "race.db")
con = sqlite3.connect(race_db)
con.executescript(LEGACY_SCHEMA)
for i in range(40):
    sid = _insert_sample(con, f"RACE-{i:02d}", "采集员", f"SL-{i:02d}")
    _insert_event(con, sid, f"RACE-E-{i:02d}", "transfer", "2026-01-01T02:00:00Z",
                  {"transfer_no": f"TR-R-{i:02d}", "from_custodian": "采集员", "to_custodian": "运输员"})
con.commit()
con.close()
use_db(race_db)

barrier = threading.Barrier(2)
race_results: dict[str, str] = {}


def race_worker(name: str) -> None:
    barrier.wait()
    try:
        init_db()
        race_results[name] = "ok"
    except Exception as exc:  # noqa: BLE001
        race_results[name] = repr(exc)


t1 = threading.Thread(target=race_worker, args=("P1",))
t2 = threading.Thread(target=race_worker, args=("P2",))
t1.start()
t2.start()
t1.join()
t2.join()
check("两个并发启动进程都成功完成初始化", race_results == {"P1": "ok", "P2": "ok"}, str(race_results))
with db_session() as conn:
    per_sample = conn.execute(
        "SELECT sample_id, COUNT(*) c FROM sample_version GROUP BY sample_id"
    ).fetchall()
    check("每份旧样本只构建一次基线（40 份各 1 条）",
          len(per_sample) == 40 and all(r["c"] == 1 for r in per_sample),
          str([(r["sample_id"], r["c"]) for r in per_sample if r["c"] != 1]))
    all_v0 = conn.execute(
        "SELECT COUNT(*) FROM sample_version WHERE version = 0 AND change_type = 'baseline'"
    ).fetchone()[0]
    check("全部为版本 0 baseline 基线", all_v0 == 40, str(all_v0))
    marker = conn.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()
    check("并发升级后标记完整", marker is not None and marker[0] == SCHEMA_VERSION)
# 后到进程（第三个）启动：快路径直接沿用，不再触碰任何数据
with db_session() as conn:
    before = [tuple(r) for r in conn.execute("SELECT * FROM sample_version ORDER BY id").fetchall()]
init_db()
with db_session() as conn:
    after = [tuple(r) for r in conn.execute("SELECT * FROM sample_version ORDER BY id").fetchall()]
check("后续启动沿用已完成状态（版本行零变化）", before == after)

# ---------------------------------------------------------------- 6. 已有版本/案卷/复核记录不重写
print("6) 已有版本、案卷、复核记录不被重写（模拟上一版代码的库重跑完整升级）")
modern_db = os.path.join(_tmp, "modern.db")
use_db(modern_db)
init_db()
client = TestClient(app)


def reg(code, collector="张三", seal=None):
    return client.post(f"{API}/samples", json={
        "sample_code": code, "collected_at": "2026-09-30T01:00:00Z",
        "collector": collector, "seal_no": seal or f"SEAL-{code}"})


def ev(payload):
    return client.post(f"{API}/events", json=payload)


def valid_chain_http(code, seal=None):
    seal = seal or f"SEAL-{code}"
    for payload in (
        {"event_id": f"{code}-T1", "event_type": "transfer", "sample_code": code,
         "occurred_at": "2026-09-30T02:00:00Z", "transfer_no": f"TR-{code}",
         "from_custodian": "张三", "to_custodian": "李四"},
        {"event_id": f"{code}-R1", "event_type": "receive", "sample_code": code,
         "occurred_at": "2026-09-30T04:00:00Z", "transfer_no": f"TR-{code}",
         "receiver": "李四", "seal_no": seal},
        {"event_id": f"{code}-C1", "event_type": "seal_check", "sample_code": code,
         "occurred_at": "2026-09-30T04:30:00Z", "checked_seal_no": seal, "seal_intact": True},
        {"event_id": f"{code}-S1", "event_type": "storage", "sample_code": code,
         "occurred_at": "2026-09-30T05:00:00Z", "custodian": "实验室"},
    ):
        ev(payload)


reg("M-1")
valid_chain_http("M-1")
reg("M-2")
valid_chain_http("M-2")
# 一次替换纠错 + 一次核定，让版本/纠错/核定数据齐备
r = client.post(f"{API}/corrections", json={
    "correction_id": "M-FIX1", "sample_code": "M-1", "target_event_id": "M-1-C1",
    "action": "replace", "operator": "质控员", "reason": "检查记录更正",
    "new_event": {"event_id": "M-1-C2", "event_type": "seal_check",
                  "occurred_at": "2026-09-30T04:30:00Z",
                  "checked_seal_no": "SEAL-M-1", "seal_intact": True}})
check("准备数据：替换纠错 201", r.status_code == 201, r.text)
r = client.post(f"{API}/samples/M-1/approve", json={"approver": "质量主管", "note": "准予入库"})
check("准备数据：核定 200", r.status_code == 200, r.text)
r = client.post(f"{API}/dossiers", json={
    "dossier_id": "D-UPG", "items": [{"sample_code": "M-1", "version": 5}, {"sample_code": "M-2"}]})
check("准备数据：案卷封存 201", r.status_code == 201, r.text)
digest = r.json()["dossier"]["digest"]
for rno, reviewer in (("REV-U1", "QC-甲"), ("REV-U2", "QC-乙")):
    r = client.post(f"{API}/dossiers/D-UPG/reviews", json={
        "review_no": rno, "reviewer": reviewer, "expected_digest": digest,
        "items": [{"sample_code": "M-1", "passed": True}, {"sample_code": "M-2", "passed": True}]})
check(f"准备数据：复核 {rno} 201", r.status_code == 201, r.text)

TRACKED_TABLES = ("sample_version", "dossier", "dossier_item",
                  "review_case", "review", "review_opinion", "event_correction")


def dump_tables(conn):
    return {t: [tuple(r) for r in conn.execute(f"SELECT * FROM {t} ORDER BY id").fetchall()]
            for t in TRACKED_TABLES}


def http_views():
    return {
        "samples": client.get(f"{API}/samples").json(),
        "m1_chain": client.get(f"{API}/samples/M-1/chain").json(),
        "m1_versions": client.get(f"{API}/samples/M-1/versions").json(),
        "m1_v0": client.get(f"{API}/samples/M-1/versions/0").json(),
        "m1_latest": client.get(f"{API}/samples/M-1/versions/latest").json(),
        "dossier": client.get(f"{API}/dossiers/D-UPG").json(),
        "dossier_verify": client.post(f"{API}/dossiers/D-UPG/verify").json(),
        "dossier_reviews": client.get(f"{API}/dossiers/D-UPG/reviews").json(),
    }


with db_session() as conn:
    tables_before = dump_tables(conn)
views_before = http_views()
check("准备数据：案卷核验一致", views_before["dossier_verify"]["match"] is True)
check("准备数据：案卷复核已终局", views_before["dossier_reviews"]["stage"] == "completed")

# 模拟上一版代码的库：无升级标记（其余结构与数据齐备）→ 重跑完整升级
with db_session(immediate=True) as conn:
    conn.execute("DROP TABLE schema_meta")
init_db()
with db_session() as conn:
    tables_after = dump_tables(conn)
    marker = conn.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()
check("升级后已有版本/案卷/复核/纠错记录逐行不变", tables_before == tables_after)
check("升级标记重新写入", marker is not None and marker[0] == SCHEMA_VERSION)
views_after = http_views()
check("升级前后样本/链/版本/案卷/复核 HTTP 响应完全一致", views_before == views_after)

# ---------------------------------------------------------------- 7. 升级后旧库 HTTP 查询语义不变
print("7) 升级完成后样本 / 版本 / 案卷 HTTP 查询可调用且响应含义不变")
use_db(legacy_db)  # 第 1 节升级完成的旧库
r = client.get(f"{API}/samples")
status_map = {it["sample_code"]: it["status"] for it in r.json()["items"]}
check("样本列表可查且结论含义不变",
      status_map == {"OLD-A": "valid", "OLD-B": "valid", "OLD-C": "pending"}, str(status_map))
r = client.get(f"{API}/samples/OLD-A")
check("单样本查询附 current_version=0（基线）",
      r.status_code == 200 and r.json()["evaluation"]["current_version"] == 0, r.text)
r = client.get(f"{API}/samples/OLD-A/chain")
check("证据链查询可查（valid / 4 事件 / current_version=0）",
      r.json()["status"] == "valid" and len(r.json()["timeline"]) == 4
      and r.json()["current_version"] == 0, r.text)
r = client.get(f"{API}/samples/OLD-A/versions")
check("版本索引：旧样本仅基线版本 0",
      r.json()["total"] == 1 and r.json()["current_version"] == 0
      and r.json()["items"][0]["change_type"] == "baseline", r.text)
r0 = client.get(f"{API}/samples/OLD-A/versions/0")
rl = client.get(f"{API}/samples/OLD-A/versions/latest")
check("指定版本 0 与 latest 快照一致（基线即最新）",
      r0.status_code == 200 and r0.json() == rl.json()
      and r0.json()["version_snapshot"]["change_type"] == "baseline", r0.text)
r = client.get(f"{API}/samples/OLD-B/versions/0")
check("已核定旧样本基线快照保留核定信息",
      r.json()["version_snapshot"]["approval"]["approver"] == "主管", r.text)
r = client.get(f"{API}/dossiers/DOSSIER-NONE")
check("案卷查询 404 语义不变",
      r.status_code == 404 and r.json()["error"]["code"] == "dossier_not_found", r.text)
r = client.get(f"{API}/samples/OLD-A/corrections")
check("纠错审计查询可查（空记录）", r.status_code == 200 and r.json()["total"] == 0, r.text)
# 升级后库继续正常工作：新事件产生版本 1；已核定样本保持冻结
r = ev({"event_id": "OLD-C-T1", "event_type": "transfer", "sample_code": "OLD-C",
        "occurred_at": "2026-01-01T02:00:00Z", "transfer_no": "TR-C",
        "from_custodian": "丙", "to_custodian": "丁"})
check("升级后新事件写入成功且版本从 1 递增",
      r.status_code == 201 and r.json()["evaluation"]["current_version"] == 1, r.text)
r = client.get(f"{API}/samples/OLD-C/versions")
check("版本索引变为 基线0 + 事件1",
      [i["version"] for i in r.json()["items"]] == [0, 1]
      and r.json()["items"][0]["change_type"] == "baseline"
      and r.json()["items"][1]["change_type"] == "event", r.text)
r = ev({"event_id": "OLD-B-X1", "event_type": "seal_check", "sample_code": "OLD-B",
        "occurred_at": "2026-01-01T06:00:00Z", "checked_seal_no": "SL-B"})
check("已核定旧样本保持冻结（409 history_locked）",
      r.status_code == 409 and r.json()["error"]["code"] == "history_locked", r.text)

print(f"\n结果: {PASS} 通过, {FAIL} 失败")
raise SystemExit(1 if FAIL else 0)
