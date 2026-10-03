#!/bin/sh
# 容器入口：初始化依赖（已在镜像内安装）、执行启动升级（建表/旧库补列/索引/版本 0 基线，
# 单事务、可恢复、并发启动安全）、按需写入演示数据，然后启动服务。
# set -e：升级失败即非零退出（不留部分升级结果、不对外服务），由 restart 策略重试恢复。
set -e

echo "[entrypoint] 执行启动升级（幂等，可恢复）..."
python -c "from app.db import init_db; init_db()"

if [ "${SEED_ON_START:-true}" = "true" ]; then
  echo "[entrypoint] 写入演示数据（库非空时自动跳过）..."
  python -m app.seed || echo "[entrypoint] 演示数据初始化跳过（可能已存在）"
fi

echo "[entrypoint] 启动 uvicorn: ${HOST:-0.0.0.0}:${PORT:-8080}"
exec uvicorn app.main:app --host "${HOST:-0.0.0.0}" --port "${PORT:-8080}"
