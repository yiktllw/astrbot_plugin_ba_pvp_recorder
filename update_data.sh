#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "[1/5] 下载 CN students.min.json"
curl -fL "https://schaledb.com/data/cn/students.min.json" -o "students.min.json"

echo "[2/5] 下载 TW students.min.json -> zh_tw_students.min.json"
curl -fL "https://schaledb.com/data/tw/students.min.json" -o "zh_tw_students.min.json"

echo "[3/5] 下载 EN students.min.json -> en_students.min.json"
curl -fL "https://schaledb.com/data/en/students.min.json" -o "en_students.min.json"

echo "[4/5] 运行 build_simplified_json.py（生成 students.simplified.json + students.team_index.json）"
python3 build_simplified_json.py

echo "[5/5] 运行 download_avatars.py"
python3 download_avatars.py

echo "完成：CN/TW/EN 数据、队伍索引与头像已更新。"
