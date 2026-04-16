#!/usr/bin/env python3
import subprocess
import sys
from pathlib import Path
from urllib.request import Request, urlopen


def download_file(url: str, output_path: Path, step: int, total: int, title: str):
    print(f"[{step}/{total}] {title}")
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=60) as resp:
        data = resp.read()
    if not data:
        raise RuntimeError(f"下载失败，内容为空: {url}")
    output_path.write_bytes(data)


def run_python_script(script_name: str, step: int, total: int, title: str):
    print(f"[{step}/{total}] {title}")
    subprocess.run([sys.executable, script_name], check=True)


def main() -> int:
    script_dir = Path(__file__).resolve().parent

    cn_file = script_dir / "students.min.json"
    tw_file = script_dir / "zh_tw_students.min.json"
    en_file = script_dir / "en_students.min.json"

    try:
        download_file(
            "https://schaledb.com/data/cn/students.min.json",
            cn_file,
            1,
            5,
            "下载 CN students.min.json",
        )
        download_file(
            "https://schaledb.com/data/tw/students.min.json",
            tw_file,
            2,
            5,
            "下载 TW students.min.json -> zh_tw_students.min.json",
        )
        download_file(
            "https://schaledb.com/data/en/students.min.json",
            en_file,
            3,
            5,
            "下载 EN students.min.json -> en_students.min.json",
        )

        run_python_script(
            str(script_dir / "build_simplified_json.py"),
            4,
            5,
            "运行 build_simplified_json.py（生成 students.simplified.json + students.team_index.json）",
        )
        run_python_script(
            str(script_dir / "download_avatars.py"),
            5,
            5,
            "运行 download_avatars.py",
        )
    except Exception as e:
        print(f"更新失败: {e}")
        return 1

    print("完成：CN/TW/EN 数据、队伍索引与头像已更新。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
