#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def load_students(json_path: Path):
    data = json.loads(json_path.read_text(encoding='utf-8'))
    if not isinstance(data, list):
        raise ValueError('输入 JSON 必须是数组')

    students = []
    for item in data:
        if not isinstance(item, dict):
            continue
        sid = item.get('id')
        if sid is None:
            continue
        sid = str(sid).strip()
        if not sid:
            continue
        students.append(sid)
    return students


def download_avatar(student_id: str, out_dir: Path, timeout: int):
    out_file = out_dir / f'{student_id}.webp'
    if out_file.exists() and out_file.stat().st_size > 0:
        return None

    url = f'https://schaledb.com/images/student/collection/{student_id}.webp'
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    with urlopen(req, timeout=timeout) as resp:
        data = resp.read()

    if not data:
        raise ValueError('空响应')

    out_file.write_bytes(data)
    return True


def main():
    parser = argparse.ArgumentParser(
        description='从精简后的学生 JSON 下载头像，已有文件自动跳过。'
    )
    parser.add_argument(
        '--input',
        default='students.simplified.json',
        help='输入 JSON 路径，默认 students.simplified.json',
    )
    parser.add_argument(
        '--out-dir',
        default='avatars',
        help='头像输出目录，默认 avatars',
    )
    parser.add_argument(
        '--timeout',
        type=int,
        default=20,
        help='下载超时时间（秒），默认 20',
    )
    args = parser.parse_args()

    json_path = Path(args.input)
    out_dir = Path(args.out_dir)

    if not json_path.exists():
        raise FileNotFoundError(f'未找到输入文件: {json_path}')

    out_dir.mkdir(parents=True, exist_ok=True)
    students = load_students(json_path)

    downloaded = 0
    skipped = 0
    failed = 0

    for sid in students:
        try:
            result = download_avatar(sid, out_dir, args.timeout)
            if result is True:
                downloaded += 1
                print(f'[DOWNLOADED] {sid}.webp')
            else:
                skipped += 1
                print(f'[SKIPPED] {sid}.webp')
        except HTTPError as e:
            failed += 1
            print(f'[FAILED] {sid}.webp HTTP {e.code}')
        except URLError as e:
            failed += 1
            print(f'[FAILED] {sid}.webp URL error: {e.reason}')
        except Exception as e:
            failed += 1
            print(f'[FAILED] {sid}.webp {e}')

    print('----')
    print(f'total: {len(students)}')
    print(f'downloaded: {downloaded}')
    print(f'skipped: {skipped}')
    print(f'failed: {failed}')


if __name__ == '__main__':
    main()
