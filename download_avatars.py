#!/usr/bin/env python3
import argparse
import asyncio
import json
from pathlib import Path
from typing import Any

import aiohttp

SCRIPT_DIR = Path(__file__).resolve().parent



def load_students(json_path: Path) -> list[str]:
    data = json.loads(json_path.read_text(encoding='utf-8'))
    if not isinstance(data, list):
        raise ValueError('输入 JSON 必须是数组')

    students: list[str] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        sid = item.get('id')
        if sid is None:
            continue
        sid_str = str(sid).strip()
        if sid_str:
            students.append(sid_str)
    return students


async def _download_avatar(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    student_id: str,
    out_dir: Path,
) -> tuple[str, str]:
    out_file = out_dir / f'{student_id}.webp'
    if out_file.exists() and out_file.stat().st_size > 0:
        return 'skipped', student_id

    url = f'https://schaledb.com/images/student/collection/{student_id}.webp'
    async with sem:
        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return 'failed', f'{student_id}.webp HTTP {resp.status}'
                data = await resp.read()
                if not data:
                    return 'failed', f'{student_id}.webp 空响应'
                out_file.write_bytes(data)
                return 'downloaded', student_id
        except Exception as e:
            return 'failed', f'{student_id}.webp {e}'


async def download_avatars_from_file(
    json_path: Path,
    out_dir: Path,
    timeout: int = 20,
    concurrency: int = 16,
    verbose: bool = True,
) -> dict[str, int]:
    if not json_path.exists():
        raise FileNotFoundError(f'未找到输入文件: {json_path}')

    out_dir.mkdir(parents=True, exist_ok=True)
    students = load_students(json_path)

    stats = {
        'total': len(students),
        'downloaded': 0,
        'skipped': 0,
        'failed': 0,
    }

    timeout_cfg = aiohttp.ClientTimeout(total=timeout)
    sem = asyncio.Semaphore(max(1, concurrency))
    headers = {'User-Agent': 'Mozilla/5.0'}

    async with aiohttp.ClientSession(timeout=timeout_cfg, headers=headers) as session:
        tasks = [
            _download_avatar(session=session, sem=sem, student_id=sid, out_dir=out_dir)
            for sid in students
        ]

        for coro in asyncio.as_completed(tasks):
            status, detail = await coro
            if status == 'downloaded':
                stats['downloaded'] += 1
                if verbose:
                    print(f'[DOWNLOADED] {detail}.webp')
            elif status == 'skipped':
                stats['skipped'] += 1
                if verbose:
                    print(f'[SKIPPED] {detail}.webp')
            else:
                stats['failed'] += 1
                if verbose:
                    print(f'[FAILED] {detail}')

    if verbose:
        print('----')
        print(f"total: {stats['total']}")
        print(f"downloaded: {stats['downloaded']}")
        print(f"skipped: {stats['skipped']}")
        print(f"failed: {stats['failed']}")

    return stats


def main() -> int:
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
    parser.add_argument(
        '--concurrency',
        type=int,
        default=16,
        help='并发下载数量，默认 16',
    )
    args = parser.parse_args()

    json_path = Path(args.input)
    if args.input == "students.simplified.json" and not json_path.is_absolute():
        json_path = SCRIPT_DIR / json_path

    out_dir = Path(args.out_dir)
    if args.out_dir == "avatars" and not out_dir.is_absolute():
        out_dir = SCRIPT_DIR / out_dir
    stats = asyncio.run(
        download_avatars_from_file(
            json_path=json_path,
            out_dir=out_dir,
            timeout=args.timeout,
            concurrency=args.concurrency,
            verbose=True,
        )
    )

    return 0 if stats['failed'] == 0 else 1


if __name__ == '__main__':
    raise SystemExit(main())
