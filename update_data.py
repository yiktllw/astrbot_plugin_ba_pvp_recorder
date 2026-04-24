#!/usr/bin/env python3
import argparse
import asyncio
import sys
from pathlib import Path

import aiohttp

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import build_simplified_json
import download_avatars


async def _download_one(
    session: aiohttp.ClientSession,
    url: str,
    output_path: Path,
    title: str,
    verbose: bool,
):
    if verbose:
        print(title)
    async with session.get(url) as resp:
        resp.raise_for_status()
        data = await resp.read()
        if not data:
            raise RuntimeError(f'下载失败，内容为空: {url}')
        output_path.write_bytes(data)


async def _download_sources(script_dir: Path, timeout: int, verbose: bool):
    timeout_cfg = aiohttp.ClientTimeout(total=timeout)
    headers = {'User-Agent': 'Mozilla/5.0'}

    jobs = [
        (
            'https://schaledb.com/data/cn/students.min.json',
            script_dir / 'students.min.json',
            '[1/5] 下载 ZH_CN students.min.json',
        ),
        (
            'https://schaledb.com/data/tw/students.min.json',
            script_dir / 'zh_tw_students.min.json',
            '[2/5] 下载 ZH_TW students.min.json -> zh_tw_students.min.json',
        ),
        (
            'https://schaledb.com/data/en/students.min.json',
            script_dir / 'en_students.min.json',
            '[3/5] 下载 EN students.min.json -> en_students.min.json',
        ),
    ]

    async with aiohttp.ClientSession(timeout=timeout_cfg, headers=headers) as session:
        await asyncio.gather(
            *[
                _download_one(
                    session=session,
                    url=url,
                    output_path=output_path,
                    title=title,
                    verbose=verbose,
                )
                for url, output_path, title in jobs
            ]
        )


async def run_update_async(
    script_dir: Path | None = None,
    timeout: int = 60,
    avatar_timeout: int = 20,
    avatar_concurrency: int = 16,
    verbose: bool = True,
) -> int:
    base_dir = script_dir or SCRIPT_DIR

    try:
        await _download_sources(base_dir, timeout=timeout, verbose=verbose)

        if verbose:
            print('[4/5] 运行 build_simplified_json.py（生成 students.simplified.json + students.team_index.json）')
        build_ret = await asyncio.to_thread(build_simplified_json.main, base_dir)
        if int(build_ret) != 0:
            raise RuntimeError(f'build_simplified_json.py 返回异常状态: {build_ret}')

        if verbose:
            print('[5/5] 运行 download_avatars.py')
        avatar_stats = await download_avatars.download_avatars_from_file(
            json_path=base_dir / 'students.simplified.json',
            out_dir=base_dir / 'avatars',
            timeout=avatar_timeout,
            concurrency=avatar_concurrency,
            verbose=verbose,
        )
        if avatar_stats.get('failed', 0) > 0:
            raise RuntimeError(f"头像下载存在失败项: {avatar_stats.get('failed')}")

    except Exception as e:
        print(f'更新失败: {e}')
        return 1

    if verbose:
        print('完成：CN/TW/EN 数据、队伍索引与头像已更新。')
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description='异步更新学生数据并下载头像。')
    parser.add_argument('--timeout', type=int, default=60, help='学生数据下载超时（秒）')
    parser.add_argument('--avatar-timeout', type=int, default=20, help='头像下载超时（秒）')
    parser.add_argument('--avatar-concurrency', type=int, default=16, help='头像下载并发数')
    args = parser.parse_args()

    return asyncio.run(
        run_update_async(
            script_dir=SCRIPT_DIR,
            timeout=args.timeout,
            avatar_timeout=args.avatar_timeout,
            avatar_concurrency=args.avatar_concurrency,
            verbose=True,
        )
    )


if __name__ == '__main__':
    raise SystemExit(main())
