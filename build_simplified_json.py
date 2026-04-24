#!/usr/bin/env python3
import json
from pathlib import Path
from typing import Any

# ===== 手动配置区 =====
SOURCE_JSON = "students.min.json"
OUTPUT_JSON = "students.simplified.json"
TEAM_OUTPUT_JSON = "students.team_index.json"
ABBR_JSON = "abbr.json"
DICT_KEY_FIELD = "id"
STRICT_MODE = False
INDENT = 2

ENABLE_ZH_TW_NAME = True
ZH_TW_SOURCE_JSON = "zh_tw_students.min.json"
ZH_TW_OUTPUT_KEY = "t_name"

ENABLE_EN_NAME = True
EN_SOURCE_JSON = "en_students.min.json"
EN_OUTPUT_KEY = "en_name"

KEEP_KEYS: list[tuple[str, str]] = [
    ("Id", "id"),
    ("Name", "name"),
]
# ===== 配置区结束 =====


def load_records(source_path: Path, dict_key_field: str) -> list[dict[str, Any]]:
    with source_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    records: list[dict[str, Any]] = []

    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                records.append(item)
        return records

    if isinstance(data, dict):
        for obj_key, item in data.items():
            if not isinstance(item, dict):
                continue
            rec = dict(item)
            if dict_key_field and dict_key_field not in rec:
                rec[dict_key_field] = obj_key
            records.append(rec)
        return records

    raise ValueError("Source JSON must be a list or object.")


def simplify_records(
    records: list[dict[str, Any]],
    key_specs: list[tuple[str, str]],
    strict: bool,
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for rec in records:
        out_item: dict[str, Any] = {}
        for src_key, dst_key in key_specs:
            if src_key in rec:
                out_item[dst_key] = rec[src_key]
            elif strict:
                raise KeyError(f"Missing key '{src_key}' in record: {rec}")
        output.append(out_item)
    return output


def build_name_map(source_path: Path) -> dict[str, str]:
    if not source_path.exists():
        return {}

    with source_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    result: dict[str, str] = {}

    if isinstance(data, dict):
        for obj_key, item in data.items():
            if not isinstance(item, dict):
                continue
            sid = str(item.get("Id", obj_key))
            name = item.get("Name")
            if sid and isinstance(name, str):
                result[sid] = name
        return result

    if isinstance(data, list):
        for item in data:
            if not isinstance(item, dict):
                continue
            sid = str(item.get("Id", item.get("id", "")))
            name = item.get("Name", item.get("name"))
            if sid and isinstance(name, str):
                result[sid] = name
        return result

    return {}


def load_abbr_map(path: Path) -> dict[str, list[str]]:
    if not path.exists():
        return {}

    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict):
        return {}

    result: dict[str, list[str]] = {}
    for raw_key, raw_vals in data.items():
        ids = [x.strip() for x in str(raw_key or "").split(",") if x.strip()]
        if not ids:
            continue

        abbrs: list[str] = []
        if isinstance(raw_vals, list):
            for v in raw_vals:
                s = str(v or "").strip()
                if s and s not in abbrs:
                    abbrs.append(s)

        for sid in ids:
            existing = result.get(sid, [])
            merged = existing + [x for x in abbrs if x not in existing]
            result[sid] = merged

    return result


def to_int(val: Any, default: int = 0) -> int:
    try:
        return int(val)
    except Exception:
        pass

    try:
        return int(float(str(val).strip()))
    except Exception:
        return default


def build_team_index(
    records: list[dict[str, Any]],
    zh_tw_map: dict[str, str],
    en_map: dict[str, str],
    abbr_map: dict[str, list[str]],
) -> dict[str, dict[str, dict[str, Any]]]:
    out: dict[str, dict[str, dict[str, Any]]] = {
        "strikers": {},
        "specials": {},
    }

    for rec in records:
        sid = str(rec.get("Id", rec.get("id", ""))).strip()
        if not sid:
            continue

        item = {
            "TacticRole": str(rec.get("TacticRole", "") or ""),
            "Range": to_int(rec.get("Range", 0), 0),
            "abbr": list(abbr_map.get(sid, [])),
            "name_chs": str(rec.get("Name", rec.get("name", "")) or ""),
            "name_cht": str(zh_tw_map.get(sid, "") or ""),
            "name_en": str(en_map.get(sid, "") or ""),
        }

        if sid.startswith("1"):
            out["strikers"][sid] = item
        elif sid.startswith("2"):
            out["specials"][sid] = item

    return out


def main(base_dir: Path | None = None) -> int:
    work_dir = Path(base_dir).resolve() if base_dir is not None else Path(__file__).resolve().parent

    source_path = work_dir / SOURCE_JSON
    output_path = work_dir / OUTPUT_JSON
    team_output_path = work_dir / TEAM_OUTPUT_JSON

    records = load_records(source_path, DICT_KEY_FIELD)
    simplified = simplify_records(records, KEEP_KEYS, STRICT_MODE)

    zh_tw_map: dict[str, str] = {}
    if ENABLE_ZH_TW_NAME:
        zh_tw_map = build_name_map(work_dir / ZH_TW_SOURCE_JSON)
        for item in simplified:
            sid = str(item.get("id", ""))
            item[ZH_TW_OUTPUT_KEY] = zh_tw_map.get(sid, "")

    en_map: dict[str, str] = {}
    if ENABLE_EN_NAME:
        en_map = build_name_map(work_dir / EN_SOURCE_JSON)
        for item in simplified:
            sid = str(item.get("id", ""))
            item[EN_OUTPUT_KEY] = en_map.get(sid, "")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        if INDENT and INDENT > 0:
            json.dump(simplified, f, ensure_ascii=False, indent=INDENT)
        else:
            json.dump(simplified, f, ensure_ascii=False, separators=(",", ":"))

    abbr_map = load_abbr_map(work_dir / ABBR_JSON)
    team_index = build_team_index(records, zh_tw_map, en_map, abbr_map)
    with team_output_path.open("w", encoding="utf-8") as f:
        json.dump(team_index, f, ensure_ascii=False, indent=2)

    print(f"Wrote {len(simplified)} records to {output_path}")
    print(
        "Wrote team index: "
        f"strikers={len(team_index.get('strikers', {}))}, "
        f"specials={len(team_index.get('specials', {}))} -> {team_output_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
