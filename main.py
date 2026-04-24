import asyncio
import importlib.util
import json
import re
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlparse, unquote
from typing import Any

from PIL import Image, ImageDraw, ImageFont, ImageOps

from astrbot.api import AstrBotConfig, logger
import astrbot.api.message_components as Comp
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.provider import ProviderRequest
from astrbot.api.star import Context, Star, register
from astrbot.core.star import StarTools

try:
    from astrbot.core.utils.quoted_message_parser import extract_quoted_message_images
except Exception:
    extract_quoted_message_images = None

@register("astrbot_plugin_ba_pvp_recorder", "yiktllw", "蔚蓝档案竞技场记录插件", "0.4.0")
class BAPvpRecorderPlugin(Star):
    _RENDER_LAYOUT = {
        "width": 1080,
        "margin": 16,
        "header_h": 70,
        "card_h": 162,
        "card_gap": 10,
        "header_radius": 12,
        "card_radius": 12,
        "header_title_x": 16,
        "header_title_y": 10,
        "header_meta_x": 16,
        "header_meta_y": 44,
        "half_gap": 10,
        "card_time_x": 12,
        "card_time_y": 4,
        "card_inner_left_x": 8,
        "card_inner_top_y": 24,
        "slot_w": 76,
        "slot_h": 76,
        "slot_gap": 6,
        "slot_radius": 10,
        "avatar_size": 48,
        "avatar_top_pad": 6,
        "name_max_len": 8,
        "name_trim_len": 7,
        "name_y": 58,
        "status_header_h": 34,
        "status_radius": 10,
        "status_text_min_pad": 12,
    }

    _RENDER_THEME = {
        "canvas_bg": (239, 244, 250),
        "header_bg": (19, 33, 49),
        "header_title_text": (255, 255, 255),
        "header_meta_text": (177, 197, 220),
        "card_bg": (248, 251, 255),
        "card_border": (210, 219, 230),
        "card_time_text": (78, 92, 108),
        "slot_bg": (245, 247, 251),
        "slot_border": (214, 222, 231),
        "placeholder_bg": (209, 215, 223),
        "placeholder_text": (96, 106, 118),
        "slot_name_text": (48, 57, 68),
        "status_header_text": (255, 255, 255),
        "attack_win_light": (44, 153, 255),
        "attack_win_dark": (18, 87, 168),
        "status_lose_light": (139, 154, 173),
        "status_lose_dark": (91, 103, 120),
        "defend_win_light": (240, 98, 98),
        "defend_win_dark": (167, 56, 56),
    }
    def __init__(self, context: Context, config: AstrBotConfig | None = None):
        super().__init__(context)
        self.config = config if isinstance(config, dict) else {}

        plugin_dir = Path(__file__).resolve().parent
        self._plugin_dir = plugin_dir
        self._data_dir = StarTools.get_data_dir("astrbot_plugin_ba_pvp_recorder")

        self._img_prompt_file = plugin_dir / "prompt_of_img2txt.txt"
        self._name2id_prompt_file = plugin_dir / "prompt_of_name2id.txt"
        self._judge_prompt_file = plugin_dir / "prompt_of_judging_img.txt"
        self._students_file = plugin_dir / "students.simplified.json"
        self._team_index_file = plugin_dir / "students.team_index.json"
        self._abbr_file = plugin_dir / "abbr.json"

        self._img_prompt_template = ""
        self._name2id_prompt_template = ""
        self._judge_prompt_template = ""
        self._students_context = "[]"

        self._students_list: list[dict[str, Any]] = []
        self._student_dict: dict[str, str] = {}
        self._name_to_id: dict[str, str] = {}
        self._team_strikers: dict[str, dict[str, Any]] = {}
        self._team_specials: dict[str, dict[str, Any]] = {}
        self._abbr_equiv_ids: dict[str, set[str]] = {}
        self._monitored_group_ids: set[str] = set()

        self._minimal_effort_sessions: set[str] = set()
        self._llm_timeout_seconds = 90
        self._llm_max_retries = 2
        self._daily_update_hour = 18
        self._daily_update_task: asyncio.Task | None = None
        self._update_data_module: Any = None
        self._verbose_auto_monitor_logs = False
        self._timezone_offset_hours = 8

    async def initialize(self):
        self._data_dir.mkdir(parents=True, exist_ok=True)
        self._img_prompt_template = self._load_img_prompt_template()
        self._name2id_prompt_template = self._load_name2id_prompt_template()
        self._judge_prompt_template = self._load_judge_prompt_template()
        self._students_context = self._load_students_context()
        self._load_team_index_context()
        self._refresh_monitored_group_ids()
        self._start_daily_update_task()
        logger.info("BAPvpRecorderPlugin initialized")

    def _load_update_data_module(self):
        module_path = self._plugin_dir / "update_data.py"
        if not module_path.exists():
            logger.warning(f"update_data.py 不存在: {module_path.as_posix()}")
            self._update_data_module = None
            return

        try:
            spec = importlib.util.spec_from_file_location(
                "astrbot_plugin_ba_pvp_update_data", module_path
            )
            if spec is None or spec.loader is None:
                raise RuntimeError("无法创建 update_data.py 的模块规格")
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            if not hasattr(module, "run_update_async"):
                raise RuntimeError("update_data.py 缺少 run_update_async")
            self._update_data_module = module
        except Exception as e:
            self._update_data_module = None
            logger.warning(f"加载 update_data.py 失败: {e}")

    def _seconds_until_next_daily_update(self) -> tuple[float, datetime]:
        now = datetime.now().astimezone()
        next_run = now.replace(hour=self._daily_update_hour, minute=0, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)
        return (next_run - now).total_seconds(), next_run

    async def _run_daily_update_once(self):
        self._load_update_data_module()
        if self._update_data_module is None:
            return

        try:
            ret = await self._update_data_module.run_update_async(
                script_dir=self._plugin_dir,
                verbose=False,
            )
        except Exception as e:
            logger.warning(f"[定时更新] 执行 update_data.py 失败: {e}")
            return

        if int(ret) != 0:
            logger.warning(f"[定时更新] update_data.py 返回非0状态: {ret}")
            return

        try:
            self._students_context = self._load_students_context()
            self._load_team_index_context()
            logger.info("[定时更新] 数据更新完成并重新加载映射成功")
        except Exception as e:
            logger.warning(f"[定时更新] 数据更新后重载映射失败: {e}")

    async def _daily_update_loop(self):
        try:
            while True:
                wait_seconds, next_run = self._seconds_until_next_daily_update()
                logger.info(
                    f"[定时更新] 下一次执行时间: {next_run.strftime('%Y-%m-%d %H:%M:%S %z')}"
                )
                await asyncio.sleep(wait_seconds)
                logger.info("[定时更新] 开始执行每日 update_data.py")
                await self._run_daily_update_once()
        except asyncio.CancelledError:
            logger.info("[定时更新] 每日更新任务已停止")
            raise

    def _start_daily_update_task(self):
        if self._daily_update_task and not self._daily_update_task.done():
            return
        self._daily_update_task = asyncio.create_task(self._daily_update_loop())

    @staticmethod
    def _parse_bool_config(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        text = str(value).strip().lower()
        return text in {"1", "true", "yes", "y", "on", "enabled"}

    @staticmethod
    def _parse_timezone_offset_hours(value: Any, default: int = 8) -> tuple[int, bool]:
        try:
            offset = int(str(value).strip())
        except Exception:
            return default, False
        if offset < -12 or offset > 14:
            return default, False
        return offset, True

    def _query_timezone(self) -> timezone:
        return timezone(timedelta(hours=self._timezone_offset_hours))

    def _refresh_monitored_group_ids(self):
        raw = self.config.get("monitor_group_ids", "") if isinstance(self.config, dict) else ""
        ids: set[str] = set()
        if isinstance(raw, list):
            for item in raw:
                v = str(item or "").strip()
                if v:
                    ids.add(v)
        elif isinstance(raw, str):
            for item in re.split(r"[,，\s\n\r\t]+", raw):
                v = str(item or "").strip()
                if v:
                    ids.add(v)
        self._monitored_group_ids = ids

        verbose_raw = self.config.get("verbose_auto_monitor_logs", False) if isinstance(self.config, dict) else False
        self._verbose_auto_monitor_logs = self._parse_bool_config(verbose_raw)

        tz_raw = self.config.get("timezone_offset_hours", 8) if isinstance(self.config, dict) else 8
        tz_offset, tz_valid = self._parse_timezone_offset_hours(tz_raw, default=8)
        self._timezone_offset_hours = tz_offset
        if not tz_valid:
            logger.warning(f"timezone_offset_hours invalid, fallback to 8 (input={tz_raw})")

        logger.info(
            f"监控群聊ID加载完成: {sorted(self._monitored_group_ids)}; verbose_auto_monitor_logs={self._verbose_auto_monitor_logs}; timezone_offset_hours={self._timezone_offset_hours}"
        )

    @filter.on_llm_request()
    async def _on_llm_request(self, event: AstrMessageEvent, req: ProviderRequest):
        if event.unified_msg_origin not in self._minimal_effort_sessions:
            return
        try:
            if hasattr(req, "reasoning") and isinstance(req.reasoning, dict):
                req.reasoning["effort"] = "minimal"
                return
            req.reasoning = {"effort": "minimal"}
            return
        except Exception:
            pass
        try:
            if hasattr(req, "extra_body") and isinstance(req.extra_body, dict):
                reasoning = req.extra_body.get("reasoning")
                if not isinstance(reasoning, dict):
                    reasoning = {}
                    req.extra_body["reasoning"] = reasoning
                reasoning["effort"] = "minimal"
                return
            req.extra_body = {"reasoning": {"effort": "minimal"}}
        except Exception as e:
            logger.warning(f"设置 reasoning.effort=minimal 失败: {e}")

    def _load_img_prompt_template(self) -> str:
        if not self._img_prompt_file.exists():
            raise FileNotFoundError(f"提示词文件不存在: {self._img_prompt_file.as_posix()}")
        template = self._img_prompt_file.read_text(encoding="utf-8").strip()
        for token in ("{{IMAGE_URLS}}", "{{USER_TEXT}}"):
            if token not in template:
                raise RuntimeError(f"提示词模板缺少占位符: {token}")
        return template

    def _load_name2id_prompt_template(self) -> str:
        if not self._name2id_prompt_file.exists():
            raise FileNotFoundError(f"提示词文件不存在: {self._name2id_prompt_file.as_posix()}")
        template = self._name2id_prompt_file.read_text(encoding="utf-8").strip()
        for token in ("{{UNRESOLVED_NAMES}}", "{{CURRENT_BATTLES}}", "{{STUDENTS_JSON}}"):
            if token not in template:
                raise RuntimeError(f"提示词模板缺少占位符: {token}")
        return template

    def _load_judge_prompt_template(self) -> str:
        if not self._judge_prompt_file.exists():
            raise FileNotFoundError(f"提示词文件不存在: {self._judge_prompt_file.as_posix()}")
        template = self._judge_prompt_file.read_text(encoding="utf-8").strip()
        for token in ("{{IMAGE_URLS}}", "{{USER_TEXT}}", "{{MESSAGE_OUTLINE}}"):
            if token not in template:
                raise RuntimeError(f"提示词模板缺少占位符: {token}")
        return template

    def _normalize_name(self, raw: str) -> str:
        s = str(raw or "").strip().lower()
        s = s.replace("（", "(").replace("）", ")")
        s = s.replace(" ", "")
        s = s.replace("·", "").replace("•", "")
        return s

    def _load_students_context(self) -> str:
        if not self._students_file.exists():
            raise FileNotFoundError(f"学生映射文件不存在: {self._students_file.as_posix()}")
        data = json.loads(self._students_file.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            raise ValueError("students.simplified.json 结构必须是数组")

        students_list_local: list[dict[str, Any]] = data
        student_dict_local: dict[str, str] = {}
        name_to_id_local: dict[str, str] = {}

        for item in data:
            if not isinstance(item, dict):
                continue
            sid = str(item.get("id", "")).strip()
            if not sid:
                continue
            student_dict_local[sid] = str(item.get("name") or sid)
            for alias in [item.get("name", ""), item.get("t_name", ""), item.get("en_name", "")]:
                norm = self._normalize_name(alias)
                if norm:
                    name_to_id_local[norm] = sid

        self._students_list = students_list_local
        self._student_dict = student_dict_local
        self._name_to_id = name_to_id_local
        return json.dumps(data, ensure_ascii=False, separators=(",", ":"))

    def _load_team_index_context(self):
        if not self._team_index_file.exists():
            logger.warning(f"队伍索引文件不存在: {self._team_index_file.as_posix()}")
            return

        try:
            data = json.loads(self._team_index_file.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning(f"读取队伍索引文件失败: {e}")
            return

        if not isinstance(data, dict):
            logger.warning("队伍索引文件结构错误：顶层必须是对象")
            return

        team_strikers_local: dict[str, dict[str, Any]] = {}
        team_specials_local: dict[str, dict[str, Any]] = {}

        strikers = data.get("strikers", {})
        specials = data.get("specials", {})
        if isinstance(strikers, dict):
            team_strikers_local = {str(k): v for k, v in strikers.items() if isinstance(v, dict)}
        if isinstance(specials, dict):
            team_specials_local = {str(k): v for k, v in specials.items() if isinstance(v, dict)}

        abbr_equiv_ids_local = self._load_abbr_equiv_ids()
        for sid in list(team_strikers_local.keys()) + list(team_specials_local.keys()):
            if sid not in abbr_equiv_ids_local:
                abbr_equiv_ids_local[sid] = {sid}

        self._team_strikers = team_strikers_local
        self._team_specials = team_specials_local
        self._abbr_equiv_ids = abbr_equiv_ids_local

        logger.info(
            f"队伍索引加载完成: strikers={len(self._team_strikers)}, specials={len(self._team_specials)}"
        )

    def _load_abbr_equiv_ids(self) -> dict[str, set[str]]:
        mapping: dict[str, set[str]] = {}
        if not self._abbr_file.exists():
            return mapping

        try:
            obj = json.loads(self._abbr_file.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning(f"读取 abbr.json 失败: {e}")
            return mapping

        if not isinstance(obj, dict):
            return mapping

        for raw_key in obj.keys():
            ids = [x.strip() for x in str(raw_key or "").split(",") if x.strip()]
            if not ids:
                continue
            merged = set(ids)
            for sid in ids:
                merged.update(mapping.get(sid, set()))
            for sid in ids:
                mapping.setdefault(sid, set()).update(merged)

        for sid in list(mapping.keys()):
            mapping[sid].add(sid)
        return mapping

    def _expand_equiv_ids(self, ids: set[str]) -> set[str]:
        out: set[str] = set()
        for sid in ids:
            out.update(self._abbr_equiv_ids.get(sid, {sid}))
        return out

    def _lookup_team_ids_by_priority(self, token: str, is_striker: bool) -> set[str]:
        bucket = self._team_strikers if is_striker else self._team_specials
        if not bucket:
            return set()

        raw = str(token or "").strip()
        if not raw:
            return set()
        norm = self._normalize_name(raw)

        if raw in bucket:
            return self._expand_equiv_ids({raw})

        def collect_by_abbr() -> set[str]:
            m: set[str] = set()
            for sid, meta in bucket.items():
                arr = meta.get("abbr", [])
                if not isinstance(arr, list):
                    continue
                for v in arr:
                    if self._normalize_name(v) == norm:
                        m.add(sid)
                        break
            return m

        def collect_by_field(field: str) -> set[str]:
            m: set[str] = set()
            for sid, meta in bucket.items():
                val = str(meta.get(field, "") or "")
                if self._normalize_name(val) == norm:
                    m.add(sid)
            return m

        for matcher in [collect_by_abbr, lambda: collect_by_field("name_chs"), lambda: collect_by_field("name_cht"), lambda: collect_by_field("name_en")]:
            matched = matcher()
            if matched:
                return self._expand_equiv_ids(matched)

        return set()

    def _resolve_team_token_ids(self, token: str, slot_idx: int) -> tuple[set[str] | None, str]:
        raw = str(token or "").strip()
        if not raw or raw == "_":
            return None, ""

        is_striker = slot_idx < 4
        bucket = self._team_strikers if is_striker else self._team_specials
        if not bucket:
            return None, "队伍索引未加载，请先运行 update_data.sh"

        if is_striker:
            low = raw.lower()
            if low == "t":
                out = {sid for sid, meta in bucket.items() if str(meta.get("TacticRole", "")).lower() == "tanker"}
                if not out:
                    return None, "未找到符合 t(Tanker) 条件的 Striker"
                return self._expand_equiv_ids(out), ""
            if low == "c":
                out = {sid for sid, meta in bucket.items() if str(meta.get("TacticRole", "")).lower() == "damagedealer"}
                if not out:
                    return None, "未找到符合 c(DamageDealer) 条件的 Striker"
                return self._expand_equiv_ids(out), ""
            if len(raw) == 1 and raw.isdigit() and raw in "345678":
                target_range = int(raw) * 100 + 50
                out = {sid for sid, meta in bucket.items() if int(meta.get("Range", 0) or 0) == target_range}
                if not out:
                    return None, f"未找到符合 {raw}(Range={target_range}) 条件的 Striker"
                return self._expand_equiv_ids(out), ""

        out = self._lookup_team_ids_by_priority(raw, is_striker)
        if not out:
            side_name = "Striker" if is_striker else "Special"
            return None, f"{side_name} 槽位参数 '{raw}' 未匹配到学生"
        return out, ""

    def _parse_team_query_tokens(self, args: list[str]) -> tuple[list[str], int, str]:
        cleaned = [str(x or "").strip() for x in args if str(x or "").strip()]
        if not cleaned:
            return [], 0, "用法：/队伍 [参数]；支持 1 个(4-6字)、4-6 个、或 7 个参数(第7个为数量上限)"

        limit = 10
        tokens: list[str] = []

        if len(cleaned) == 1:
            spec = cleaned[0]
            if len(spec) < 4 or len(spec) > 6:
                return [], 0, "单参数模式要求总长度为 4 到 6 字"
            tokens = list(spec) + ["_"] * (6 - len(spec))
        elif 4 <= len(cleaned) <= 6:
            tokens = cleaned + ["_"] * (6 - len(cleaned))
        elif len(cleaned) == 7:
            tokens = cleaned[:6]
            try:
                limit = int(cleaned[6])
            except ValueError:
                return [], 0, "第7个参数必须是正整数（最多输出条数）"
            if limit <= 0:
                return [], 0, "第7个参数必须大于 0"
            if limit > 100:
                limit = 100
        else:
            return [], 0, "参数数量错误：支持 1 个(4-6字)、4-6 个、或 7 个参数"

        return tokens, limit, ""

    def _side_matches_team_filters(self, side: dict[str, Any], filters: list[set[str] | None]) -> bool:
        def slot_ok(allowed: set[str] | None, sid: str) -> bool:
            if allowed is None:
                return True
            return sid in allowed

        st = [
            str(side.get("st1", "") or ""),
            str(side.get("st2", "") or ""),
            str(side.get("st3", "") or ""),
            str(side.get("st4", "") or ""),
        ]
        sp = [
            str(side.get("sp1", "") or ""),
            str(side.get("sp2", "") or ""),
        ]

        for idx in range(4):
            if not slot_ok(filters[idx], st[idx]):
                return False

        f1, f2 = filters[4], filters[5]
        return (slot_ok(f1, sp[0]) and slot_ok(f2, sp[1])) or (slot_ok(f1, sp[1]) and slot_ok(f2, sp[0]))

    def _load_font(self, size: int, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
        local_dir = self._plugin_dir / "fonts"
        if bold:
            candidates = [
                local_dir / "NotoSansCJK-Bold.ttc",
                local_dir / "NotoSerifCJK-Bold.ttc",
                Path("/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc"),
                Path("/usr/share/fonts/opentype/noto/NotoSerifCJK-Bold.ttc"),
                Path("/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc"),
            ]
        else:
            candidates = [
                local_dir / "NotoSansCJK-Regular.ttc",
                local_dir / "NotoSerifCJK-Regular.ttc",
                Path("/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"),
                Path("/usr/share/fonts/opentype/noto/NotoSerifCJK-Regular.ttc"),
                Path("/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc"),
            ]

        for font_path in candidates:
            if not font_path.exists():
                continue
            if font_path.suffix.lower() == ".ttc":
                for idx in [0, 1, 2, 3, 4, 5, 6, 7]:
                    try:
                        return ImageFont.truetype(font_path.as_posix(), size=size, index=idx)
                    except Exception:
                        continue
            else:
                try:
                    return ImageFont.truetype(font_path.as_posix(), size=size)
                except Exception:
                    continue

        checked = ", ".join(p.as_posix() for p in candidates)
        msg = f"未找到可用中文字体（bold={bold}），请检查 fonts 目录或系统字体。已检查: {checked}"
        logger.error(msg)
        raise RuntimeError(msg)

    def _normalize_image_source_key(self, raw: str) -> str:
        val = str(raw or "").strip()
        if not val:
            return ""

        lowered = val.lower()
        if lowered.startswith(("http://", "https://", "file://")):
            try:
                u = urlparse(val)
                # 以完整 URL 语义去重（scheme/netloc/path/query），避免仅文件名导致误合并。
                scheme = (u.scheme or "").lower()
                netloc = (u.netloc or "").lower()
                path = unquote(u.path or "")
                query = u.query or ""
                return f"url_full:{scheme}://{netloc}{path}?{query}"
            except Exception:
                return f"url_raw:{val}"

        try:
            norm_path = Path(val).as_posix()
            return f"path_full:{norm_path}"
        except Exception:
            return f"path_raw:{val}"

    async def _extract_image_data_urls(self, event: AstrMessageEvent) -> list[str]:
        direct_urls: list[str] = []
        quoted_urls: list[str] = []

        for comp in event.get_messages():
            if comp.__class__.__name__.lower() == "image":
                try:
                    path = str(await comp.convert_to_file_path() or "").strip()
                    if path:
                        direct_urls.append(path)
                except Exception as e:
                    logger.error(f"提取图片路径失败: {e}")

        if extract_quoted_message_images is not None:
            try:
                raw_quoted = await extract_quoted_message_images(event)
                for url in raw_quoted:
                    norm = str(url or "").strip()
                    if norm:
                        quoted_urls.append(norm)
            except Exception as e:
                logger.warning(f"提取引用消息图片失败: {e}")

        # /记录 且带引用时，优先只使用引用消息图片，避免同一批图片被双重采集。
        user_text = str(event.message_str or "").strip()
        use_sources = quoted_urls if quoted_urls and (user_text.startswith("/记录") or user_text.startswith("记录")) else (direct_urls + quoted_urls)

        data_urls: list[str] = []
        seen_keys: set[str] = set()
        for src in use_sources:
            key = self._normalize_image_source_key(src)
            if not key or key in seen_keys:
                continue
            seen_keys.add(key)
            data_urls.append(src)

        return data_urls

    def _build_prompt(self, image_urls: list[str], user_text: str, player_name_context: list[str] | None = None) -> str:
        urls_block = "\n".join(f"- {url}" for url in image_urls)
        base = self._img_prompt_template.replace("{{IMAGE_URLS}}", urls_block).replace("{{USER_TEXT}}", user_text.strip() or "无")

        context_names: list[str] = []
        seen: set[str] = set()
        for raw in player_name_context or []:
            nm = str(raw or "").strip()
            if not nm:
                continue
            k = nm.lower()
            if k in seen:
                continue
            seen.add(k)
            context_names.append(nm)

        if not context_names:
            return base

        ctx_block = "\n".join(f"- {name}" for name in context_names)
        return (
            base
            + "\n\n判图阶段提取的玩家名称上下文（用于跨批次对齐同一玩家，请优先保持一致写法）：\n"
            + ctx_block
        )

    def _build_judge_prompt(self, image_urls: list[str], user_text: str, message_outline: str) -> str:
        urls_block = "\n".join(f"- {url}" for url in image_urls)
        return (
            self._judge_prompt_template.replace("{{IMAGE_URLS}}", urls_block)
            .replace("{{USER_TEXT}}", user_text.strip() or "无")
            .replace("{{MESSAGE_OUTLINE}}", message_outline.strip() or "无")
        )

    def _build_name2id_prompt(self, unresolved_names: list[str], current_battles: list[dict[str, Any]]) -> str:
        return (
            self._name2id_prompt_template.replace("{{UNRESOLVED_NAMES}}", json.dumps(unresolved_names, ensure_ascii=False))
            .replace("{{CURRENT_BATTLES}}", json.dumps(current_battles, ensure_ascii=False))
            .replace("{{STUDENTS_JSON}}", self._students_context)
        )

    def _image_timeout_seconds(self, image_urls: list[str]) -> int:
        count = len(image_urls)
        if count <= 0:
            return 30
        return min(count * 30, 120)

    def _split_image_batches_for_name_recognition(self, image_urls: list[str]) -> list[list[str]]:
        n = len(image_urls)
        if n <= 0:
            return []
        if n <= 5:
            return [image_urls]

        # 按 ceil(n/5) 计算请求次数，再尽量均分每批图片数量。
        req_count = (n + 4) // 5
        base = n // req_count
        rem = n % req_count

        sizes = [base] * req_count
        for i in range(rem):
            sizes[i] += 1

        batches: list[list[str]] = []
        idx = 0
        for sz in sizes:
            batches.append(image_urls[idx : idx + sz])
            idx += sz
        return batches

    async def _call_ai_for_text(
        self,
        event: AstrMessageEvent,
        prompt: str,
        image_urls: list[str],
        timeout_seconds: int,
    ) -> str:
        provider_id = await self.context.get_current_chat_provider_id(umo=event.unified_msg_origin)
        llm_resp = await self.context.llm_generate(
            chat_provider_id=provider_id,
            prompt=prompt,
            image_urls=image_urls,
            timeout=timeout_seconds,
        )
        return llm_resp.completion_text

    async def _call_ai_text_only(self, event: AstrMessageEvent, prompt: str) -> str:
        provider_id = await self.context.get_current_chat_provider_id(umo=event.unified_msg_origin)
        llm_resp = await self.context.llm_generate(chat_provider_id=provider_id, prompt=prompt, timeout=self._llm_timeout_seconds)
        return llm_resp.completion_text

    def _safe_json_parse(self, text: str) -> Any:
        cleaned = text.strip()
        if cleaned.startswith("```"):
            cleaned = re.sub(r"^```[a-zA-Z0-9_]*\n", "", cleaned)
            cleaned = re.sub(r"\n```$", "", cleaned)
        try:
            return json.loads(cleaned)
        except Exception:
            decoder = json.JSONDecoder()
            starts = [i for i in (cleaned.find("{"), cleaned.find("[")) if i != -1]
            for st in sorted(starts):
                try:
                    obj, _ = decoder.raw_decode(cleaned[st:])
                    return obj
                except Exception:
                    continue
            raise

    def _parse_judge_result(self, text: str) -> tuple[bool, list[str]]:
        obj = self._safe_json_parse(text)
        if not isinstance(obj, dict):
            return False, []

        is_report = False
        for key in ("is_ba_pvp_report", "is_pvp_report", "is_report"):
            val = obj.get(key)
            if isinstance(val, bool):
                is_report = val
                break
            if isinstance(val, str):
                is_report = val.strip().lower() in ("true", "1", "yes", "y")
                break

        names_raw = None
        for key in ("player_names", "report_player_names", "names"):
            if key in obj:
                names_raw = obj.get(key)
                break

        names: list[str] = []
        if isinstance(names_raw, list):
            seen: set[str] = set()
            for it in names_raw:
                nm = str(it or "").strip()
                if not nm:
                    continue
                k = nm.lower()
                if k in seen:
                    continue
                seen.add(k)
                names.append(nm)
        elif isinstance(names_raw, str):
            parts = [x.strip() for x in names_raw.split(",")]
            seen: set[str] = set()
            for nm in parts:
                if not nm:
                    continue
                k = nm.lower()
                if k in seen:
                    continue
                seen.add(k)
                names.append(nm)

        return is_report, names

    def _parse_name_report_text(self, text: str) -> list[dict[str, Any]]:
        obj = self._safe_json_parse(text)
        if not isinstance(obj, list):
            raise ValueError("第一步输出不是 JSON 数组")
        result: list[dict[str, Any]] = []
        for battle in obj:
            if not isinstance(battle, dict):
                continue
            attack = battle.get("attack", {}) if isinstance(battle.get("attack", {}), dict) else {}
            defend = battle.get("defend", {}) if isinstance(battle.get("defend", {}), dict) else {}
            item = {
                "status": bool(battle.get("status", False)),
                "attack": {
                    "name": str(attack.get("name", "UNKNOWN") or "UNKNOWN"),
                    "status": bool(attack.get("status", False)),
                    "st1_name": str(attack.get("st1_name", "UNKNOWN") or "UNKNOWN"),
                    "st2_name": str(attack.get("st2_name", "UNKNOWN") or "UNKNOWN"),
                    "st3_name": str(attack.get("st3_name", "UNKNOWN") or "UNKNOWN"),
                    "st4_name": str(attack.get("st4_name", "UNKNOWN") or "UNKNOWN"),
                    "sp1_name": str(attack.get("sp1_name", "UNKNOWN") or "UNKNOWN"),
                    "sp2_name": str(attack.get("sp2_name", "UNKNOWN") or "UNKNOWN"),
                },
                "defend": {
                    "name": str(defend.get("name", "UNKNOWN") or "UNKNOWN"),
                    "status": bool(defend.get("status", False)),
                    "st1_name": str(defend.get("st1_name", "UNKNOWN") or "UNKNOWN"),
                    "st2_name": str(defend.get("st2_name", "UNKNOWN") or "UNKNOWN"),
                    "st3_name": str(defend.get("st3_name", "UNKNOWN") or "UNKNOWN"),
                    "st4_name": str(defend.get("st4_name", "UNKNOWN") or "UNKNOWN"),
                    "sp1_name": str(defend.get("sp1_name", "UNKNOWN") or "UNKNOWN"),
                    "sp2_name": str(defend.get("sp2_name", "UNKNOWN") or "UNKNOWN"),
                },
            }
            result.append(item)
        if not result:
            raise ValueError("第一步识别结果为空")
        return result

    def _calc_unknown_ratio(self, battles: list[dict[str, Any]]) -> tuple[int, int, float]:
        total = 0
        unknown = 0
        name_fields = ["name", "st1_name", "st2_name", "st3_name", "st4_name", "sp1_name", "sp2_name"]

        for battle in battles:
            if not isinstance(battle, dict):
                continue
            for side_key in ["attack", "defend"]:
                side = battle.get(side_key, {})
                if not isinstance(side, dict):
                    continue
                for field in name_fields:
                    total += 1
                    val = str(side.get(field, "") or "").strip()
                    if self._normalize_name(val) == "unknown":
                        unknown += 1

        if total <= 0:
            return 0, 0, 0.0
        ratio = unknown / total
        return unknown, total, ratio

    def _name_to_id_lookup(self, name: str) -> str:
        norm = self._normalize_name(name)
        if not norm or norm == "unknown":
            return ""
        return self._name_to_id.get(norm, "")

    def _apply_local_name_mapping(self, name_battles: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str]]:
        unresolved: list[str] = []
        mapped: list[dict[str, Any]] = []
        for battle in name_battles:
            item = json.loads(json.dumps(battle, ensure_ascii=False))
            for side_key in ["attack", "defend"]:
                side = item.get(side_key, {})
                for slot in ["st1", "st2", "st3", "st4", "sp1", "sp2"]:
                    nm_key = f"{slot}_name"
                    raw_name = str(side.get(nm_key, "") or "").strip()
                    sid = self._name_to_id_lookup(raw_name)
                    if sid:
                        side[slot] = sid
                    else:
                        side[slot] = ""
                        if raw_name and raw_name.upper() != "UNKNOWN" and raw_name not in unresolved:
                            unresolved.append(raw_name)
            mapped.append(item)
        return mapped, unresolved

    async def _resolve_missing_ids_by_llm(self, event: AstrMessageEvent, unresolved_names: list[str], current_battles: list[dict[str, Any]]) -> dict[str, str]:
        if not unresolved_names:
            return {}
        prompt = self._build_name2id_prompt(unresolved_names, current_battles)
        text = await self._call_ai_text_only(event, prompt)
        obj = self._safe_json_parse(text)
        if not isinstance(obj, dict):
            return {}
        arr = obj.get("mappings")
        if not isinstance(arr, list):
            return {}
        out: dict[str, str] = {}
        for it in arr:
            if not isinstance(it, dict):
                continue
            name = str(it.get("name", "") or "").strip()
            sid = str(it.get("id", "") or "").strip()
            if name:
                out[name] = sid
        return out

    def _apply_llm_id_mapping(self, battles: list[dict[str, Any]], llm_map: dict[str, str]):
        for battle in battles:
            for side_key in ["attack", "defend"]:
                side = battle.get(side_key, {})
                for slot in ["st1", "st2", "st3", "st4", "sp1", "sp2"]:
                    if side.get(slot):
                        continue
                    nm_key = f"{slot}_name"
                    raw_name = str(side.get(nm_key, "") or "").strip()
                    sid = llm_map.get(raw_name, "")
                    if sid and sid.isdigit():
                        side[slot] = sid

    def _safe_key(self, raw: str) -> str:
        return re.sub(r"[^a-zA-Z0-9_-]", "_", raw)

    def _get_record_db(self, event: AstrMessageEvent) -> Path:
        group_id = self._get_group_id(event)
        if group_id:
            return self._data_dir / f"battle_records_group_{self._safe_key(group_id)}.sqlite3"
        return self._data_dir / f"battle_records_session_{self._safe_key(event.unified_msg_origin)}.sqlite3"

    def _get_group_id(self, event: AstrMessageEvent) -> str:
        message_obj = getattr(event, "message_obj", None)
        if message_obj is None:
            return ""
        return str(getattr(message_obj, "group_id", "") or "").strip()

    def _append_record(self, event: AstrMessageEvent, image_urls: list[str], parsed: list[dict[str, Any]], raw_output: str) -> Path:
        record_db = self._get_record_db(event)
        group_id = self._get_group_id(event)
        created_at = datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(record_db) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS battle_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    session TEXT NOT NULL,
                    group_id TEXT,
                    sender_id TEXT,
                    sender_name TEXT,
                    image_urls_json TEXT NOT NULL,
                    result_json TEXT NOT NULL,
                    raw_output TEXT NOT NULL
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_battle_records_created_at ON battle_records(created_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_battle_records_group_id ON battle_records(group_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_battle_records_session ON battle_records(session)")
            conn.execute(
                """
                INSERT INTO battle_records (
                    created_at, session, group_id, sender_id, sender_name,
                    image_urls_json, result_json, raw_output
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    created_at,
                    event.unified_msg_origin,
                    group_id,
                    event.get_sender_id(),
                    event.get_sender_name(),
                    json.dumps(image_urls, ensure_ascii=False),
                    json.dumps(parsed, ensure_ascii=False),
                    raw_output,
                ),
            )
            conn.commit()
        return record_db

    async def _reply_recorded(self, event: AstrMessageEvent):
        msg_id = str(getattr(getattr(event, "message_obj", None), "message_id", "") or "").strip()
        if msg_id:
            try:
                reply_comp = None
                for kwargs in ({"id": msg_id}, {"message_id": msg_id}, {"msg_id": msg_id}):
                    try:
                        reply_comp = Comp.Reply(**kwargs)
                        break
                    except Exception:
                        continue
                if reply_comp is not None:
                    yield event.chain_result([reply_comp, Comp.Plain("已记录")])
                    return
            except Exception:
                pass
        yield event.plain_result("已记录")

    async def _get_player_name_context_for_record(self, event: AstrMessageEvent, image_data_urls: list[str], user_text: str) -> list[str]:
        judge_prompt = self._build_judge_prompt(
            image_urls=image_data_urls,
            user_text=user_text,
            message_outline=event.get_message_outline(),
        )
        timeout_seconds = self._image_timeout_seconds(image_data_urls)
        self._minimal_effort_sessions.add(event.unified_msg_origin)
        try:
            judge_text = await asyncio.wait_for(
                self._call_ai_for_text(
                    event=event,
                    prompt=judge_prompt,
                    image_urls=image_data_urls,
                    timeout_seconds=timeout_seconds,
                ),
                timeout=timeout_seconds,
            )
        except Exception as e:
            logger.warning(f"[/记录] 获取玩家名称上下文失败: {e}")
            return []
        finally:
            self._minimal_effort_sessions.discard(event.unified_msg_origin)

        try:
            is_report, names = self._parse_judge_result(judge_text)
            logger.info(f"[/记录] 预判结果: is_ba_pvp_report={is_report}, names={len(names)}")
            return names
        except Exception as e:
            logger.warning(f"[/记录] 预判结果解析失败: {e}")
            return []

    async def _recognize_and_record_core(
        self,
        event: AstrMessageEvent,
        image_data_urls: list[str],
        user_text: str,
        silent: bool,
        player_name_context: list[str] | None = None,
    ) -> tuple[bool, str]:
        batches = self._split_image_batches_for_name_recognition(image_data_urls)
        if not batches:
            return False, "未检测到图片"

        batch_sizes = [len(b) for b in batches]
        logger.info(
            f"[名称识别] 分批计划 total_images={len(image_data_urls)} batches={len(batches)} batch_sizes={batch_sizes}"
        )

        name_battles: list[dict[str, Any]] = []
        raw_outputs: list[str] = []

        for i, batch in enumerate(batches, start=1):
            prompt = self._build_prompt(
                image_urls=batch,
                user_text=user_text,
                player_name_context=player_name_context,
            )
            image_timeout = self._image_timeout_seconds(batch)
            first_text = None

            logger.info(
                f"[名称识别] 批次 {i}/{len(batches)} 开始，batch_images={len(batch)}, timeout={image_timeout}s"
            )

            self._minimal_effort_sessions.add(event.unified_msg_origin)
            try:
                for _ in range(self._llm_max_retries):
                    try:
                        first_text = await asyncio.wait_for(
                            self._call_ai_for_text(
                                event=event,
                                prompt=prompt,
                                image_urls=batch,
                                timeout_seconds=image_timeout,
                            ),
                            timeout=image_timeout,
                        )
                        break
                    except asyncio.TimeoutError:
                        logger.warning("名称识别请求超时，正在重试...")
                        continue
                    except Exception as e:
                        if "Request timed out" in str(e):
                            logger.warning("名称识别网络超时，正在重试...")
                            continue
                        logger.error(f"名称识别失败: {e}")
                        return False, "模型调用异常"
            finally:
                self._minimal_effort_sessions.discard(event.unified_msg_origin)

            if not first_text:
                return False, f"第{i}批识图超时"

            raw_outputs.append(first_text)

            try:
                parsed_batch = self._parse_name_report_text(first_text)
            except Exception as e:
                logger.error(f"第{i}批名称解析失败: {e}")
                return False, f"第{i}批名称输出格式异常"

            name_battles.extend(parsed_batch)
            logger.info(f"[名称识别] 批次 {i}/{len(batches)} 完成，records={len(parsed_batch)}")

        unknown_count, total_count, unknown_ratio = self._calc_unknown_ratio(name_battles)
        if total_count > 0 and unknown_ratio > (1 / 3):
            logger.warning(
                f"识别结果拒绝入库：UNKNOWN占比过高 unknown={unknown_count}/{total_count} ratio={unknown_ratio:.2%}"
            )
            return False, f"识别结果无效：UNKNOWN占比过高({unknown_count}/{total_count})"

        mapped_battles, unresolved = self._apply_local_name_mapping(name_battles)
        if unresolved:
            try:
                self._minimal_effort_sessions.add(event.unified_msg_origin)
                llm_map = await self._resolve_missing_ids_by_llm(event, unresolved, mapped_battles)
                self._apply_llm_id_mapping(mapped_battles, llm_map)
            except Exception as e:
                logger.warning(f"二次补全ID失败: {e}")
            finally:
                self._minimal_effort_sessions.discard(event.unified_msg_origin)

        try:
            self._append_record(
                event=event,
                image_urls=image_data_urls,
                parsed=mapped_battles,
                raw_output="\n\n---BATCH---\n\n".join(raw_outputs),
            )
        except Exception as e:
            logger.error(f"写入记录失败: {e}")
            return False, "写入数据库失败"

        if silent and self._verbose_auto_monitor_logs:
            logger.info(
                f"[静默记录] 已记录战报 group={self._get_group_id(event)} sender={event.get_sender_id()} images={len(image_data_urls)} batches={len(batches)}"
            )
        return True, "ok"

    @filter.regex(r".*(\[图片\]|\[CQ:image\]).*")
    async def auto_monitor_group_images(self, event: AstrMessageEvent):
        group_id = self._get_group_id(event)
        if not group_id:
            return
        if group_id not in self._monitored_group_ids:
            return
        user_text = str(event.message_str or "").strip()
        if user_text.startswith("/记录") or user_text.startswith("记录"):
            if self._verbose_auto_monitor_logs:
                logger.info(f"[自动监控] 跳过 /记录 指令消息 group={group_id}")
            return
        image_data_urls = await self._extract_image_data_urls(event)
        if not image_data_urls:
            return

        if self._verbose_auto_monitor_logs:
            logger.info(f"[自动监控] 命中监控群图片消息 group={group_id} sender={event.get_sender_id()} images={len(image_data_urls)}")

        judge_prompt = self._build_judge_prompt(image_urls=image_data_urls, user_text=user_text, message_outline=event.get_message_outline())
        image_timeout = self._image_timeout_seconds(image_data_urls)
        self._minimal_effort_sessions.add(event.unified_msg_origin)
        try:
            judge_text = await asyncio.wait_for(
                self._call_ai_for_text(event=event, prompt=judge_prompt, image_urls=image_data_urls, timeout_seconds=image_timeout),
                timeout=image_timeout,
            )
        except Exception as e:
            self._minimal_effort_sessions.discard(event.unified_msg_origin)
            logger.warning(f"[自动监控] 判图失败 group={group_id}: {e}")
            return
        finally:
            self._minimal_effort_sessions.discard(event.unified_msg_origin)

        try:
            is_report, judge_player_names = self._parse_judge_result(judge_text)
        except Exception as e:
            logger.warning(f"[自动监控] 判图结果解析失败 group={group_id}: {e}; raw={judge_text}")
            return

        if self._verbose_auto_monitor_logs:
            logger.info(
                f"[自动监控] 判图结果 group={group_id}: is_ba_pvp_report={is_report}, names={len(judge_player_names)}"
            )
        if not is_report:
            return

        ok, reason = await self._recognize_and_record_core(
            event=event,
            image_data_urls=image_data_urls,
            user_text=user_text,
            silent=True,
            player_name_context=judge_player_names,
        )
        if ok:
            if self._verbose_auto_monitor_logs:
                logger.info(f"[自动监控] 静默记录完成 group={group_id}")
        else:
            logger.warning(f"[自动监控] 静默记录失败 group={group_id}: {reason}")

    @filter.command("帮助")
    async def ba_pvp_help(self, event: AstrMessageEvent):
        help_text = (
            "蔚蓝档案竞技场记录插件已加载。\n"
            "当前可用命令:\n"
            "/帮助 - 查看帮助\n"
            "/记录 [图片][图片] - 识别图片战报并记录；手机端可以 [引用图片] /记录\n"
            "/查询战报 [用户名] [YYYY-MM-DD] - 按人/按天查询\n"
            "/最近战报 [用户名] [条数] - 查询最近N条(默认5)\n"
            "/今日战报 [用户名] - 快捷查询当日战报\n"
            "/队伍 - 按队伍阵容查询，支持 _ 通配；前4位为Striker，后2位为Special\n"
            "  单参数: /队伍 [4-6字]；可用 学生单字简称/t(坦克)/c(输出)/3-8(射程350-850)\n"
            "  多参数: /队伍 [st1] [st2] [st3] [st4] [sp1?] [sp2?] [n?]\n"
            "/清空战报 [玩家名称] - 清空当前群组/会话战报；带玩家名时仅清空该玩家"
        )
        yield event.plain_result(help_text)

    @filter.command("记录")
    async def record_battle(self, event: AstrMessageEvent):
        event.stop_event()
        yield event.plain_result("正在识别图片")

        image_data_urls = await self._extract_image_data_urls(event)
        if not image_data_urls:
            yield event.plain_result("记录失败：未检测到图片或图片无法下载，请确认网络或图片格式。")
            return

        player_name_context = await self._get_player_name_context_for_record(
            event=event,
            image_data_urls=image_data_urls,
            user_text=event.message_str,
        )

        ok, reason = await self._recognize_and_record_core(
            event=event,
            image_data_urls=image_data_urls,
            user_text=event.message_str,
            silent=False,
            player_name_context=player_name_context,
        )
        if not ok:
            yield event.plain_result(f"记录失败：{reason}。")
            return

        async for msg in self._reply_recorded(event):
            yield msg

    def _build_student_render_info(self, student_id: str, raw_name: str = "") -> dict[str, str]:
        sid = str(student_id)
        display_name = str(raw_name).strip() if str(raw_name).strip() else self._student_dict.get(sid, sid)
        avatar_path = self._plugin_dir / "avatars" / f"{sid}.webp"
        return {"id": sid, "name": display_name, "avatar_path": avatar_path.as_posix() if avatar_path.exists() else ""}

    def _parse_side_render_info(self, side_data: dict) -> dict[str, Any]:
        strikers: list[dict[str, str]] = []
        specials: list[dict[str, str]] = []
        for key in ["st1", "st2", "st3", "st4"]:
            val = side_data.get(key)
            raw_name = str(side_data.get(f"{key}_name", "") or "").strip()
            if val and str(val).isdigit():
                strikers.append(self._build_student_render_info(str(val), raw_name))
            elif raw_name:
                strikers.append({"id": "", "name": raw_name, "avatar_path": ""})
            else:
                strikers.append({"id": "", "name": "未知", "avatar_path": ""})
        for key in ["sp1", "sp2"]:
            val = side_data.get(key)
            raw_name = str(side_data.get(f"{key}_name", "") or "").strip()
            if val and str(val).isdigit():
                specials.append(self._build_student_render_info(str(val), raw_name))
            elif raw_name:
                specials.append({"id": "", "name": raw_name, "avatar_path": ""})
            else:
                specials.append({"id": "", "name": "未知", "avatar_path": ""})
        return {"name": side_data.get("name", "未知"), "status": bool(side_data.get("status", False)), "strikers": strikers, "specials": specials}

    def _draw_student_slot(self, canvas: Image.Image, draw: ImageDraw.ImageDraw, member: dict[str, str], x: int, y: int, slot_w: int, slot_h: int, name_font: ImageFont.FreeTypeFont | ImageFont.ImageFont):
        layout = self._RENDER_LAYOUT
        theme = self._RENDER_THEME
        draw.rounded_rectangle(
            (x, y, x + slot_w, y + slot_h),
            radius=layout["slot_radius"],
            fill=theme["slot_bg"],
            outline=theme["slot_border"],
            width=1,
        )
        avatar_size = layout["avatar_size"]
        avatar_x = x + (slot_w - avatar_size) // 2
        avatar_y = y + layout["avatar_top_pad"]
        avatar = None
        avatar_path = member.get("avatar_path", "")
        if avatar_path:
            try:
                avatar = Image.open(avatar_path).convert("RGB")
                avatar = ImageOps.fit(avatar, (avatar_size, avatar_size), method=Image.Resampling.LANCZOS)
            except Exception as e:
                logger.warning(f"头像加载失败: {avatar_path}, {e}")
        if avatar is None:
            avatar = Image.new("RGB", (avatar_size, avatar_size), theme["placeholder_bg"])
            qd = ImageDraw.Draw(avatar)
            qd.text((18, 14), "?", fill=theme["placeholder_text"], font=self._load_font(22, True))
        canvas.paste(avatar, (avatar_x, avatar_y))

        name = str(member.get("name", "未知"))
        if len(name) > layout["name_max_len"]:
            name = name[: layout["name_trim_len"]] + "…"
        tw = draw.textlength(name, font=name_font)
        draw.text((x + (slot_w - tw) / 2, y + layout["name_y"]), name, font=name_font, fill=theme["slot_name_text"])
    def _status_colors(self, is_attack: bool, status: bool) -> tuple[tuple[int, int, int], tuple[int, int, int]]:
        theme = self._RENDER_THEME
        if is_attack:
            if status:
                return theme["attack_win_light"], theme["attack_win_dark"]
            return theme["status_lose_light"], theme["status_lose_dark"]
        if status:
            return theme["defend_win_light"], theme["defend_win_dark"]
        return theme["status_lose_light"], theme["status_lose_dark"]
    def _draw_side_header(self, draw: ImageDraw.ImageDraw, x: int, y: int, w: int, h: int, side_name: str, status: bool, is_attack: bool, font: ImageFont.FreeTypeFont | ImageFont.ImageFont):
        layout = self._RENDER_LAYOUT
        theme = self._RENDER_THEME
        _, c2 = self._status_colors(is_attack, status)
        draw.rounded_rectangle((x, y, x + w, y + h), radius=layout["status_radius"], fill=c2)
        draw.rectangle((x, y + h // 2, x + w, y + h), fill=c2)
        status_text = "Win" if status else "Lose"
        text = f"{side_name}  {status_text}"
        tw = draw.textlength(text, font=font)
        tx = x + max(layout["status_text_min_pad"], (w - tw) / 2)
        draw.text((tx, y), text, font=font, fill=theme["status_header_text"])
    def _draw_side_students(self, canvas: Image.Image, draw: ImageDraw.ImageDraw, side: dict[str, Any], x: int, y: int, w: int):
        layout = self._RENDER_LAYOUT
        members = list(side.get("strikers", [])) + list(side.get("specials", []))
        while len(members) < 6:
            members.append({"name": "空", "avatar_path": ""})
        members = members[:6]
        slot_w = layout["slot_w"]
        slot_h = layout["slot_h"]
        gap = layout["slot_gap"]
        total_w = slot_w * 6 + gap * 5
        start_x = x + max(0, (w - total_w) // 2)
        name_font = self._load_font(13)
        for i in range(6):
            px = start_x + i * (slot_w + gap)
            self._draw_student_slot(canvas, draw, members[i], px, y, slot_w, slot_h, name_font)
    def _render_records_image(self, records: list[dict[str, Any]], query_user: str, query_date: str) -> Path:
        layout = self._RENDER_LAYOUT
        theme = self._RENDER_THEME

        width = layout["width"]
        margin = layout["margin"]
        header_h = layout["header_h"]
        card_h = layout["card_h"]
        card_gap = layout["card_gap"]
        height = margin * 2 + header_h + len(records) * (card_h + card_gap)

        image = Image.new("RGB", (width, height), theme["canvas_bg"])
        draw = ImageDraw.Draw(image)

        title_font = self._load_font(28, True)
        meta_font = self._load_font(15)
        side_font = self._load_font(22, True)
        time_font = self._load_font(14)

        draw.rounded_rectangle(
            (margin, margin, width - margin, margin + header_h),
            radius=layout["header_radius"],
            fill=theme["header_bg"],
        )
        draw.text(
            (margin + layout["header_title_x"], margin + layout["header_title_y"]),
            "BA PVP 战报",
            font=title_font,
            fill=theme["header_title_text"],
        )
        meta_text = f"用户: {query_user or '全部'}   日期: {query_date or '全部'}   条数: {len(records)}"
        draw.text(
            (margin + layout["header_meta_x"], margin + layout["header_meta_y"]),
            meta_text,
            font=meta_font,
            fill=theme["header_meta_text"],
        )

        y = margin + header_h + 8
        half_gap = layout["half_gap"]
        half_w = (width - margin * 2 - half_gap) // 2
        for rec in records:
            draw.rounded_rectangle(
                (margin, y, width - margin, y + card_h),
                radius=layout["card_radius"],
                fill=theme["card_bg"],
                outline=theme["card_border"],
                width=1,
            )
            draw.text(
                (margin + layout["card_time_x"], y + layout["card_time_y"]),
                f"时间: {rec.get('time', '未知')}",
                font=time_font,
                fill=theme["card_time_text"],
            )
            left_x = margin + layout["card_inner_left_x"]
            right_x = left_x + half_w + half_gap
            top_y = y + layout["card_inner_top_y"]

            attack = rec.get("attack", {})
            defend = rec.get("defend", {})
            self._draw_side_header(
                draw,
                left_x,
                top_y,
                half_w,
                layout["status_header_h"],
                str(attack.get("name") or "未知"),
                bool(attack.get("status")),
                True,
                side_font,
            )
            self._draw_side_header(
                draw,
                right_x,
                top_y,
                half_w,
                layout["status_header_h"],
                str(defend.get("name") or "未知"),
                bool(defend.get("status")),
                False,
                side_font,
            )
            self._draw_side_students(image, draw, attack, left_x, top_y + 42, half_w)
            self._draw_side_students(image, draw, defend, right_x, top_y + 42, half_w)
            y += card_h + card_gap

        out_dir = self._data_dir / "rendered_reports"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"report_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.png"
        image.save(out_file.as_posix(), format="PNG")
        return out_file
    def _battle_date_str(self, dt_local: datetime) -> str:
        if dt_local.hour < 3:
            dt_local = dt_local - timedelta(days=1)
        return dt_local.strftime("%Y-%m-%d")

    @filter.command("查询战报")
    async def query_records_command(self, event: AstrMessageEvent, username: str = "", date_str: str = ""):
        if date_str and not re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
            yield event.plain_result("日期格式错误，请使用 YYYY-MM-DD 格式。")
            return
        async for r in self._do_query_and_render(event, username, date_str):
            yield r

    @filter.command("今日战报")
    async def today_records_command(self, event: AstrMessageEvent, username: str = ""):
        tz_local = self._query_timezone()
        now_local = datetime.now(tz_local)
        today_str = self._battle_date_str(now_local)
        async for r in self._do_query_and_render(event, username, today_str):
            yield r

    @filter.command("最近战报")
    async def recent_records_command(self, event: AstrMessageEvent, username: str = "", count: str = "5"):
        user = str(username or "").strip()
        raw_count = str(count or "").strip()
        if user.isdigit() and (not raw_count or raw_count == "5"):
            raw_count = user
            user = ""
        try:
            limit = int(raw_count or "5")
        except ValueError:
            yield event.plain_result("条数格式错误，请输入正整数，例如：/最近战报 小明 5")
            return
        if limit <= 0:
            yield event.plain_result("条数必须大于 0。")
            return
        if limit > 50:
            limit = 50
        async for r in self._do_query_and_render(event, user, "", limit):
            yield r

    @filter.command("队伍")
    async def team_query_command(
        self,
        event: AstrMessageEvent,
        a1: str = "",
        a2: str = "",
        a3: str = "",
        a4: str = "",
        a5: str = "",
        a6: str = "",
        a7: str = "",
    ):
        tokens, limit, err = self._parse_team_query_tokens([a1, a2, a3, a4, a5, a6, a7])
        if err:
            yield event.plain_result(err)
            return

        filters: list[set[str] | None] = []
        for idx, token in enumerate(tokens):
            allowed, ferr = self._resolve_team_token_ids(token, idx)
            if ferr:
                yield event.plain_result(f"槽位{idx + 1}参数错误：{ferr}")
                return
            filters.append(allowed)

        async for r in self._do_team_query_and_render(event, tokens, filters, limit):
            yield r

    @filter.command("清空战报")
    async def clear_records_command(self, event: AstrMessageEvent, username: str = ""):
        user = str(username or "").strip()

        record_db = self._get_record_db(event)
        if not record_db.exists():
            yield event.plain_result("当前群组/会话暂无战报记录。")
            return

        if not user:
            removed_rows = 0
            removed_battles = 0
            with sqlite3.connect(record_db) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                cur.execute("SELECT id, result_json FROM battle_records")
                rows = cur.fetchall()
                removed_rows = len(rows)
                for row in rows:
                    try:
                        parsed = json.loads(row["result_json"])
                        if isinstance(parsed, list):
                            removed_battles += len(parsed)
                    except Exception:
                        continue
                conn.execute("DELETE FROM battle_records")
                conn.commit()

            if removed_rows <= 0:
                yield event.plain_result("当前群组/会话暂无战报记录。")
                return

            yield event.plain_result(
                f"已清空当前群组/会话全部战报：删除 {removed_battles} 条，对应记录 {removed_rows} 行。"
            )
            return

        uname = user.lower()
        removed_battles = 0
        touched_rows = 0
        deleted_rows = 0

        with sqlite3.connect(record_db) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("SELECT id, result_json FROM battle_records")
            rows = cur.fetchall()

            for row in rows:
                try:
                    parsed = json.loads(row["result_json"])
                except Exception:
                    continue
                if not isinstance(parsed, list):
                    continue

                kept: list[dict[str, Any]] = []
                removed_in_row = 0
                for battle in parsed:
                    if not isinstance(battle, dict):
                        continue
                    b_attack_name = str(battle.get("attack", {}).get("name", ""))
                    b_defend_name = str(battle.get("defend", {}).get("name", ""))
                    is_match = uname in b_attack_name.lower() or uname in b_defend_name.lower()
                    if is_match:
                        removed_in_row += 1
                    else:
                        kept.append(battle)

                if removed_in_row <= 0:
                    continue

                removed_battles += removed_in_row
                touched_rows += 1
                if kept:
                    conn.execute(
                        "UPDATE battle_records SET result_json = ? WHERE id = ?",
                        (json.dumps(kept, ensure_ascii=False), row["id"]),
                    )
                else:
                    conn.execute("DELETE FROM battle_records WHERE id = ?", (row["id"],))
                    deleted_rows += 1

            conn.commit()

        if removed_battles <= 0:
            yield event.plain_result(f"未找到玩家 {user} 的战报记录。")
            return

        yield event.plain_result(
            f"已清空玩家 {user} 的战报：删除 {removed_battles} 条，对应记录 {touched_rows} 行（整行删除 {deleted_rows} 行）。"
        )

    async def _do_team_query_and_render(
        self,
        event: AstrMessageEvent,
        tokens: list[str],
        filters: list[set[str] | None],
        limit: int,
    ):
        record_db = self._get_record_db(event)
        if not record_db.exists():
            yield event.plain_result("当前群组/会话暂无战报记录。")
            return

        tz_local = self._query_timezone()
        with sqlite3.connect(record_db) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("SELECT * FROM battle_records ORDER BY created_at DESC LIMIT 400")
            rows = cur.fetchall()

        records_to_render: list[dict[str, Any]] = []
        for row in rows:
            utc_dt = datetime.fromisoformat(row["created_at"])
            local_dt = utc_dt.astimezone(tz_local)

            try:
                parsed = json.loads(row["result_json"])
            except Exception:
                continue
            if not isinstance(parsed, list):
                continue

            for battle in parsed:
                if not isinstance(battle, dict):
                    continue

                attack = battle.get("attack", {}) if isinstance(battle.get("attack", {}), dict) else {}
                defend = battle.get("defend", {}) if isinstance(battle.get("defend", {}), dict) else {}

                matched = self._side_matches_team_filters(attack, filters) or self._side_matches_team_filters(defend, filters)
                if not matched:
                    continue

                records_to_render.append(
                    {
                        "time": local_dt.strftime("%Y-%m-%d %H:%M:%S"),
                        "attack": self._parse_side_render_info(attack),
                        "defend": self._parse_side_render_info(defend),
                    }
                )
                if len(records_to_render) >= limit:
                    break
            if len(records_to_render) >= limit:
                break

        if not records_to_render:
            yield event.plain_result("未找到符合队伍条件的战报记录。")
            return

        try:
            q = "".join(tokens)
            out_file = await asyncio.to_thread(self._render_records_image, records_to_render, f"队伍:{q}", "")
            yield event.image_result(out_file.as_posix())
        except Exception as e:
            logger.error(f"渲染队伍查询图片失败: {e}")
            yield event.plain_result(f"渲染图片失败: {e}")

    async def _do_query_and_render(self, event: AstrMessageEvent, username: str, date_str: str, limit: int | None = None):
        record_db = self._get_record_db(event)
        if not record_db.exists():
            yield event.plain_result("当前群组/会话暂无战报记录。")
            return

        tz_local = self._query_timezone()
        with sqlite3.connect(record_db) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            fetch_limit = 200 if limit else 50
            cur.execute(f"SELECT * FROM battle_records ORDER BY created_at DESC LIMIT {fetch_limit}")
            rows = cur.fetchall()

        records_to_render: list[dict[str, Any]] = []
        for row in rows:
            utc_dt = datetime.fromisoformat(row["created_at"])
            local_dt = utc_dt.astimezone(tz_local)
            battle_date = self._battle_date_str(local_dt)
            if date_str and battle_date != date_str:
                continue

            try:
                parsed = json.loads(row["result_json"])
            except Exception:
                continue

            for battle in parsed:
                if not isinstance(battle, dict):
                    continue
                b_attack_name = str(battle.get("attack", {}).get("name", ""))
                b_defend_name = str(battle.get("defend", {}).get("name", ""))
                if username:
                    uname = username.lower()
                    if uname not in b_attack_name.lower() and uname not in b_defend_name.lower():
                        continue

                records_to_render.append(
                    {
                        "time": local_dt.strftime("%Y-%m-%d %H:%M:%S"),
                        "attack": self._parse_side_render_info(battle.get("attack", {})),
                        "defend": self._parse_side_render_info(battle.get("defend", {})),
                    }
                )

                if limit and len(records_to_render) >= limit:
                    break
            if limit and len(records_to_render) >= limit:
                break

        if not records_to_render:
            yield event.plain_result("未找到符合条件的战报记录。")
            return

        try:
            out_file = await asyncio.to_thread(self._render_records_image, records_to_render, username, date_str)
            yield event.image_result(out_file.as_posix())
        except Exception as e:
            logger.error(f"渲染战报图片失败: {e}")
            yield event.plain_result(f"渲染图片失败: {e}")

    async def terminate(self):
        if self._daily_update_task and not self._daily_update_task.done():
            self._daily_update_task.cancel()
            try:
                await self._daily_update_task
            except asyncio.CancelledError:
                pass
        logger.info("BAPvpRecorderPlugin terminated")
