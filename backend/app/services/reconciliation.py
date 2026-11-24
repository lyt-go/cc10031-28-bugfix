"""测点时段对账共享口径。

区间重采工作台（range_replay）与批量上送入口（upload_batch）必须按同一测点、
同一申请时段对账，保证：

1. 回放只交付申请时段 [起始时刻, 截止时刻] 内的观测，时段外样本只能在原上送
   （edge_samples）中回查，不能记成这次重采交付；
2. 每个观测在一次回放结果里只有唯一一行，重试后同一观测仍只有一条；
3. 回放行记录实际采用的采样出处（原上送/标定文件）与量纲，标定更正发生在回放
   生成成果之前时，回放读到的是更正后的有效值。
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any


def parse_ts(value: Any) -> datetime | None:
    """解析采样时间戳；无法解析时返回 None（交由字符串兜底比较）。"""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _ts_key(value: Any) -> tuple[int, Any]:
    parsed = parse_ts(value)
    if parsed is not None:
        # 统一去掉时区影响后再比较，避免 aware/naive 混用时抛 TypeError
        return (0, parsed.replace(tzinfo=None))
    return (1, str(value or ""))


def in_window(timestamp: Any, start: Any, end: Any) -> bool:
    """闭区间判定：起始时刻 <= 采样时刻 <= 截止时刻。

    优先按真实时间比较；时间戳无法解析时退化为 ISO 字符串字典序比较；
    任一边界缺失视为该侧不限制。
    """
    if start not in (None, ""):
        if _ts_key(timestamp) < _ts_key(start):
            return False
    if end not in (None, ""):
        if _ts_key(timestamp) > _ts_key(end):
            return False
    return True


def source_checksum(record: dict[str, Any]) -> str:
    """按采样出处身份字段计算校验值，供恢复后按原出处复验。"""
    identity = json.dumps(
        {
            "point": record.get("point"),
            "timestamp": record.get("timestamp"),
            "raw_value_mV": record.get("raw_value_mV", record.get("raw_value")),
            "calibrated_value_V": record.get("calibrated_value_V"),
        },
        ensure_ascii=False,
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return rows


def split_by_window(
    records: list[dict[str, Any]], start: Any, end: Any
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """按申请时段把候选样本拆成（时段内、时段外）两组。"""
    inside = [r for r in records if in_window(r.get("timestamp"), start, end)]
    outside = [r for r in records if not in_window(r.get("timestamp"), start, end)]
    return inside, outside


def dedupe_observations(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """同一测点同一采样时刻只保留一条观测，保证交付行唯一。"""
    seen: dict[Any, dict[str, Any]] = {}
    order: list[Any] = []
    for r in records:
        key = _ts_key(r.get("timestamp"))
        if key not in seen:
            seen[key] = r
            order.append(key)
    return [seen[k] for k in order]


def _latest_confirmed_calibrations(
    calibrated: list[dict[str, Any]], point_key: str
) -> dict[Any, dict[str, Any]]:
    """以采样时刻为键取最后一次写出的标定值（标定更正后文件中后写者生效）。"""
    latest: dict[Any, dict[str, Any]] = {}
    for r in calibrated:
        if r.get("point") not in (None, point_key):
            continue
        latest[_ts_key(r.get("timestamp"))] = r
    return latest


def reconcile_point_window(
    data_root: Path, point_key: str, start: Any, end: Any
) -> dict[str, Any]:
    """按同一测点 + 申请时段完成原始实收对账。

    返回：
      - raw_received：原上送边缘文件中该时段的原始实收观测（可回查出处）；
      - outside_window：时段外样本（仍可在原上送中回查，不属本次重采交付）；
      - latest_calibrations：该测点各采样时刻当前生效的标定观测（已含更正）；
      - calibrated_in_window：当前生效标定中落在申请时段内的观测。
    """
    edge_file = data_root / "edge_samples" / f"{point_key}.jsonl"
    cal_file = data_root / "calibrated" / f"{point_key}_calibrated.jsonl"
    edge_records = _load_jsonl(edge_file)
    cal_records = _load_jsonl(cal_file)

    raw_inside, raw_outside = split_by_window(edge_records, start, end)
    raw_inside = dedupe_observations(raw_inside)
    latest_cal = _latest_confirmed_calibrations(cal_records, point_key)
    cal_inside = dedupe_observations(
        [r for r in latest_cal.values() if in_window(r.get("timestamp"), start, end)]
    )
    return {
        "point": point_key,
        "start": start,
        "end": end,
        "edge_file": str(edge_file),
        "calibrated_file": str(cal_file),
        "raw_received": raw_inside,
        "outside_window": raw_outside,
        "latest_calibrations": latest_cal,
        "calibrated_in_window": cal_inside,
    }


def build_replay_records(
    data_root: Path,
    point_key: str,
    start: Any,
    end: Any,
    *,
    replay_id: str,
    reason: str,
    overwrite_mode: str,
    replayed_at: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """生成符合合规口径的回放交付行，并同时返回时段外样本清单。

    交付行只包含申请时段内的唯一观测；每行携带实际采用的采样出处与量纲。
    标定文件中存在该时刻的已确认观测时采用标定后的 V 值（回放前确认的更正
    自然生效），否则回退原上送的 mV 原始值并注明出处。
    """
    audit = reconcile_point_window(data_root, point_key, start, end)
    latest_cal = audit["latest_calibrations"]

    delivered: list[dict[str, Any]] = []
    for raw in audit["raw_received"]:
        ts = raw.get("timestamp")
        cal_row = latest_cal.get(_ts_key(ts))
        if cal_row is not None:
            value = cal_row.get("calibrated_value_V")
            unit = "V"
            source_kind = "calibrated"
            source_file = audit["calibrated_file"]
            calibrate_id = cal_row.get("calibrate_id")
            scale = cal_row.get("scale")
            offset = cal_row.get("offset")
        else:
            value = raw.get("raw_value_mV", raw.get("raw_value"))
            unit = "mV"
            source_kind = "edge_upload"
            source_file = audit["edge_file"]
            calibrate_id = None
            scale = None
            offset = None
        source_record = {
            "point": point_key,
            "timestamp": ts,
            "raw_value_mV": raw.get("raw_value_mV", raw.get("raw_value")),
            "calibrated_value_V": cal_row.get("calibrated_value_V") if cal_row else None,
        }
        delivered.append({
            "replay_id": replay_id,
            "point": point_key,
            "reason": reason,
            "overwrite_mode": overwrite_mode,
            "original_timestamp": ts,
            "replay_value": value,
            "replay_unit": unit,
            "source_kind": source_kind,
            "source_file": source_file,
            "source_calibrate_id": calibrate_id,
            "calibration_scale": scale,
            "calibration_offset": offset,
            "raw_value_mV": raw.get("raw_value_mV", raw.get("raw_value")),
            "source_checksum": source_checksum(source_record),
            "replayed_at": replayed_at,
        })
    return delivered, audit["outside_window"]


def read_replay_result(data_root: Path, replay_id: str) -> list[dict[str, Any]]:
    """读取某次回放实际写出的成果行，供批量故障重采核对。"""
    return _load_jsonl(
        data_root / "replayed_ranges" / f"{replay_id}_replay.jsonl"
    )


def verify_replay_sources(
    data_root: Path, point_key: str, replay_rows: list[dict[str, Any]]
) -> dict[str, Any]:
    """按原出处复验已取结果：重算每行出处校验值并核对有效值与量纲。

    恢复后可凭此确认回放交付的观测仍与原上送/标定文件一致；任一对不上即标记
    校验失败，防止范围外样本或过期标定值混入交付。
    """
    edge = {_ts_key(r.get("timestamp")): r
            for r in _load_jsonl(data_root / "edge_samples" / f"{point_key}.jsonl")}
    cal = _latest_confirmed_calibrations(
        _load_jsonl(data_root / "calibrated" / f"{point_key}_calibrated.jsonl"),
        point_key,
    )
    checked = []
    ok = True
    seen_ts: set[Any] = set()
    for row in replay_rows:
        key = _ts_key(row.get("original_timestamp"))
        unique = key not in seen_ts
        seen_ts.add(key)
        raw = edge.get(key)
        cal_row = cal.get(key)
        source_record = {
            "point": point_key,
            "timestamp": row.get("original_timestamp"),
            "raw_value_mV": (raw or {}).get("raw_value_mV", (raw or {}).get("raw_value")),
            "calibrated_value_V": (cal_row or {}).get("calibrated_value_V"),
        }
        checksum_ok = (
            raw is not None
            and row.get("source_checksum") == source_checksum(source_record)
        )
        if cal_row is not None:
            value_ok = row.get("replay_value") == cal_row.get("calibrated_value_V")
            unit_ok = row.get("replay_unit") == "V"
            source_ok = row.get("source_kind") == "calibrated"
        else:
            value_ok = row.get("replay_value") == (raw or {}).get(
                "raw_value_mV", (raw or {}).get("raw_value")
            )
            unit_ok = row.get("replay_unit") == "mV"
            source_ok = row.get("source_kind") == "edge_upload"
        row_ok = unique and checksum_ok and value_ok and unit_ok and source_ok
        ok = ok and row_ok
        checked.append({
            "original_timestamp": row.get("original_timestamp"),
            "replay_value": row.get("replay_value"),
            "replay_unit": row.get("replay_unit"),
            "source_kind": row.get("source_kind"),
            "source_calibrate_id": row.get("source_calibrate_id"),
            "unique_observation": unique,
            "checksum_ok": checksum_ok,
            "value_ok": value_ok,
            "unit_ok": unit_ok,
            "source_ok": source_ok,
            "verified": row_ok,
        })
    return {
        "point": point_key,
        "rows_checked": len(replay_rows),
        "all_verified": ok,
        "checks": checked,
    }
