"""区间重采时段对账专项测试。

复现题述故障场景：原数据有 10:05、10:20、10:40 三个观测，只申请 10:10~10:30；
回放前标定端把 10:20 的值从先确认的 0.11V 更正为 0.12V（上送端原始 120mV 仍
可核实）。验证：
- 回放只交付申请内观测，范围外样本不进成果、重试后同一观测仍只有一条；
- 回放行记录实际采用的采样出处与量纲，标定更正值在回放时生效；
- 批量上送入口按同一测点时段给出原始实收与已被回放采用的记录；
- 故障恢复后可按原出处复验已取结果。
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest


@pytest.fixture()
def data_root():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        with patch("app.services.base.DATA_ROOT", root):
            import app.services.base as base
            import app.services.range_replay as rr
            import app.services.upload_batch as ub
            import app.services.reconciliation as rc
            for mod in (base, rr, ub, rc):
                mod.DATA_ROOT = root
            yield root


def _write_jsonl(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _seed_point_data(root: Path, point: str = "SENS-R1"):
    """落盘上送端可核实的原始观测：10:05、10:20、10:40，10:20 原值 120mV。"""
    edge_rows = [
        {"shard": "EDGE-R1", "point": point, "sample_index": 1,
         "timestamp": "2026-09-01T10:05:00", "raw_value_mV": 100.0, "unit": "mV"},
        {"shard": "EDGE-R1", "point": point, "sample_index": 2,
         "timestamp": "2026-09-01T10:20:00", "raw_value_mV": 120.0, "unit": "mV"},
        {"shard": "EDGE-R1", "point": point, "sample_index": 3,
         "timestamp": "2026-09-01T10:40:00", "raw_value_mV": 140.0, "unit": "mV"},
    ]
    _write_jsonl(root / "edge_samples" / f"{point}.jsonl", edge_rows)
    return edge_rows


def _calibrated_rows(point: str, value_1020: float, calibrate_id: str):
    return [
        {"point": point, "timestamp": "2026-09-01T10:05:00",
         "raw_value_mV": 100.0, "calibrated_value_V": 0.1, "scale": 0.001,
         "offset": 0, "calibrate_id": calibrate_id},
        {"point": point, "timestamp": "2026-09-01T10:20:00",
         "raw_value_mV": 120.0, "calibrated_value_V": value_1020, "scale": 0.001,
         "offset": 0, "calibrate_id": calibrate_id},
        {"point": point, "timestamp": "2026-09-01T10:40:00",
         "raw_value_mV": 140.0, "calibrated_value_V": 0.14, "scale": 0.001,
         "offset": 0, "calibrate_id": calibrate_id},
    ]


def _make_replay(reason: str = "传感器故障重采", replay_no: str = "RANG-R1"):
    from app.store import store
    rows = store.rows("range_replay")
    entry = {
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "重采编号": replay_no,
        "测点编号": "SENS-R1",
        "起始时刻": "2026-09-01T10:10:00",
        "截止时刻": "2026-09-01T10:30:00",
        "重采原因": reason,
        "覆盖方式": "追加",
        "重采点数": "0",
        "重采水位": "0",
        "status": "待重采", "pending": True, "abnormal": False,
    }
    rows.append(entry)
    return entry


def _make_batch():
    from app.store import store
    rows = store.rows("upload_batch")
    entry = {
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "批次编号": "UPLO-R1",
        "分片范围": "SENS-R1",
        "测点数量": 1,
        "记录条数": "3",
        "压缩方式": "gzip",
        "批次校验值": "seed",
        "上送时长": "0",
        "批次水位": "0",
        "status": "待上送", "pending": True, "abnormal": False,
    }
    rows.append(entry)
    return entry


class TestReplayWindowReconciliation:
    def test_replay_only_delivers_in_window_unique_observation(self, data_root):
        from app.services.range_replay import RangeReplayService
        from app.services.reconciliation import read_replay_result

        _seed_point_data(data_root)
        replay = _make_replay()
        svc = RangeReplayService()

        result = svc.replay_range(replay["id"])

        # 只申请 10:10~10:30，范围内只有 10:20 一条
        assert result["records_replayed"] == 1
        assert result["delivered_timestamps"] == ["2026-09-01T10:20:00"]
        # 10:05、10:40 两条范围外样本被识别但不交付
        assert result["out_of_window_samples"] == 2
        assert set(result["out_of_window_timestamps"]) == {
            "2026-09-01T10:05:00", "2026-09-01T10:40:00",
        }

        rows = read_replay_result(data_root, replay["重采编号"])
        assert len(rows) == 1
        line = rows[0]
        # 无标定文件时回退原上送 mV 原值 120，并注明出处与量纲
        assert line["replay_value"] == 120.0
        assert line["replay_unit"] == "mV"
        assert line["source_kind"] == "edge_upload"
        assert line["original_timestamp"] == "2026-09-01T10:20:00"
        assert line["source_checksum"]
        assert result["source_verification"]["all_verified"] is True

    def test_retry_replaces_stale_result_and_keeps_single_row(self, data_root):
        """先制造含范围外样本的错误成果，重试后错误记录不得残留、观测仍唯一。"""
        from app.services.range_replay import RangeReplayService
        from app.services.reconciliation import read_replay_result

        _seed_point_data(data_root)
        replay = _make_replay()
        output = data_root / "replayed_ranges" / f"{replay['重采编号']}_replay.jsonl"
        output.parent.mkdir(parents=True, exist_ok=True)
        # 模拟旧版本错误地把三条全写出（追加模式残留）
        with open(output, "w", encoding="utf-8") as f:
            for ts in ("10:05:00", "10:20:00", "10:40:00"):
                f.write(json.dumps({"original_timestamp": f"2026-09-01T{ts}"}) + "\n")

        result = RangeReplayService().replay_range(replay["id"])

        assert result["records_replayed"] == 1
        rows = read_replay_result(data_root, replay["重采编号"])
        assert len(rows) == 1
        assert rows[0]["original_timestamp"] == "2026-09-01T10:20:00"

        # 再次重试仍是同一唯一行（幂等，不追加重复行）
        RangeReplayService().replay_range(replay["id"])
        rows = read_replay_result(data_root, replay["重采编号"])
        assert len(rows) == 1

    def test_calibration_correction_confirmed_before_replay_takes_effect(self, data_root):
        """标定更正在回放生成成果之前确认：回放采用 0.12V 而非先前的 0.11V。"""
        from app.services.range_replay import RangeReplayService
        from app.services.reconciliation import read_replay_result

        point = "SENS-R1"
        _seed_point_data(data_root)
        cal_file = data_root / "calibrated" / f"{point}_calibrated.jsonl"
        # 标定端先保存一份先前确认的 0.11V 记录
        _write_jsonl(cal_file, _calibrated_rows(point, 0.11, "UNIT-OLD"))
        # 回放生成成果之前确认更正：整文件重写为 0.12V（同一时刻后写者生效）
        _write_jsonl(cal_file, _calibrated_rows(point, 0.12, "UNIT-NEW"))

        replay = _make_replay()
        result = RangeReplayService().replay_range(replay["id"])

        assert result["records_replayed"] == 1
        rows = read_replay_result(data_root, replay["重采编号"])
        line = rows[0]
        assert line["replay_value"] == 0.12
        assert line["replay_unit"] == "V"
        assert line["source_kind"] == "calibrated"
        assert line["source_calibrate_id"] == "UNIT-NEW"
        # 合规回放同时保留原始值与出处校验值，可回溯到 120mV 原上送
        assert line["raw_value_mV"] == 120.0
        assert line["source_checksum"]
        assert result["source_verification"]["all_verified"] is True

    def test_batch_upload_reconciles_same_point_window(self, data_root):
        """批量上送端按重采选中的实际观测核对来源与采用记录。"""
        from app.services.range_replay import RangeReplayService
        from app.services.upload_batch import UploadBatchService

        point = "SENS-R1"
        _seed_point_data(data_root)
        cal_file = data_root / "calibrated" / f"{point}_calibrated.jsonl"
        _write_jsonl(cal_file, _calibrated_rows(point, 0.12, "UNIT-NEW"))

        replay = _make_replay()
        batch = _make_batch()
        RangeReplayService().replay_range(replay["id"])

        report = UploadBatchService().reconcile_replay_window(batch["id"], replay["id"])

        # 该时段原始实收只有 10:20 一条（120mV 可核实）
        assert report["raw_received_count"] == 1
        raw = report["raw_received"][0]
        assert raw["timestamp"] == "2026-09-01T10:20:00"
        assert raw["raw_value_mV"] == 120.0
        assert raw["adopted_by_replay"] is True
        # 已被回放采用的记录带实际出处与量纲（标定后的 0.12V）
        assert report["adopted_count"] == 1
        adopted = report["adopted_records"][0]
        assert adopted["replay_value"] == 0.12
        assert adopted["replay_unit"] == "V"
        assert adopted["source_kind"] == "calibrated"
        assert adopted["source_calibrate_id"] == "UNIT-NEW"
        # 范围外两条可回查但不计入本次交付
        assert report["out_of_window_samples"] == 2
        assert report["source_verification"]["all_verified"] is True
        # 对账凭证落盘
        reconcile_log = data_root / "upload_batches" / "UPLO-R1_replay_reconcile.jsonl"
        assert reconcile_log.exists()

    def test_reconcile_rejects_different_point(self, data_root):
        from app.services.upload_batch import UploadBatchService
        from app.store import store

        _seed_point_data(data_root)
        replay = _make_replay()
        rows = store.rows("upload_batch")
        other = {
            "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
            "批次编号": "UPLO-OTHER", "分片范围": "SENS-OTHER", "测点数量": 1,
            "记录条数": "0", "压缩方式": "gzip", "批次校验值": "",
            "上送时长": "0", "批次水位": "0",
            "status": "待上送", "pending": True, "abnormal": False,
        }
        rows.append(other)

        report = UploadBatchService().reconcile_replay_window(other["id"], replay["id"])
        assert "error" in report
        assert "同一测点" in report["error"]

    def test_batch_faulty_replay_reads_actual_results_for_verification(self, data_root):
        """批量故障重采读取实际各项回放结果，并跨过标定完成时刻核对。"""
        from app.services.range_replay import RangeReplayService

        point = "SENS-R1"
        _seed_point_data(data_root)
        cal_file = data_root / "calibrated" / f"{point}_calibrated.jsonl"
        _write_jsonl(cal_file, _calibrated_rows(point, 0.11, "UNIT-OLD"))
        replay = _make_replay(reason="传感器故障重采", replay_no="RANG-FAULT")

        # 批量重采开始后、回放生成成果之前，标定更正确认
        _write_jsonl(cal_file, _calibrated_rows(point, 0.12, "UNIT-NEW"))

        summary = RangeReplayService().batch_replay_faulty()

        # store 为进程级内存仓库，可能含其他用例的故障任务，这里只核对本任务
        task = next(r for r in summary["results"] if r.get("replay_id") == "RANG-FAULT")
        assert task["records_replayed"] == 1
        assert task["out_of_window_samples"] == 2
        assert task["source_verification"]["all_verified"] is True
        # 批量入口读到的实际回放行就是更正后的唯一有效行
        line = task["replayed_rows"][0]
        assert line["replay_value"] == 0.12
        assert line["replay_unit"] == "V"
        assert line["source_calibrate_id"] == "UNIT-NEW"

        batch_log = data_root / "replayed_ranges" / "batch_replay_summary.jsonl"
        logged = [json.loads(line) for line in batch_log.read_text().splitlines() if line.strip()]
        logged_task = next(t for t in logged[-1]["tasks"] if t["replay_id"] == "RANG-FAULT")
        assert logged_task["records_replayed"] == 1
        assert logged_task["out_of_window_samples"] == 2
        assert logged_task["all_verified"] is True

    def test_recovery_reverifies_adopted_results_by_original_source(self, data_root):
        """恢复后按原出处复验已取结果：出处被篡改时校验失败并可定位。"""
        from app.services.range_replay import RangeReplayService
        from app.services.reconciliation import read_replay_result

        point = "SENS-R1"
        _seed_point_data(data_root)
        cal_file = data_root / "calibrated" / f"{point}_calibrated.jsonl"
        _write_jsonl(cal_file, _calibrated_rows(point, 0.12, "UNIT-NEW"))
        replay = _make_replay()
        svc = RangeReplayService()
        svc.replay_range(replay["id"])

        # 正常恢复：按原出处复验通过
        verify = svc.verify_replay(replay["id"])
        assert verify["all_verified"] is True
        assert verify["rows_checked"] == 1
        check = verify["checks"][0]
        assert check["value_ok"] and check["unit_ok"] and check["checksum_ok"]
        assert check["unique_observation"] is True

        # 原上送被改动（恢复后数据与交付时出处不一致）：复验应失败
        edge_file = data_root / "edge_samples" / f"{point}.jsonl"
        tampered = json.loads(edge_file.read_text().splitlines()[1])
        tampered["raw_value_mV"] = 999.0
        lines = edge_file.read_text().splitlines()
        lines[1] = json.dumps(tampered)
        edge_file.write_text("\n".join(lines) + "\n")

        rows = read_replay_result(data_root, replay["重采编号"])
        from app.services.reconciliation import verify_replay_sources
        failed = verify_replay_sources(data_root, point, rows)
        assert failed["all_verified"] is False
        assert failed["checks"][0]["checksum_ok"] is False
