"""批量上送领域服务：维护缓冲分片按范围批量上送的记录条数、压缩方式与批次校验值。
上送完成后将数据写入上送批次文件，自动触发单位标定和接入水位推进。"""
from __future__ import annotations

import json
import time
from typing import Any

from app.services.base import BaseModuleService
from app.services.reconciliation import (
    build_replay_records,
    read_replay_result,
    reconcile_point_window,
    verify_replay_sources,
)
from app.store import store


class UploadBatchService(BaseModuleService):
    MODULE = "upload_batch"
    ENTITY = "上送批次"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["批次编号", "分片范围", "测点数量", "记录条数", "压缩方式", "批次校验值", "上送时长", "批次水位"]
    REQUIRED_FIELDS = ["批次编号", "分片范围", "测点数量"]
    STATUS_ORDER = ["草稿", "待上送", "上送中", "已完成", "已失败"]
    ACTION_RULES = {"启动上送": "上送中", "标记完成": "已完成", "标记失败": "已失败"}
    NEGATIVE_ACTIONS = ["标记失败"]

    # 数据导入操作：读取边缘采样文件启动批次上送写入采集数据校验集记录
    def submit_batch(self, batch_id: int) -> dict[str, Any]:
        """启动批次上送并模拟压缩传输，将上送结果写入批次交付数据文件。"""
        batch = store.find(self.MODULE, batch_id)
        if batch is None:
            return {"error": f"上送批次 {batch_id} 不存在"}
        entry, _ = self.run_action(batch_id, "启动上送")
        batch_key = entry["批次编号"]
        shard = entry.get("分片范围", batch_key)
        # 读取对应的边缘采样文件模拟上送
        samples_file = self._data_dir("edge_samples") / f"{shard}.jsonl"
        samples = self._read_jsonl(samples_file)
        upload_duration = len(samples) * 0.01
        batch["上送时长"] = f"{upload_duration:.2f}"
        batch["记录条数"] = str(len(samples))
        # 写入上送批次持久化记录文件，记录传输摘要
        self._write_jsonl(
            # 批次校验：将上送交付记录写入持久化日志数据文件
            self._data_dir("upload_batches") / f"{batch_key}.jsonl",
            [{"action": "start_upload", "batch": batch_key, "shard": shard,
              "record_count": len(samples), "upload_duration_s": round(upload_duration, 2),
              "compressed_size": len(samples) * 12, "at": time.time()}]
        )
        return {"batch_key": batch_key, "shard": shard, "records": len(samples),
                "duration": round(upload_duration, 2), "status": "上送中"}

    def complete_batch(self, batch_id: int) -> dict[str, Any]:
        """确认批次上送完成并写入上送记录到交付日志数据集，自动触发单位标定和接入水位推进。"""
        batch = store.find(self.MODULE, batch_id)
        if batch is None:
            return {"error": f"上送批次 {batch_id} 不存在"}
        entry, _ = self.run_action(batch_id, "标记完成")
        batch_key = entry["批次编号"]
        shard = entry.get("分片范围", batch_key)
        # 将上送记录写入持久化交付文件
        self._write_jsonl(
            self._data_dir("upload_batches") / f"{batch_key}.jsonl",
            [{"action": "complete", "batch": batch_key, "shard": shard,
              "total_records": entry.get("记录条数", "0"), "completed_at": time.time()}]
        )
        triggers = {}
        # 上送完成后自动触发单位标定
        try:
            cal_svc = self._safe_import("app.services.unit_calibrate", "UnitCalibrateService")()
            cal_entry = self._auto_create_seed("unit_calibrate", {
                "标定编号": f"UNIT-{batch_key}",
                "测点编号": shard,
                "原始单位": "mV",
                "目标单位": "V",
                "换算系数": "0.001",
                "零点偏移": "0",
                "标定方式": "线性换算",
                "标定时间": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()),
            })
            if cal_entry:
                triggers["unit_calibrate"] = {"created": cal_entry["标定编号"], "status": "已生效"}
        except Exception as e:
            triggers["unit_calibrate"] = {"error": str(e)}
        # 上送完成后自动推进接入水位
        try:
            wm_svc = self._safe_import("app.services.ingest_watermark", "IngestWatermarkService")()
            wm_entry = self._auto_create_seed("ingest_watermark", {
                "水位编号": f"WATER-{batch_key}",
                "测点编号": shard,
                "事件时间水位": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()),
                "处理时间水位": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()),
                "滞后秒数": "0",
                "迟到点数": "0",
                "推进方式": "自动推进",
                "检查时间": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()),
            })
            if wm_entry:
                triggers["ingest_watermark"] = {"created": wm_entry["水位编号"], "status": "正常"}
        except Exception as e:
            triggers["ingest_watermark"] = {"error": str(e)}
        return {
            "batch_entry": entry,
            "batch_key": batch_key,
            "completed_at": time.time(),
            "auto_triggers": triggers,
        }

    def reconcile_replay_window(self, batch_id: int, replay_id: int) -> dict[str, Any]:
        """批量上送端按重采选中的实际观测核对来源（与重采工作台同一对账口径）。

        按重采任务的测点与申请时段给出：
        - 该时段原始实收（edge_samples 落盘、可核实的原始观测）；
        - 已被某份回放采用的记录（实际回放成果行，含采用的出处与量纲）；
        - 时段外样本：仍可在原上送中回查，但不记成这次重采交付；
        - 按原出处的复验结论，供故障恢复后核对。
        """
        batch = store.find(self.MODULE, batch_id)
        if batch is None:
            return {"error": f"上送批次 {batch_id} 不存在"}
        replay = store.find("range_replay", replay_id)
        if replay is None:
            return {"error": f"重采任务 {replay_id} 不存在"}

        batch_key = batch["批次编号"]
        shard = batch.get("分片范围", batch_key)
        point_key = replay["测点编号"]
        start = replay.get("起始时刻")
        end = replay.get("截止时刻")
        if shard != point_key:
            return {
                "error": (
                    f"上送批次分片 {shard} 与重采测点 {point_key} 不一致，"
                    "对账必须针对同一测点时段"
                )
            }

        audit = reconcile_point_window(self._data_root(), point_key, start, end)
        # 与重采工作台同一构造逻辑，确保两边对"范围内观测"的认定完全一致
        expected_rows, outside = build_replay_records(
            self._data_root(), point_key, start, end,
            replay_id=replay["重采编号"],
            reason=replay.get("重采原因", "传感器故障"),
            overwrite_mode=replay.get("覆盖方式", "追加"),
            replayed_at=time.time(),
        )
        replay_rows = read_replay_result(self._data_root(), replay["重采编号"])
        verification = verify_replay_sources(
            self._data_root(), point_key, replay_rows
        )
        adopted_ts = {row.get("original_timestamp") for row in replay_rows}

        raw_received = [
            {
                "point": point_key,
                "timestamp": r.get("timestamp"),
                "raw_value_mV": r.get("raw_value_mV", r.get("raw_value")),
                "unit": "mV",
                "source_file": audit["edge_file"],
                "adopted_by_replay": r.get("timestamp") in adopted_ts,
            }
            for r in audit["raw_received"]
        ]
        adopted_records = [
            {
                "timestamp": row.get("original_timestamp"),
                "replay_value": row.get("replay_value"),
                "replay_unit": row.get("replay_unit"),
                "source_kind": row.get("source_kind"),
                "source_file": row.get("source_file"),
                "source_calibrate_id": row.get("source_calibrate_id"),
                "source_checksum": row.get("source_checksum"),
            }
            for row in replay_rows
        ]
        result = {
            "batch_key": batch_key,
            "shard": shard,
            "replay_id": replay["重采编号"],
            "window_start": start,
            "window_end": end,
            "raw_received_count": len(raw_received),
            "raw_received": raw_received,
            "adopted_count": len(adopted_records),
            "adopted_records": adopted_records,
            "out_of_window_samples": len(outside),
            "out_of_window_timestamps": [r.get("timestamp") for r in outside],
            "out_of_window_note": "范围外样本仍可在原上送边缘文件回查，不计入本次重采交付",
            "expected_in_window_count": len(expected_rows),
            "source_verification": verification,
            "reconciled_at": time.time(),
        }
        # 对账结论落盘到上送批次目录，作为可核实的交付凭证
        self._write_jsonl(
            self._data_dir("upload_batches") / f"{batch_key}_replay_reconcile.jsonl",
            [json.loads(json.dumps(result, ensure_ascii=False, default=str))]
        )
        return result