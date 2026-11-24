"""区间重采领域服务：维护传感器故障或标定变更引起的历史区间重采与覆盖方式。
对历史时间窗口的传感器数据按申请时段重采样，写入重采覆盖率数据文件。

合规口径（与批量上送入口共用 app.services.reconciliation）：
- 只交付申请时段 [起始时刻, 截止时刻] 内的观测，时段外样本不写入回放成果；
- 同一观测在一次回放成果中只有唯一一行，重试后仍只有一条（整文件替换）；
- 每行记录实际采用的采样出处与量纲，回放前确认的标定更正在回放时生效。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.services.reconciliation import (
    build_replay_records,
    read_replay_result,
    verify_replay_sources,
)
from app.store import store


class RangeReplayService(BaseModuleService):
    MODULE = "range_replay"
    ENTITY = "重采任务"
    OPERATION_TERM = "水位"
    LIST_FIELDS = ["重采编号", "测点编号", "起始时刻", "截止时刻", "重采原因", "覆盖方式", "重采点数", "重采水位"]
    REQUIRED_FIELDS = ["重采编号", "测点编号", "起始时刻"]
    STATUS_ORDER = ["草稿", "待重采", "重采中", "已完成", "已取消"]
    ACTION_RULES = {"启动重采": "重采中", "标记完成": "已完成", "取消重采": "已取消"}
    NEGATIVE_ACTIONS = ["取消重采"]

    # 数据回填操作：按申请时段对传感器历史区间执行重采样，写入重采覆盖结果数据
    def replay_range(self, replay_id: int) -> dict[str, Any]:
        """按重采任务的申请时段执行重采样，并整文件替换重采覆盖结果。"""
        replay = store.find(self.MODULE, replay_id)
        if replay is None:
            return {"error": f"重采任务 {replay_id} 不存在"}
        self.run_action(replay_id, "启动重采")
        point_key = replay["测点编号"]
        reason = replay.get("重采原因", "传感器故障")
        overwrite = replay.get("覆盖方式", "追加")
        start = replay.get("起始时刻")
        end = replay.get("截止时刻")
        replay_key = replay["重采编号"]

        # 同一测点 + 同一申请时段对账：只取范围内唯一观测，范围外样本仅留痕
        replay_records, outside_records = build_replay_records(
            self._data_root(), point_key, start, end,
            replay_id=replay_key, reason=reason,
            overwrite_mode=overwrite, replayed_at=time.time(),
        )

        # 整文件替换：清除上次（可能含时段外样本的）错误记录，保证重试幂等
        output_file = self._data_dir("replayed_ranges") / f"{replay_key}_replay.jsonl"
        written = self._replace_jsonl(output_file, replay_records)
        replay["重采点数"] = str(written)
        replay["重采水位"] = str(written)
        self.run_action(replay_id, "标记完成")

        # 回读实际写出的成果，按原出处复验后随结果返回，供批量故障重采核对
        replayed_rows = read_replay_result(self._data_root(), replay_key)
        verification = verify_replay_sources(self._data_root(), point_key, replayed_rows)
        return {
            "replay_id": replay_key,
            "point": point_key,
            "window_start": start,
            "window_end": end,
            "reason": reason,
            "records_replayed": written,
            "out_of_window_samples": len(outside_records),
            "out_of_window_timestamps": [r.get("timestamp") for r in outside_records],
            "delivered_timestamps": [r.get("original_timestamp") for r in replayed_rows],
            "source_verification": verification,
        }

    def verify_replay(self, replay_id: int) -> dict[str, Any]:
        """对已生成的回放成果按原出处复验（恢复后核对有效值、量纲与唯一性）。"""
        replay = store.find(self.MODULE, replay_id)
        if replay is None:
            return {"error": f"重采任务 {replay_id} 不存在"}
        replay_key = replay["重采编号"]
        rows = read_replay_result(self._data_root(), replay_key)
        result = verify_replay_sources(self._data_root(), replay["测点编号"], rows)
        result["replay_id"] = replay_key
        return result

    def batch_replay_faulty(self, reason: str = "传感器故障校准") -> dict[str, Any]:
        """批量重采所有因传感器故障导致异常的测点历史区间数据。

        逐个读取实际回放结果供核对，汇总里带上每个任务的交付行数、范围外
        样本数与出处复验结论，而不是只给一个计数。
        """
        all_replays = store.rows(self.MODULE)
        faulty = [r for r in all_replays if "故障" in (r.get("重采原因", ""))]
        results = []
        for r in faulty:
            res = self.replay_range(r["id"])
            if "error" not in res:
                rows = read_replay_result(self._data_root(), res["replay_id"])
                res["replayed_rows"] = rows
            results.append(res)
        # 汇总批量重采记录（汇总日志可追加；每个任务自己的成果文件仍是替换语义）
        self._write_jsonl(
            self._data_dir("replayed_ranges") / "batch_replay_summary.jsonl",
            [{
                "reason": reason,
                "total_replayed": len(results),
                "tasks": [
                    {
                        "replay_id": res.get("replay_id"),
                        "point": res.get("point"),
                        "window_start": res.get("window_start"),
                        "window_end": res.get("window_end"),
                        "records_replayed": res.get("records_replayed"),
                        "out_of_window_samples": res.get("out_of_window_samples"),
                        "all_verified": (
                            res.get("source_verification", {}) or {}
                        ).get("all_verified"),
                    }
                    for res in results if "error" not in res
                ],
                "at": time.time(),
            }]
        )
        return {"reason": reason, "total_replayed": len(results), "results": results}
