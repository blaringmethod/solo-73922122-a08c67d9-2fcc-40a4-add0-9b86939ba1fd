"""识别任务服务层：领取、提交、按邮件汇总。

任务生命周期（全部持久化在 SQLite，进程重启不丢失）：

    pending --claim-->  pending + 租约（30 秒） --submit-->  completed + 结果
                            |                              ^
                            +-- 租约到期，可被再次 claim --+

- claim：只领取 pending 且无有效租约的任务；BEGIN IMMEDIATE + 条件 UPDATE
  保证并发领取不会拿到同一项，到期任务可被重新领取（旧凭据随即失效）。
- submit：未完成任务只接受当前未过期凭据；首次完成时结果与 completed 状态
  在同一条 UPDATE 中落库。
- 重交：已完成任务使用"完成时的那份原凭据"重交同内容，返回原结果；
  异内容拒绝；其它凭据一律拒绝。
"""
from __future__ import annotations

import hashlib
import secrets
import time
from datetime import datetime, timezone
from typing import Any, Optional

from .config import Settings
from .database import Database


def _hash_credential(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _iso(epoch: Optional[float]) -> Optional[str]:
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()


def _result_payload(row: Any) -> Optional[dict[str, Any]]:
    if row["status"] != "completed":
        return None
    return {
        "invoice_number": row["invoice_number"],
        "amount_cents": int(row["amount_cents"]),
        "completed_at": row["completed_at"],
    }


def _task_payload(row: Any, base_url: str = "") -> dict[str, Any]:
    download_path = f"/api/messages/attachments/{row['attachment_id']}/download"
    return {
        "task_id": row["id"],
        "attachment_id": row["attachment_id"],
        "uidvalidity": row["uidvalidity"],
        "uid": row["uid"],
        "part_number": row["part_number"],
        "filename": row["filename"],
        "status": row["status"],
        "original_file_url": f"{base_url}{download_path}" if base_url else download_path,
        "result": _result_payload(row),
        "lease_expires_at": _iso(row["lease_expires_at"]),
    }


class RecognitionService:
    def __init__(self, settings: Settings, db: Database) -> None:
        self._settings = settings
        self._db = db

    # ------------------------------------------------------------------
    def claim(self, base_url: str = "") -> Optional[dict[str, Any]]:
        """领取一项待识别 PDF；没有可领取任务时返回 None。"""
        token = secrets.token_urlsafe(32)
        now = time.time()
        expires_at = now + self._settings.recognition_lease_seconds
        row = self._db.claim_recognition_task(now, _hash_credential(token), expires_at)
        if row is None:
            return None
        payload = _task_payload(row, base_url)
        # 凭据只在本响应明文出现一次；库里只存哈希
        payload["credential"] = token
        payload["lease_expires_at"] = _iso(expires_at)
        payload["lease_expires_in_seconds"] = self._settings.recognition_lease_seconds
        return payload

    # ------------------------------------------------------------------
    def submit(self, task_id: int, credential: str, invoice_number: str,
               amount_cents: int) -> tuple[int, dict[str, Any]]:
        """提交识别结果，返回 (HTTP 状态码, 响应体)。

        成功（首次完成或原凭据同内容重交）均为 200；其余为 4xx 且带 code。
        """
        code, row = self._db.submit_recognition_result(
            task_id, _hash_credential(credential), invoice_number,
            amount_cents, time.time(),
        )
        if code == "not_found":
            return 404, {"code": "task_not_found", "message": "识别任务不存在"}
        if code == "completed_by_other":
            return 409, {
                "code": "credential_not_original",
                "message": "任务已完成；仅接受完成该任务的原凭据重交",
            }
        if code == "content_conflict":
            return 409, {
                "code": "content_conflict",
                "message": "任务已完成；重交内容与原结果不一致",
                "original_result": _result_payload(row),
            }
        if code == "credential_mismatch":
            return 409, {
                "code": "credential_stale",
                "message": "凭据已失效：任务已被重新领取，只接受当前凭据；"
                           "若凭据到期，请重新领取任务",
            }
        if code == "credential_expired":
            return 409, {
                "code": "credential_expired",
                "message": "凭据已过期：请重新领取任务",
            }

        payload = _task_payload(row)
        payload["replayed"] = code == "replay"
        return 200, payload

    # ------------------------------------------------------------------
    def message_summary(self, message_pk: int) -> Optional[dict[str, Any]]:
        """按邮件汇总识别结果：全部 PDF 完成才返回结果，否则只返回进度。"""
        msg = self._db.get_message_with_epoch(message_pk)
        if msg is None:
            return None
        tasks = self._db.list_message_recognition_tasks(message_pk)
        total = len(tasks)
        completed = sum(1 for t in tasks if t["status"] == "completed")
        pending = total - completed
        now = time.time()
        leased = sum(
            1 for t in tasks
            if t["status"] == "pending"
            and t["lease_expires_at"] is not None
            and t["lease_expires_at"] > now
        )
        all_done = pending == 0
        if total == 0:
            state = "no_pdf"
        elif all_done:
            state = "completed"
        else:
            state = "pending"
        payload: dict[str, Any] = {
            "message_id": msg["id"],
            "uidvalidity": msg["uidvalidity"],
            "uid": msg["uid"],
            "subject": msg["subject"],
            "state": state,
            "progress": {
                "total": total,
                "completed": completed,
                "pending": pending,
                "leased": leased,
            },
            # 未齐时 results 为空，只暴露进度，不泄露部分识别结果
            "results": [],
        }
        if all_done:
            payload["results"] = [
                {
                    "task_id": t["id"],
                    "attachment_id": t["attachment_id"],
                    "part_number": t["part_number"],
                    "filename": t["filename"],
                    "invoice_number": t["invoice_number"],
                    "amount_cents": int(t["amount_cents"]),
                    "completed_at": t["completed_at"],
                    "original_file_url":
                        f"/api/messages/attachments/{t['attachment_id']}/download",
                }
                for t in tasks
            ]
        return payload
