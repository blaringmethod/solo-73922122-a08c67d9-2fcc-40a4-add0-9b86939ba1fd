"""SQLite 持久层。

记录 UIDVALIDITY、UID、逐封采集进度以及附件索引。
所有"索引 + 进度"的写入都在同一事务内提交（见 collector.pipeline），
查询接口只可能看到完整提交的邮件，永远不会看到半封附件。
"""
from __future__ import annotations

import os
import sqlite3
import threading
import time
from typing import Any, Iterable, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS mailbox_epochs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    uidvalidity     INTEGER NOT NULL UNIQUE,
    mailbox         TEXT NOT NULL,
    observed_at     TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS messages (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    epoch_id        INTEGER NOT NULL REFERENCES mailbox_epochs(id),
    uid             INTEGER NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('done', 'failed', 'oversize')),
    failure_reason  TEXT,
    message_size    INTEGER,
    subject         TEXT,
    from_header     TEXT,
    message_id_hdr  TEXT,
    collected_at    TEXT,
    UNIQUE(epoch_id, uid)
);

CREATE TABLE IF NOT EXISTS attachments (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id      INTEGER NOT NULL REFERENCES messages(id),
    part_number     TEXT NOT NULL,
    filename        TEXT NOT NULL,
    content_type    TEXT NOT NULL,
    size_bytes      INTEGER NOT NULL,
    storage_path    TEXT NOT NULL,
    stored_at       TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(message_id, part_number)
);

CREATE TABLE IF NOT EXISTS collection_runs (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    epoch_id            INTEGER REFERENCES mailbox_epochs(id),
    uidvalidity         INTEGER,
    started_at          TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at         TEXT,
    status              TEXT NOT NULL DEFAULT 'running'
                        CHECK (status IN ('running', 'completed', 'aborted', 'busy')),
    total_seen          INTEGER NOT NULL DEFAULT 0,
    processed_ok        INTEGER NOT NULL DEFAULT 0,
    failed              INTEGER NOT NULL DEFAULT 0,
    oversize            INTEGER NOT NULL DEFAULT 0,
    skipped_terminal    INTEGER NOT NULL DEFAULT 0,
    attachments_saved   INTEGER NOT NULL DEFAULT 0,
    note                TEXT
);

-- 每份已落盘 PDF 一条持久识别任务。
-- 任务在邮件/附件事务内创建；历史附件由启动时的幂等回填补齐。
-- 租约字段（lease_token_hash / lease_expires_at）持久化，重启不会重置截止时间；
-- 完成结果（invoice_number / amount_cents）与 completed 状态同一条 UPDATE 保存。
CREATE TABLE IF NOT EXISTS recognition_tasks (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    attachment_id       INTEGER NOT NULL UNIQUE REFERENCES attachments(id),
    uidvalidity         INTEGER NOT NULL,
    uid                 INTEGER NOT NULL,
    part_number         TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'completed')),
    lease_token_hash    TEXT,
    lease_expires_at    REAL,
    invoice_number      TEXT,
    amount_cents        INTEGER CHECK (amount_cents IS NULL OR amount_cents >= 0),
    completed_at        TEXT,
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at          TEXT
);

CREATE INDEX IF NOT EXISTS idx_recognition_tasks_claim
    ON recognition_tasks(status, lease_expires_at, id);
"""


class Database:
    def __init__(self, db_path: str) -> None:
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=10000")
        self._lock = threading.RLock()
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._conn.commit()
            # 上次进程退出时仍处于 running 的采集，标记为 aborted（可被重新采集）
            self._conn.execute(
                "UPDATE collection_runs SET status='aborted', "
                "finished_at=datetime('now'), "
                "note=COALESCE(note,'') || ' [进程重启，自动中断标记]' "
                "WHERE status='running'"
            )
            self._conn.commit()
            # 历史附件可能早于识别任务表上线；启动时幂等补齐，每份 PDF 恰好一项任务。
            # NOT EXISTS + UNIQUE(attachment_id) 保证重复启动不会重复建任务，
            # 已完成任务及其结果不会被回填覆盖。
            self._conn.execute(
                "INSERT INTO recognition_tasks"
                "(attachment_id, uidvalidity, uid, part_number) "
                "SELECT a.id, e.uidvalidity, m.uid, a.part_number "
                "FROM attachments a "
                "JOIN messages m ON m.id=a.message_id "
                "JOIN mailbox_epochs e ON e.id=m.epoch_id "
                "WHERE NOT EXISTS ("
                "  SELECT 1 FROM recognition_tasks t WHERE t.attachment_id=a.id"
                ") "
                "ON CONFLICT(attachment_id) DO NOTHING"
            )
            self._conn.commit()

    # ---- 基础工具 ----
    @property
    def lock(self) -> threading.RLock:
        return self._lock

    def query_all(self, sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(sql, tuple(params)).fetchall()

    def query_one(self, sql: str, params: Iterable[Any] = ()) -> Optional[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(sql, tuple(params)).fetchone()

    def begin(self) -> sqlite3.Connection:
        """调用方在同一线程内随后使用 commit()/rollback()。"""
        self._lock.acquire()
        self._conn.execute("BEGIN IMMEDIATE")
        return self._conn

    def commit(self) -> None:
        try:
            self._conn.commit()
        finally:
            self._lock.release()

    def rollback(self) -> None:
        try:
            self._conn.rollback()
        finally:
            self._lock.release()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---- UIDVALIDITY / 邮箱代际 ----
    def get_or_create_epoch(self, uidvalidity: int, mailbox: str) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT id FROM mailbox_epochs WHERE uidvalidity=?",
                (uidvalidity,),
            ).fetchone()
            if row is not None:
                return int(row["id"])
            cur = self._conn.execute(
                "INSERT INTO mailbox_epochs(uidvalidity, mailbox) VALUES (?, ?)",
                (uidvalidity, mailbox),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def latest_epoch(self) -> Optional[sqlite3.Row]:
        return self.query_one(
            "SELECT * FROM mailbox_epochs ORDER BY uidvalidity DESC, id DESC LIMIT 1"
        )

    def list_epochs(self) -> list[sqlite3.Row]:
        return self.query_all("SELECT * FROM mailbox_epochs ORDER BY id")

    # ---- 采集运行记录 ----
    def start_run(self, uidvalidity: Optional[int], epoch_id: Optional[int]) -> int:
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO collection_runs(epoch_id, uidvalidity) VALUES (?, ?)",
                (epoch_id, uidvalidity),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def finish_run(self, run_id: int, status: str, counters: dict[str, int],
                   note: Optional[str] = None) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE collection_runs SET status=?, finished_at=datetime('now'), "
                "total_seen=?, processed_ok=?, failed=?, oversize=?, "
                "skipped_terminal=?, attachments_saved=?, note=? WHERE id=?",
                (
                    status,
                    counters.get("total_seen", 0),
                    counters.get("processed_ok", 0),
                    counters.get("failed", 0),
                    counters.get("oversize", 0),
                    counters.get("skipped_terminal", 0),
                    counters.get("attachments_saved", 0),
                    note,
                    run_id,
                ),
            )
            self._conn.commit()

    def list_runs(self, limit: int = 50) -> list[sqlite3.Row]:
        return self.query_all(
            "SELECT * FROM collection_runs ORDER BY id DESC LIMIT ?", (limit,)
        )

    def attach_epoch(self, run_id: int, epoch_id: int, uidvalidity: int) -> None:
        """IMAP 连接成功、拿到 UIDVALIDITY 后，把运行记录挂到对应邮箱代际。"""
        with self._lock:
            self._conn.execute(
                "UPDATE collection_runs SET epoch_id=?, uidvalidity=? WHERE id=?",
                (epoch_id, uidvalidity, run_id),
            )
            self._conn.commit()

    # ---- 单封邮件：进度 + 索引在同一事务内提交 ----
    def get_message(self, epoch_id: int, uid: int) -> Optional[sqlite3.Row]:
        return self.query_one(
            "SELECT * FROM messages WHERE epoch_id=? AND uid=?", (epoch_id, uid)
        )

    def insert_message(self, conn: sqlite3.Connection, epoch_id: int, uid: int,
                       status: str, failure_reason: Optional[str],
                       message_size: Optional[int], subject: Optional[str],
                       from_header: Optional[str], message_id_hdr: Optional[str]) -> int:
        cur = conn.execute(
            "INSERT INTO messages(epoch_id, uid, status, failure_reason, message_size, "
            "subject, from_header, message_id_hdr, collected_at) "
            "VALUES (?,?,?,?,?,?,?,?, datetime('now'))",
            (epoch_id, uid, status, failure_reason, message_size,
             subject, from_header, message_id_hdr),
        )
        return int(cur.lastrowid)

    def insert_attachment(self, conn: sqlite3.Connection, message_row_id: int,
                          part_number: str, filename: str, content_type: str,
                          size_bytes: int, storage_path: str) -> int:
        cur = conn.execute(
            "INSERT INTO attachments(message_id, part_number, filename, content_type, "
            "size_bytes, storage_path) VALUES (?,?,?,?,?,?)",
            (message_row_id, part_number, filename, content_type,
             size_bytes, storage_path),
        )
        return int(cur.lastrowid)

    # ---- 查询接口 ----
    def list_messages(self, epoch_id: Optional[int] = None,
                      status: Optional[str] = None, limit: int = 200) -> list[sqlite3.Row]:
        sql = (
            "SELECT m.*, e.uidvalidity FROM messages m "
            "JOIN mailbox_epochs e ON e.id=m.epoch_id WHERE 1=1"
        )
        params: list[Any] = []
        if epoch_id is not None:
            sql += " AND m.epoch_id=?"
            params.append(epoch_id)
        if status is not None:
            sql += " AND m.status=?"
            params.append(status)
        sql += " ORDER BY e.uidvalidity, m.uid LIMIT ?"
        params.append(limit)
        return self.query_all(sql, params)

    def get_message_with_epoch(self, message_pk: int) -> Optional[sqlite3.Row]:
        return self.query_one(
            "SELECT m.*, e.uidvalidity FROM messages m "
            "JOIN mailbox_epochs e ON e.id=m.epoch_id WHERE m.id=?",
            (message_pk,),
        )

    def list_attachments(self, message_pk: int) -> list[sqlite3.Row]:
        return self.query_all(
            "SELECT * FROM attachments WHERE message_id=? ORDER BY part_number",
            (message_pk,),
        )

    # ---- 识别任务：创建 / 领取 / 提交 / 查询 ----
    def insert_recognition_task(self, conn: sqlite3.Connection,
                                attachment_id: int, uidvalidity: int,
                                uid: int, part_number: str) -> None:
        """在附件入库的同一事务内创建对应识别任务。

        ON CONFLICT DO NOTHING 只用于兼容启动回填与新建采集的并发窗口，
        绝不会覆盖已存在的任务（尤其是已完成结果与原租约截止时间）。
        """
        conn.execute(
            "INSERT INTO recognition_tasks"
            "(attachment_id, uidvalidity, uid, part_number) VALUES (?,?,?,?) "
            "ON CONFLICT(attachment_id) DO NOTHING",
            (attachment_id, uidvalidity, uid, part_number),
        )

    def get_recognition_task(self, task_id: int,
                             conn: Optional[sqlite3.Connection] = None) -> Optional[sqlite3.Row]:
        if conn is not None:
            return conn.execute(
                "SELECT t.*, a.filename, a.size_bytes, a.storage_path "
                "FROM recognition_tasks t JOIN attachments a ON a.id=t.attachment_id "
                "WHERE t.id=?",
                (task_id,),
            ).fetchone()
        return self.query_one(
            "SELECT t.*, a.filename, a.size_bytes, a.storage_path "
            "FROM recognition_tasks t JOIN attachments a ON a.id=t.attachment_id "
            "WHERE t.id=?",
            (task_id,),
        )

    def claim_recognition_task(self, now: float, token_hash: str,
                               lease_expires_at: float) -> Optional[sqlite3.Row]:
        """原子领取一项任务：仅领取 pending 且未租用/租约已过期的任务。

        BEGIN IMMEDIATE + 条件 UPDATE 保证两个并发客户端不可能拿到同一条任务；
        到期任务可被下一次领取直接覆盖旧租约（旧凭据随即失效）。
        """
        for _ in range(5):
            db = self.begin()
            try:
                row = db.execute(
                    "SELECT t.*, a.filename, a.size_bytes, a.storage_path "
                    "FROM recognition_tasks t JOIN attachments a ON a.id=t.attachment_id "
                    "WHERE t.status='pending' "
                    "AND (t.lease_expires_at IS NULL OR t.lease_expires_at <= ?) "
                    "ORDER BY t.id LIMIT 1",
                    (now,),
                ).fetchone()
                if row is None:
                    self.commit()
                    return None
                cur = db.execute(
                    "UPDATE recognition_tasks SET lease_token_hash=?, lease_expires_at=?, "
                    "updated_at=datetime('now') WHERE id=? AND status='pending' "
                    "AND (lease_expires_at IS NULL OR lease_expires_at <= ?)",
                    (token_hash, lease_expires_at, row["id"], now),
                )
                if cur.rowcount != 1:
                    # 跨进程竞争窗口：重试选择下一项
                    self.rollback()
                    continue
                claimed = db.execute(
                    "SELECT t.*, a.filename, a.size_bytes, a.storage_path "
                    "FROM recognition_tasks t JOIN attachments a ON a.id=t.attachment_id "
                    "WHERE t.id=?",
                    (row["id"],),
                ).fetchone()
                self.commit()
                return claimed
            except Exception:
                self.rollback()
                raise
        return None

    def submit_recognition_result(self, task_id: int, credential_hash: str,
                                  invoice_number: str, amount_cents: int,
                                  now: float) -> tuple[str, Optional[sqlite3.Row]]:
        """提交识别结果，返回 (结果代码, 任务行)。

        结果代码：
        - "completed"：本次首次完成（结果与 completed 状态同一条 UPDATE 保存）
        - "replay"：   已完成任务，原凭据 + 同内容，返回原结果
        - "not_found"：任务不存在
        - "credential_mismatch" / "credential_expired"：未完成任务凭据不是当前有效凭据
        - "content_conflict"：已完成任务，但提交内容与原结果不同
        - "completed_by_other"：已完成任务且凭据不是完成时使用的原凭据
        """
        db = self.begin()
        try:
            row = db.execute(
                "SELECT t.*, a.filename, a.size_bytes, a.storage_path "
                "FROM recognition_tasks t JOIN attachments a ON a.id=t.attachment_id "
                "WHERE t.id=?",
                (task_id,),
            ).fetchone()
            if row is None:
                self.commit()
                return "not_found", None

            if row["status"] == "completed":
                if not row["lease_token_hash"] or row["lease_token_hash"] != credential_hash:
                    self.commit()
                    return "completed_by_other", row
                if (row["invoice_number"] == invoice_number
                        and int(row["amount_cents"]) == amount_cents):
                    self.commit()
                    return "replay", row
                self.commit()
                return "content_conflict", row

            if not row["lease_token_hash"] or row["lease_token_hash"] != credential_hash:
                self.commit()
                return "credential_mismatch", row
            if row["lease_expires_at"] is None or row["lease_expires_at"] <= now:
                self.commit()
                return "credential_expired", row

            cur = db.execute(
                "UPDATE recognition_tasks SET status='completed', invoice_number=?, "
                "amount_cents=?, completed_at=datetime('now'), "
                "updated_at=datetime('now') WHERE id=? AND status='pending' "
                "AND lease_token_hash=? AND lease_expires_at > ?",
                (invoice_number, amount_cents, task_id, credential_hash, now),
            )
            if cur.rowcount != 1:
                # 理论上不可能（BEGIN IMMEDIATE 内行不会被改写），防御性回滚
                self.rollback()
                return "credential_mismatch", row
            # 保留 lease_token_hash：它是"完成时使用的原凭据"，
            # 用于后续同内容重交的原结果返回校验。
            completed = db.execute(
                "SELECT t.*, a.filename, a.size_bytes, a.storage_path "
                "FROM recognition_tasks t JOIN attachments a ON a.id=t.attachment_id "
                "WHERE t.id=?",
                (task_id,),
            ).fetchone()
            self.commit()
            return "completed", completed
        except Exception:
            self.rollback()
            raise

    def list_recognition_tasks(self, status: Optional[str] = None,
                               uidvalidity: Optional[int] = None,
                               uid: Optional[int] = None,
                               limit: int = 200) -> list[sqlite3.Row]:
        sql = (
            "SELECT t.*, a.filename, a.size_bytes, a.storage_path "
            "FROM recognition_tasks t JOIN attachments a ON a.id=t.attachment_id "
            "WHERE 1=1"
        )
        params: list[Any] = []
        if status is not None:
            sql += " AND t.status=?"
            params.append(status)
        if uidvalidity is not None:
            sql += " AND t.uidvalidity=?"
            params.append(uidvalidity)
        if uid is not None:
            sql += " AND t.uid=?"
            params.append(uid)
        sql += " ORDER BY t.uidvalidity, t.uid, t.part_number, t.id LIMIT ?"
        params.append(limit)
        return self.query_all(sql, params)

    def list_message_recognition_tasks(self, message_pk: int) -> list[sqlite3.Row]:
        return self.query_all(
            "SELECT t.*, a.filename, a.size_bytes, a.storage_path "
            "FROM recognition_tasks t JOIN attachments a ON a.id=t.attachment_id "
            "WHERE a.message_id=? ORDER BY a.part_number, t.id",
            (message_pk,),
        )

    def recognition_progress(self) -> sqlite3.Row:
        return self.query_one(
            "SELECT COUNT(*) AS total, "
            "SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed, "
            "SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pending, "
            "SUM(CASE WHEN status='pending' AND lease_expires_at IS NOT NULL "
            "         AND lease_expires_at > ? THEN 1 ELSE 0 END) AS leased "
            "FROM recognition_tasks",
            (time.time(),),
        )

    def get_attachment(self, attachment_pk: int) -> Optional[sqlite3.Row]:
        return self.query_one(
            "SELECT a.*, m.epoch_id, m.uid, e.uidvalidity FROM attachments a "
            "JOIN messages m ON m.id=a.message_id "
            "JOIN mailbox_epochs e ON e.id=m.epoch_id WHERE a.id=?",
            (attachment_pk,),
        )

    def progress_for_epoch(self, epoch_id: int) -> sqlite3.Row:
        return self.query_one(
            "SELECT COUNT(*) AS total, "
            "SUM(CASE WHEN status='done' THEN 1 ELSE 0 END) AS done, "
            "SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed, "
            "SUM(CASE WHEN status='oversize' THEN 1 ELSE 0 END) AS oversize, "
            "COALESCE((SELECT COUNT(*) FROM attachments a JOIN messages m2 "
            "          ON m2.id=a.message_id WHERE m2.epoch_id=?),0) AS attachments "
            "FROM messages WHERE epoch_id=?",
            (epoch_id, epoch_id),
        )
