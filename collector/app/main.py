"""FastAPI 入口：采集触发、进度查询、按来源下载，以及识别任务领取/提交/汇总。

启动后访问 /docs 可看到全部入口并在线试用。
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field, field_validator

from .collector import Collector
from .config import load_settings
from .database import Database
from .process_lock import CollectionBusyError
from .recognition import RecognitionService
from .storage import Storage


# ---- 响应模型（/docs 中展示结构）----
class AttachmentOut(BaseModel):
    id: int
    part_number: str
    filename: str
    content_type: str
    size_bytes: int
    download_url: str


class MessageOut(BaseModel):
    id: int
    uidvalidity: int
    uid: int
    status: str
    failure_reason: Optional[str]
    message_size: Optional[int]
    subject: Optional[str]
    from_header: Optional[str]
    message_id_hdr: Optional[str]
    collected_at: Optional[str]
    attachment_count: int
    attachments: list[AttachmentOut] = Field(default_factory=list)


class EpochOut(BaseModel):
    id: int
    uidvalidity: int
    mailbox: str
    observed_at: str
    total: int
    done: int
    failed: int
    oversize: int
    attachments: int


class RunOut(BaseModel):
    id: int
    uidvalidity: Optional[int]
    started_at: str
    finished_at: Optional[str]
    status: str
    total_seen: int
    processed_ok: int
    failed: int
    oversize: int
    skipped_terminal: int
    attachments_saved: int
    note: Optional[str]


class CollectResponse(BaseModel):
    run_id: int
    status: str
    abort_reason: Optional[str]
    counters: dict


# ---- 识别任务 API 模型 ----
class RecognitionResultOut(BaseModel):
    invoice_number: str
    amount_cents: int
    completed_at: Optional[str]


class RecognitionTaskOut(BaseModel):
    task_id: int
    attachment_id: int
    uidvalidity: int
    uid: int
    part_number: str
    filename: str
    status: str
    original_file_url: str
    result: Optional[RecognitionResultOut]
    lease_expires_at: Optional[str]


class ClaimOut(RecognitionTaskOut):
    credential: str = Field(description="30 秒有效领取凭据，仅在领取响应中出现一次")
    lease_expires_in_seconds: int


class SubmitIn(BaseModel):
    credential: str = Field(min_length=1, description="领取时下发的凭据")
    invoice_number: str = Field(min_length=1, description="非空发票号")
    amount_cents: int = Field(
        ge=0, strict=True,
        description="非负整数分金额（JSON 中必须是整数，不接受布尔/浮点/数字字符串）",
    )

    @field_validator("invoice_number")
    @classmethod
    def _non_blank_invoice_number(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("发票号不能为空")
        return value


class SubmitOut(RecognitionTaskOut):
    replayed: bool = Field(description="True 表示原凭据重交同内容，返回原结果")


class RecognitionProgressOut(BaseModel):
    total: int
    completed: int
    pending: int
    leased: int


class MessageRecognitionItemOut(BaseModel):
    task_id: int
    attachment_id: int
    part_number: str
    filename: str
    invoice_number: str
    amount_cents: int
    completed_at: Optional[str]
    original_file_url: str


class MessageRecognitionOut(BaseModel):
    message_id: int
    uidvalidity: int
    uid: int
    subject: Optional[str]
    state: str = Field(
        description="completed = 全部 PDF 已识别；pending = 仍有未完成；no_pdf = 该邮件无 PDF"
    )
    progress: RecognitionProgressOut
    results: list[MessageRecognitionItemOut] = Field(
        default_factory=list,
        description="仅 state=completed 时返回全部识别结果，未齐时为空数组",
    )


class RecognitionStatsOut(BaseModel):
    total: int
    completed: int
    pending: int
    leased: int


state: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = load_settings()
    db = Database(settings.db_path)
    storage = Storage(settings.blob_root, settings.staging_dir)
    # 清理上次进程崩溃遗留的 staging 临时件
    removed = storage.cleanup_stale_staging()
    state["settings"] = settings
    state["db"] = db
    state["storage"] = storage
    state["collector"] = Collector(settings, db, storage)
    state["recognition"] = RecognitionService(settings, db)
    state["stale_staging_cleaned"] = removed
    yield
    db.close()


app = FastAPI(
    title="共享收票邮箱 PDF 采集与识别任务 API",
    description=(
        "从固定 IMAP 邮箱以 UID 采集发票 PDF 原件，并为每份 PDF 建立持久识别任务。\n\n"
        "- 只读 EXAMINE + BODY.PEEK，不改变邮件已读状态；\n"
        "- 每封 ≤ 8MiB，提取全部 application/pdf，按 MIME 部分序号区分同名附件；\n"
        "- 附件全部落盘后，索引、进度与识别任务同事务提交；\n"
        "- 识别客户端领取任务（30 秒有效凭据）→ 提交发票号与非负整数分金额；\n"
        "- 领取互斥、到期可重领、旧凭据不能提交未完成任务；\n"
        "- 已完成任务用原凭据重交同内容返回原结果，异内容拒绝；\n"
        "- 按邮件查询时全部 PDF 完成才整体返回结果，未齐仅返回进度；\n"
        "- 邮箱重建（UIDVALIDITY 变化）后在新 UID 空间重新采集，旧原件仍可查询。\n\n"
        "### 在 /docs 演示关键场景\n"
        "1. 先 `POST /api/collect` 采集并建立任务（演示邮箱含 5 份 PDF）。\n"
        "2. **双客户端争领**：连续两次 `POST /api/recognition/tasks/claim`，"
        "两次返回不同的 `task_id`（无任务时返回 204）。\n"
        "3. **到期重领 + 旧凭据迟到提交**：保存第一次领取的 `task_id` 与 `credential`，"
        "等待 30 秒后再领取一次；先用**旧** `credential` 提交会得到 "
        "`409 credential_stale`，用新凭据提交成功。\n"
        "4. **原结果重交**：用完成时的原凭据重交同内容返回 `replayed=true` 与原结果，"
        "重交异内容返回 `409 content_conflict`。\n"
        "5. **按邮件整体结果**：`GET /api/messages/{id}/recognition` 在全部 PDF "
        "完成前只返回 `progress`，全部完成后返回 `results`。\n\n"
        "也可在容器内一键跑完整演示：`docker compose run --rm client`。"
    ),
    version="1.1.0",
    lifespan=lifespan,
)


def _db() -> Database:
    return state["db"]


def _storage() -> Storage:
    return state["storage"]


def _recognition() -> RecognitionService:
    return state["recognition"]


def _recognition_task_out(row) -> RecognitionTaskOut:
    return RecognitionTaskOut(
        task_id=row["id"], attachment_id=row["attachment_id"],
        uidvalidity=row["uidvalidity"], uid=row["uid"],
        part_number=row["part_number"], filename=row["filename"],
        status=row["status"],
        original_file_url=f"/api/messages/attachments/{row['attachment_id']}/download",
        result=(
            RecognitionResultOut(
                invoice_number=row["invoice_number"],
                amount_cents=int(row["amount_cents"]),
                completed_at=row["completed_at"],
            ) if row["status"] == "completed" else None
        ),
        lease_expires_at=(
            datetime.fromtimestamp(row["lease_expires_at"], tz=timezone.utc).isoformat()
            if row["lease_expires_at"] is not None else None
        ),
    )


def _attachment_out(row) -> AttachmentOut:
    return AttachmentOut(
        id=row["id"], part_number=row["part_number"], filename=row["filename"],
        content_type=row["content_type"], size_bytes=row["size_bytes"],
        download_url=f"/api/messages/attachments/{row['id']}/download",
    )


def _message_out(row, attachments: Optional[list] = None) -> MessageOut:
    if attachments is None:
        attachments = _db().list_attachments(row["id"])
    return MessageOut(
        id=row["id"], uidvalidity=row["uidvalidity"], uid=row["uid"],
        status=row["status"], failure_reason=row["failure_reason"],
        message_size=row["message_size"], subject=row["subject"],
        from_header=row["from_header"], message_id_hdr=row["message_id_hdr"],
        collected_at=row["collected_at"], attachment_count=len(attachments),
        attachments=[_attachment_out(a) for a in attachments],
    )


# ---------------------------------------------------------------- 健康检查
@app.get("/health", tags=["系统"], summary="健康检查")
def health() -> dict:
    return {
        "status": "ok",
        "stale_staging_cleaned_on_boot": state.get("stale_staging_cleaned", 0),
    }


# ---------------------------------------------------------------- 采集触发
@app.post("/api/collect", response_model=CollectResponse, tags=["采集"],
          summary="触发一轮采集（全进程唯一，并发触发返回 409）")
def trigger_collect() -> CollectResponse:
    collector: Collector = state["collector"]
    try:
        result = collector.run_collection()
    except CollectionBusyError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return CollectResponse(**result)


@app.get("/api/runs", response_model=list[RunOut], tags=["采集"],
         summary="历史采集运行记录")
def list_runs(limit: int = 50) -> list[RunOut]:
    return [RunOut(**dict(r)) for r in _db().list_runs(limit)]


# ---------------------------------------------------------------- 进度 / 代际
@app.get("/api/progress", response_model=list[EpochOut], tags=["进度"],
         summary="各 UIDVALIDITY 代际的采集进度（含邮箱重建后的新代际）")
def progress() -> list[EpochOut]:
    result = []
    for epoch in _db().list_epochs():
        p = _db().progress_for_epoch(epoch["id"])
        result.append(EpochOut(
            id=epoch["id"], uidvalidity=epoch["uidvalidity"],
            mailbox=epoch["mailbox"], observed_at=epoch["observed_at"],
            total=p["total"] or 0, done=p["done"] or 0,
            failed=p["failed"] or 0, oversize=p["oversize"] or 0,
            attachments=p["attachments"] or 0,
        ))
    return result


@app.get("/api/epochs", response_model=list[EpochOut], tags=["进度"],
         summary="邮箱代际列表（每次 UIDVALIDITY 变化一条）")
def list_epochs() -> list[EpochOut]:
    return progress()


# ---------------------------------------------------------------- 邮件与来源
@app.get("/api/messages", response_model=list[MessageOut], tags=["来源查询"],
         summary="已采集邮件列表，可按 UIDVALIDITY 代际/状态过滤")
def list_messages(uidvalidity: Optional[int] = None,
                  status: Optional[str] = None,
                  limit: int = 200) -> list[MessageOut]:
    epoch_id = None
    if uidvalidity is not None:
        epoch = _db().query_one(
            "SELECT id FROM mailbox_epochs WHERE uidvalidity=?", (uidvalidity,)
        )
        if epoch is None:
            return []
        epoch_id = epoch["id"]
    rows = _db().list_messages(epoch_id=epoch_id, status=status, limit=limit)
    return [_message_out(r) for r in rows]


@app.get("/api/messages/{message_pk}", response_model=MessageOut,
         tags=["来源查询"], summary="按内部 ID 查询单封邮件及其附件来源")
def get_message(message_pk: int) -> MessageOut:
    row = _db().get_message_with_epoch(message_pk)
    if row is None:
        raise HTTPException(404, "邮件记录不存在")
    return _message_out(row)


@app.get("/api/sources/{uidvalidity}/{uid}", response_model=MessageOut,
         tags=["来源查询"], summary="按来源（UIDVALIDITY + UID）定位邮件")
def get_source(uidvalidity: int, uid: int) -> MessageOut:
    epoch = _db().query_one(
        "SELECT id FROM mailbox_epochs WHERE uidvalidity=?", (uidvalidity,)
    )
    if epoch is None:
        raise HTTPException(404, f"UIDVALIDITY={uidvalidity} 代际不存在")
    row = _db().get_message(epoch["id"], uid)
    if row is None:
        raise HTTPException(404, f"UIDVALIDITY={uidvalidity}, UID={uid} 无记录")
    full = _db().get_message_with_epoch(row["id"])
    return _message_out(full)


# ---------------------------------------------------------------- 识别任务
@app.post("/api/recognition/tasks/claim", response_model=ClaimOut,
          tags=["识别任务"],
          summary="领取一项待识别 PDF 任务（含原件地址与 30 秒凭据）",
          responses={
              200: {"description": "领取成功，credential 仅在本响应出现一次"},
              204: {"description": "当前没有可领取的任务"},
          })
def claim_recognition_task(request: Request):
    base_url = str(request.base_url).rstrip("/")
    task = _recognition().claim(base_url)
    if task is None:
        return Response(status_code=204)
    return ClaimOut(**task)


@app.post("/api/recognition/tasks/{task_id}/submit", response_model=SubmitOut,
          tags=["识别任务"],
          summary="提交识别结果（非空发票号 + 非负整数分金额）")
def submit_recognition_result(task_id: int, body: SubmitIn):
    status_code, payload = _recognition().submit(
        task_id, body.credential, body.invoice_number, body.amount_cents
    )
    if status_code != 200:
        raise HTTPException(status_code=status_code, detail=payload)
    return SubmitOut(**payload)


@app.get("/api/recognition/tasks", response_model=list[RecognitionTaskOut],
         tags=["识别任务"], summary="识别任务列表（可按状态/来源过滤）")
def list_recognition_tasks(status: Optional[str] = None,
                           uidvalidity: Optional[int] = None,
                           uid: Optional[int] = None,
                           limit: int = 200) -> list[RecognitionTaskOut]:
    if status is not None and status not in ("pending", "completed"):
        raise HTTPException(422, "status 只能是 pending 或 completed")
    return [
        _recognition_task_out(r)
        for r in _db().list_recognition_tasks(
            status=status, uidvalidity=uidvalidity, uid=uid, limit=limit
        )
    ]


@app.get("/api/recognition/tasks/{task_id}", response_model=RecognitionTaskOut,
         tags=["识别任务"], summary="按任务 ID 查询状态与结果")
def get_recognition_task(task_id: int) -> RecognitionTaskOut:
    row = _db().get_recognition_task(task_id)
    if row is None:
        raise HTTPException(404, "识别任务不存在")
    return _recognition_task_out(row)


@app.get("/api/recognition/stats", response_model=RecognitionStatsOut,
         tags=["识别任务"], summary="全部识别任务的完成进度")
def recognition_stats() -> RecognitionStatsOut:
    p = _db().recognition_progress()
    return RecognitionStatsOut(
        total=p["total"] or 0, completed=p["completed"] or 0,
        pending=p["pending"] or 0, leased=p["leased"] or 0,
    )


@app.get("/api/messages/{message_pk}/recognition",
         response_model=MessageRecognitionOut, tags=["识别任务"],
         summary="按邮件查询识别结果（全部 PDF 完成才返回结果，否则仅进度）")
def message_recognition(message_pk: int) -> MessageRecognitionOut:
    summary = _recognition().message_summary(message_pk)
    if summary is None:
        raise HTTPException(404, "邮件记录不存在")
    return MessageRecognitionOut(**summary)


@app.get("/api/sources/{uidvalidity}/{uid}/recognition",
         response_model=MessageRecognitionOut, tags=["识别任务"],
         summary="按来源（UIDVALIDITY + UID）查询识别结果")
def source_recognition(uidvalidity: int, uid: int) -> MessageRecognitionOut:
    epoch = _db().query_one(
        "SELECT id FROM mailbox_epochs WHERE uidvalidity=?", (uidvalidity,)
    )
    if epoch is None:
        raise HTTPException(404, f"UIDVALIDITY={uidvalidity} 代际不存在")
    msg = _db().get_message(epoch["id"], uid)
    if msg is None:
        raise HTTPException(404, f"UIDVALIDITY={uidvalidity}, UID={uid} 无记录")
    summary = _recognition().message_summary(msg["id"])
    assert summary is not None
    return MessageRecognitionOut(**summary)


# ---------------------------------------------------------------- 下载
@app.get("/api/messages/attachments/{attachment_pk}/download",
         tags=["下载"], summary="按附件 ID 下载 PDF 原件")
def download_attachment(attachment_pk: int):
    row = _db().get_attachment(attachment_pk)
    if row is None:
        raise HTTPException(404, "附件不存在")
    return _serve_blob(row["storage_path"], row["filename"])


@app.get("/api/sources/{uidvalidity}/{uid}/download",
         tags=["下载"],
         summary="按来源（UIDVALIDITY + UID）下载该封全部 PDF（ZIP 打包）")
def download_source_bundle(uidvalidity: int, uid: int):
    import io
    import zipfile

    epoch = _db().query_one(
        "SELECT id FROM mailbox_epochs WHERE uidvalidity=?", (uidvalidity,)
    )
    if epoch is None:
        raise HTTPException(404, f"UIDVALIDITY={uidvalidity} 代际不存在")
    msg = _db().get_message(epoch["id"], uid)
    if msg is None:
        raise HTTPException(404, f"UIDVALIDITY={uidvalidity}, UID={uid} 无记录")
    attachments = _db().list_attachments(msg["id"])
    if not attachments:
        raise HTTPException(404, "该邮件没有已落盘的 PDF 附件")

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for att in attachments:
            abs_path = _storage().absolute_path(att["storage_path"])
            # 防目录穿越：正式文件必须位于 blob_root 之内
            if not os.path.isfile(abs_path):
                raise HTTPException(410, f"原件缺失: {att['storage_path']}")
            zf.write(abs_path, arcname=att["filename"])
    buffer.seek(0)
    from fastapi.responses import Response
    return Response(
        content=buffer.getvalue(),
        media_type="application/zip",
        headers={
            "Content-Disposition":
                f'attachment; filename="uv-{uidvalidity}-uid-{uid}.zip"'
        },
    )


def _serve_blob(storage_path: str, download_name: str):
    storage = _storage()
    abs_path = storage.absolute_path(storage_path)
    # 规范化后必须仍在 blob_root 内，防止任何路径穿越
    root = os.path.realpath(storage.blob_root)
    target = os.path.realpath(abs_path)
    if os.path.commonpath([root, target]) != root or not os.path.isfile(target):
        raise HTTPException(404, "原件不可用")
    return FileResponse(
        target, media_type="application/pdf",
        filename=download_name,
    )
