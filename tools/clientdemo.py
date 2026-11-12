"""识别客户端演示：双客户端争领、到期重领、旧凭据迟到提交。

用法（容器内或本地均可）：
  python -m clientdemo --base http://localhost:8000
  python -m clientdemo --base http://localhost:8000 --lease-wait 2

步骤：
  1. 两个客户端同时 POST /api/recognition/tasks/claim —— 打印各自拿到的任务，
     若只有一条任务可用，第二个客户端收到 204；
  2. 客户端 A 不提交，等待凭据过期（默认 30 秒，可用 --lease-wait 缩短演示等待）；
  3. 客户端 B 重新领取同一任务，用新凭据提交成功；
  4. 客户端 A 用过期的旧凭据迟到提交 —— 必须被拒绝（409 credential_expired/stale）；
  5. 客户端 B 用原凭据重交同内容 —— 返回原结果；重交异内容 —— 被拒绝。
"""
from __future__ import annotations

import argparse
import json
import sys
import threading
import time
import urllib.error
import urllib.request


def call(method: str, url: str, body: dict | None = None) -> tuple[int, dict | None]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={"Content-Type": "application/json"} if data else {},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        return exc.code, (json.loads(raw) if raw else None)


def claim(base: str, label: str, results: list, barrier: threading.Barrier) -> None:
    barrier.wait()
    status, payload = call("POST", f"{base}/api/recognition/tasks/claim")
    results.append((label, status, payload))


def main() -> None:
    parser = argparse.ArgumentParser(description="识别任务客户端演示")
    parser.add_argument("--base", default="http://localhost:8000")
    parser.add_argument("--lease-wait", type=float, default=30.0,
                        help="等待租约到期的秒数（默认 30，与凭据有效期一致）")
    parser.add_argument("--invoice-number", default="DEMO-2026-001")
    parser.add_argument("--amount-cents", type=int, default=123456)
    args = parser.parse_args()
    base = args.base.rstrip("/")

    print("== 1. 双客户端并发领取 ==")
    results: list = []
    barrier = threading.Barrier(2)
    threads = [
        threading.Thread(target=claim, args=(base, f"客户端{name}", results, barrier))
        for name in ("A", "B")
    ]
    [t.start() for t in threads]
    [t.join() for t in threads]
    for label, status, payload in results:
        if status == 204:
            print(f"{label}: 204 无可领取任务")
        elif status == 200:
            print(f"{label}: 领取 task_id={payload['task_id']} "
                  f"file={payload['filename']} 到期={payload['lease_expires_at']}")
        else:
            print(f"{label}: HTTP {status} {payload}")

    claimed = [(label, p) for label, st, p in results if st == 200]
    if not claimed:
        print("没有可用任务：请先 POST /api/collect 采集并建任务")
        sys.exit(1)
    # 选"先领到、id 最小"的一方作为迟到提交的一方（A）
    claimed.sort(key=lambda item: item[1]["task_id"])
    late_label, late = claimed[0]
    other_label = "客户端B" if late_label == "客户端A" else "客户端A"

    # 把其余待办一次性领空（不提交），保证到期后重领按 id 顺序仍取到同一项
    drained = 0
    while True:
        status, payload = call("POST", f"{base}/api/recognition/tasks/claim")
        if status != 200:
            break
        drained += 1
    if drained:
        print(f"（演示前领空其余待办 {drained} 项，以便确定性地重领同一任务）")

    print(f"\n== 2. {late_label} 不提交，等待凭据过期（{args.lease_wait:.0f} 秒）==")
    time.sleep(args.lease_wait)

    print(f"\n== 3. {other_label} 重新领取到期任务（新凭据即刻替换旧凭据）==")
    status, payload = call("POST", f"{base}/api/recognition/tasks/claim")
    if status != 200 or payload["task_id"] != late["task_id"]:
        # 若其它待办尚未全部领空，先取到的可能不是同一项；按 id 排序后仍可能是任意项。
        print(f"重领结果 HTTP {status}，task_id={payload and payload.get('task_id')}"
              f"（期望 {late['task_id']}）")
    if status != 200:
        sys.exit(1)
    fresh = payload
    invoice_url = fresh["original_file_url"]
    try:
        with urllib.request.urlopen(invoice_url, timeout=10) as resp:
            print(f"原件地址 {invoice_url} -> HTTP {resp.status}, "
                  f"{resp.headers.get('Content-Type')}, {len(resp.read())} 字节")
    except urllib.error.HTTPError as exc:
        print(f"原件地址 {invoice_url} -> HTTP {exc.code}")

    # 迟到提交必须在重领并完成之前（旧凭据在重领那一刻即失效）。
    print(f"\n== 4. {late_label} 用旧凭据迟到提交（必须被拒绝）==")
    status, payload = call(
        "POST", f"{base}/api/recognition/tasks/{late['task_id']}/submit",
        {"credential": late["credential"],
         "invoice_number": args.invoice_number, "amount_cents": args.amount_cents},
    )
    print(f"旧凭据提交: HTTP {status} {payload}")

    print(f"\n== 5. {other_label} 用新凭据提交，再用原凭据重交 ==")
    status, payload = call(
        "POST", f"{base}/api/recognition/tasks/{fresh['task_id']}/submit",
        {"credential": fresh["credential"],
         "invoice_number": args.invoice_number, "amount_cents": args.amount_cents},
    )
    print(f"新凭据提交: HTTP {status} result={payload and payload.get('result')}")
    if status == 200 and fresh["task_id"] == late["task_id"]:
        status, payload = call(
            "POST", f"{base}/api/recognition/tasks/{fresh['task_id']}/submit",
            {"credential": fresh["credential"],
             "invoice_number": args.invoice_number, "amount_cents": args.amount_cents},
        )
        print(f"同内容重交: HTTP {status} replayed={payload and payload.get('replayed')} "
              f"result={payload and payload.get('result')}")
        status, payload = call(
            "POST", f"{base}/api/recognition/tasks/{fresh['task_id']}/submit",
            {"credential": fresh["credential"],
             "invoice_number": "CHANGED-INVOICE", "amount_cents": 1},
        )
        print(f"异内容重交: HTTP {status} {payload}")


if __name__ == "__main__":
    main()
