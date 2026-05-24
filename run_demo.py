"""一键演示脚本

自动启动 RabbitMQ、声明资源、启动所有服务、发送测试订单、展示异步效果。
运行方式：python run_demo.py
"""

import asyncio
import json
import subprocess
import sys
import time
import signal

import httpx

from config import (
    SIMULATE_CV,
    SIMULATE_CUSTOMS,
    SIMULATE_INVENTORY,
    SIMULATE_NLP,
)
from models import SAMPLE_ORDERS


def run(cmd: list[str], **kwargs) -> subprocess.Popen:
    return subprocess.Popen(
        cmd,
        stdout=sys.stdout,
        stderr=sys.stderr,
        **kwargs,
    )


def banner(text: str):
    print(f"\n{'=' * 60}")
    print(f"  {text}")
    print(f"{'=' * 60}\n", flush=True)


def main():
    banner("跨境电商异步消息枢纽 — RabbitMQ Tutorial Demo")

    # ── Step 1: 启动 RabbitMQ ───────────────────────────────
    print("[1/5] 检查 RabbitMQ...")
    result = subprocess.run(
        ["docker", "compose", "ps", "-q"],
        capture_output=True,
        text=True,
    )
    if not result.stdout.strip():
        print("  正在启动 RabbitMQ (docker compose up -d)...")
        subprocess.run(["docker", "compose", "up", "-d"], check=True)
        print("  等待 RabbitMQ 就绪...")
        time.sleep(8)
    else:
        print("  RabbitMQ 已运行")
    print()

    # ── Step 2: 声明 Exchange / Queue / Binding ──────────────
    print("[2/5] 声明 Exchange / Queue / Binding...")
    subprocess.run(
        [sys.executable, "setup_exchanges.py"],
        check=True,
    )
    print()

    # ── Step 3: 启动所有消费者 + Payment Service ────────────
    print("[3/5] 启动下游服务...")
    procs = []
    services = [
        ("库存服务 (Fanout)", "inventory_service.py"),
        ("海关服务 (Fanout)", "customs_service.py"),
        ("NLP 服务  (Topic)", "nlp_service.py"),
        ("CV 服务   (Topic)", "cv_service.py"),
    ]
    for name, script in services:
        p = run([sys.executable, script])
        procs.append(p)
        print(f"  {name} PID={p.pid}")

    # 启动 FastAPI
    api_proc = run(
        [sys.executable, "-m", "uvicorn", "payment_service:app", "--port", "8000"]
    )
    procs.append(api_proc)
    print(f"  Payment Service PID={api_proc.pid}")
    print("  等待服务就绪...")
    time.sleep(3)
    print()

    try:
        # ── Step 4: 发送测试订单 ──────────────────────────────
        print("[4/5] 发送测试订单...\n")
        sync_total = SIMULATE_INVENTORY + SIMULATE_CUSTOMS + SIMULATE_NLP + SIMULATE_CV
        print(f"  同步调用理论耗时: {sync_total}s（串行叠加）")
        print(f"  异步处理最慢服务: {max(SIMULATE_INVENTORY, SIMULATE_CUSTOMS, SIMULATE_NLP, SIMULATE_CV)}s（并行）\n")

        for order in SAMPLE_ORDERS:
            print(f"{'─' * 50}")
            print(f"  订单: {order.order_id} | 客户: {order.customer}")
            print(f"  商品: {', '.join(i.name for i in order.items)}")
            print(f"  文本: {'有' if order.has_text else '无'} | 图片: {'有' if order.has_image else '无'}")

            t0 = time.perf_counter()
            resp = httpx.post(
                f"http://localhost:8000/orders/{order.order_id}/pay",
                timeout=10,
            )
            elapsed_ms = (time.perf_counter() - t0) * 1000
            result = resp.json()
            print(f"  支付接口返回: {result['status']} ({elapsed_ms:.1f}ms)")
            print(f"  消息已发布到: {', '.join(result['published_to'])}")
            print()

        # ── Step 5: 等待处理完成 ──────────────────────────────
        slowest = max(SIMULATE_INVENTORY, SIMULATE_CUSTOMS, SIMULATE_NLP, SIMULATE_CV)
        print(f"[5/5] 等待所有服务处理完成（预计 ~{slowest}s）...\n")
        print(f"{'─' * 50}")
        print("  各服务日志输出:")
        print(f"{'─' * 50}\n")

        time.sleep(slowest + 2)

        banner("演示完成")
        print("  对比：")
        print(f"    同步调用: ~{sync_total}s（阻塞用户等待）")
        print(f"    异步处理: 支付接口 <100ms 返回，后台并行 ~{slowest}s 全部完成")
        print()
        print("  打开管理面板查看详情: http://localhost:15672 (guest/guest)")
        print()

    except httpx.ConnectError:
        print("\n  Payment Service 未就绪，请检查日志")
    except KeyboardInterrupt:
        print("\n  用户中断")
    finally:
        print("\n  正在停止所有服务...")
        for p in procs:
            p.terminate()
        for p in procs:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
        print("  已停止")


if __name__ == "__main__":
    main()
