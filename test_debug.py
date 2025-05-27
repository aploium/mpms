#!/usr/bin/env python3
# coding=utf-8
"""
调试 lifecycle_duration_hard 功能
"""

import time
import logging
from mpms import MPMS

# 设置日志级别为DEBUG
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def worker_hang(index):
    """会hang住的任务"""
    if index == 2:  # 第2个任务会hang住
        print(f"[Worker] Task {index} will hang for 30 seconds...")
        time.sleep(30)  # 模拟hang住30秒
    else:
        print(f"[Worker] Task {index} processing...")
        time.sleep(0.2)
    return f"Task {index} completed"

def collector(meta, result):
    """结果收集器"""
    if isinstance(result, Exception):
        print(f"[Collector] Task {meta.taskid} failed: {type(result).__name__}: {result}")
    else:
        print(f"[Collector] Task {meta.taskid} result: {result}")

def test_simple():
    """简单测试"""
    print("\n=== Simple test with lifecycle_duration_hard=3s ===")
    
    m = MPMS(
        worker_hang,
        collector,
        processes=1,  # 只用1个进程，便于观察
        threads=1,    # 只用1个线程
        lifecycle_duration_hard=3.0,  # 3秒硬性超时
        subproc_check_interval=0.5   # 每0.5秒检查一次
    )
    m.start()
    
    # 提交5个任务
    for i in range(5):
        m.put(i)
        print(f"[Main] Submitted task {i}")
    
    print("\n[Main] All tasks submitted, now joining...")
    
    m.join()
    print(f"\n[Main] Summary: Total tasks: {m.total_count}, Finished: {m.finish_count}")

if __name__ == '__main__':
    test_simple() 