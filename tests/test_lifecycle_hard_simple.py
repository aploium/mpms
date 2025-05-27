#!/usr/bin/env python3
# coding=utf-8
"""
测试 lifecycle_duration_hard 功能 - 简化版
"""

import time
import logging
from mpms import MPMS

# 只设置根日志器，避免多进程日志问题
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(message)s'
)

def worker_hang(index):
    """会hang住的任务"""
    if index == 5:  # 第5个任务会hang住
        print(f"Task {index} will hang for 10 seconds...")
        time.sleep(10)  # 模拟hang住10秒
    else:
        time.sleep(0.1)
    return f"Task {index} completed"

def collector(meta, result):
    """结果收集器"""
    if isinstance(result, Exception):
        print(f"[ERROR] Task {meta.taskid} failed: {type(result).__name__}: {result}")
    else:
        print(f"[OK] {result}")

def test_task_timeout():
    """测试任务超时功能"""
    print("\n=== Testing task timeout (lifecycle_duration_hard=3s) ===")
    m = MPMS(
        worker_hang,
        collector,
        processes=2,
        threads=2,
        lifecycle_duration_hard=3.0,  # 3秒硬性超时
        subproc_check_interval=0.5
    )
    m.start()
    
    # 提交10个任务
    for i in range(10):
        m.put(i)
        print(f"Submitted task {i}")
    
    # 等待足够的时间让超时机制生效
    print("\nWaiting for tasks to complete or timeout...")
    time.sleep(5)
    
    m.join()
    print(f"\nSummary: Total tasks: {m.total_count}, Finished: {m.finish_count}")

def test_process_hard_timeout():
    """测试进程硬性超时"""
    print("\n=== Testing process hard timeout ===")
    
    def worker_very_slow(index):
        """非常慢的任务，会导致进程超时"""
        print(f"Task {index} starting (will take 5 seconds)...")
        time.sleep(5)
        return f"Task {index} completed"
    
    m = MPMS(
        worker_very_slow,
        collector,
        processes=1,
        threads=1,
        lifecycle_duration_hard=3.0,  # 3秒后进程会被杀死
        subproc_check_interval=0.5
    )
    m.start()
    
    # 提交3个任务，每个需要5秒，但进程3秒后会被杀死
    for i in range(3):
        m.put(i)
        print(f"Submitted task {i}")
    
    print("\nWaiting for process timeout...")
    time.sleep(5)
    
    m.join()
    print(f"\nSummary: Total tasks: {m.total_count}, Finished: {m.finish_count}")

if __name__ == '__main__':
    test_task_timeout()
    print("\n" + "="*60 + "\n")
    test_process_hard_timeout() 