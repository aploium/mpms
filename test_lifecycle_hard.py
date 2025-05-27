#!/usr/bin/env python3
# coding=utf-8
"""
测试 lifecycle_duration_hard 功能
"""

import time
import logging
from mpms import MPMS

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def worker_normal(index):
    """正常任务，快速完成"""
    time.sleep(0.1)
    return f"Task {index} completed"

def worker_hang(index):
    """会hang住的任务"""
    if index % 5 == 0:  # 每5个任务有一个会hang住
        print(f"Task {index} will hang...")
        time.sleep(100)  # 模拟hang住100秒
    else:
        time.sleep(0.1)
    return f"Task {index} completed"

def collector(meta, result):
    """结果收集器"""
    if isinstance(result, Exception):
        print(f"Task {meta.taskid} failed with error: {type(result).__name__}: {result}")
    else:
        print(f"Task {meta.taskid} result: {result}")

def test_normal_lifecycle():
    """测试正常的生命周期"""
    print("\n=== Testing normal lifecycle ===")
    m = MPMS(
        worker_normal,
        collector,
        processes=2,
        threads=2,
        lifecycle_duration_hard=5.0,  # 5秒硬性超时
        subproc_check_interval=0.5
    )
    m.start()
    
    # 提交20个任务
    for i in range(20):
        m.put(i)
    
    m.join()
    print(f"Total tasks: {m.total_count}, Finished: {m.finish_count}")

def test_hang_tasks():
    """测试会hang住的任务"""
    print("\n=== Testing hang tasks ===")
    m = MPMS(
        worker_hang,
        collector,
        processes=2,
        threads=2,
        lifecycle_duration_hard=3.0,  # 3秒硬性超时
        subproc_check_interval=0.5
    )
    m.start()
    
    # 提交20个任务，其中会有一些hang住
    for i in range(20):
        m.put(i)
        time.sleep(0.05)  # 稍微延迟一下，让任务分散
    
    # 等待一段时间，让超时机制生效
    time.sleep(10)
    
    m.join()
    print(f"Total tasks: {m.total_count}, Finished: {m.finish_count}")

def test_process_timeout():
    """测试进程超时"""
    print("\n=== Testing process timeout ===")
    
    def worker_slow(index):
        """慢速任务"""
        time.sleep(2)
        return f"Task {index} completed"
    
    m = MPMS(
        worker_slow,
        collector,
        processes=2,
        threads=1,
        lifecycle_duration_hard=5.0,  # 5秒后进程会被杀死
        subproc_check_interval=0.5
    )
    m.start()
    
    # 提交10个任务，每个需要2秒
    for i in range(10):
        m.put(i)
    
    m.join()
    print(f"Total tasks: {m.total_count}, Finished: {m.finish_count}")

if __name__ == '__main__':
    test_normal_lifecycle()
    test_hang_tasks()
    test_process_timeout() 