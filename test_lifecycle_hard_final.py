#!/usr/bin/env python3
# coding=utf-8
"""
测试 lifecycle_duration_hard 功能 - 最终版
"""

import time
import logging
from mpms import MPMS

# 设置日志级别为INFO，显示关键信息
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def worker_hang(index):
    """会hang住的任务"""
    if index == 5:  # 第5个任务会hang住
        print(f"[Worker] Task {index} will hang for 20 seconds...")
        time.sleep(20)  # 模拟hang住20秒
    else:
        print(f"[Worker] Task {index} processing...")
        time.sleep(0.5)
    return f"Task {index} completed"

def collector(meta, result):
    """结果收集器"""
    if isinstance(result, Exception):
        print(f"[Collector] Task {meta.taskid} failed: {type(result).__name__}: {result}")
    else:
        print(f"[Collector] Task {meta.taskid} result: {result}")

def test_task_and_process_timeout():
    """测试任务和进程超时功能"""
    print("\n=== Testing task and process timeout (lifecycle_duration_hard=5s) ===")
    print("说明：")
    print("- Task 5 会hang住20秒")
    print("- lifecycle_duration_hard=5秒")
    print("- 预期：处理Task 5的进程会在5秒后被杀死，Task 5会超时")
    print("-" * 60)
    
    m = MPMS(
        worker_hang,
        collector,
        processes=2,
        threads=2,
        lifecycle_duration_hard=5.0,  # 5秒硬性超时
        subproc_check_interval=0.5   # 每0.5秒检查一次
    )
    m.start()
    
    # 提交10个任务
    for i in range(10):
        m.put(i)
        print(f"[Main] Submitted task {i}")
        time.sleep(0.1)
    
    print("\n[Main] All tasks submitted, waiting for completion or timeout...")
    
    # 等待足够的时间让所有任务完成或超时
    time.sleep(10)
    
    m.join()
    print(f"\n[Main] Summary: Total tasks: {m.total_count}, Finished: {m.finish_count}")

def test_multiple_hang_tasks():
    """测试多个hang任务的情况"""
    print("\n\n=== Testing multiple hang tasks ===")
    print("说明：")
    print("- 多个任务会hang住")
    print("- lifecycle_duration_hard=3秒")
    print("- 预期：hang住的任务都会超时")
    print("-" * 60)
    
    def worker_multi_hang(index):
        """多个任务会hang住"""
        if index in [2, 5, 8]:  # 这些任务会hang住
            print(f"[Worker] Task {index} will hang...")
            time.sleep(30)
        else:
            print(f"[Worker] Task {index} processing...")
            time.sleep(0.2)
        return f"Task {index} completed"
    
    m = MPMS(
        worker_multi_hang,
        collector,
        processes=2,
        threads=2,
        lifecycle_duration_hard=3.0,  # 3秒硬性超时
        subproc_check_interval=0.3   # 更频繁的检查
    )
    m.start()
    
    # 提交12个任务
    for i in range(12):
        m.put(i)
        print(f"[Main] Submitted task {i}")
        time.sleep(0.05)
    
    print("\n[Main] All tasks submitted, waiting...")
    
    # 等待足够时间
    time.sleep(8)
    
    m.join()
    print(f"\n[Main] Summary: Total tasks: {m.total_count}, Finished: {m.finish_count}")

if __name__ == '__main__':
    test_task_and_process_timeout()
    test_multiple_hang_tasks() 