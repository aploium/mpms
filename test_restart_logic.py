#!/usr/bin/env python3
# coding=utf-8
"""
测试进程重启逻辑
"""

import time
import logging
from mpms import MPMS

# 设置日志级别
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def worker_with_crash(index):
    """会崩溃的任务"""
    if index == 3:
        print(f"[Worker] Task {index} will crash the process...")
        import os
        os._exit(1)  # 强制退出进程
    elif index == 7:
        print(f"[Worker] Task {index} will hang...")
        time.sleep(10)
    else:
        print(f"[Worker] Task {index} processing...")
        time.sleep(0.3)
    return f"Task {index} completed"

def collector(meta, result):
    """结果收集器"""
    if isinstance(result, Exception):
        print(f"[Collector] Task {meta.taskid} failed: {type(result).__name__}: {result}")
    else:
        print(f"[Collector] Task {meta.taskid} result: {result}")

def test_process_restart():
    """测试进程重启逻辑"""
    print("\n=== Testing process restart logic ===")
    print("说明：")
    print("- Task 3 会导致进程崩溃")
    print("- Task 7 会hang住")
    print("- lifecycle_duration_hard=4秒")
    print("- 预期：进程崩溃后会重启，hang的进程会被杀死并重启")
    print("-" * 60)
    
    m = MPMS(
        worker_with_crash,
        collector,
        processes=2,
        threads=2,
        lifecycle_duration_hard=4.0,
        subproc_check_interval=0.5
    )
    m.start()
    
    # 提交15个任务
    for i in range(15):
        m.put(i)
        print(f"[Main] Submitted task {i}")
        time.sleep(0.1)
    
    print("\n[Main] All tasks submitted, waiting...")
    
    # 等待足够时间
    time.sleep(8)
    
    m.join()
    print(f"\n[Main] Summary: Total tasks: {m.total_count}, Finished: {m.finish_count}")
    print(f"[Main] Success rate: {m.finish_count}/{m.total_count} = {m.finish_count/m.total_count*100:.1f}%")

if __name__ == '__main__':
    test_process_restart() 