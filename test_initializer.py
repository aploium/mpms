#!/usr/bin/env python3
# coding=utf-8
"""
测试 MPMS 初始化函数功能
"""

import os
import threading
import time
from mpms import MPMS

# 全局变量用于验证初始化
process_init_called = False
thread_init_count = 0
process_data = None
thread_data = threading.local()


def process_init(msg):
    """进程初始化函数"""
    global process_init_called, process_data
    process_init_called = True
    process_data = f"Process {os.getpid()} initialized with: {msg}"
    print(f"[PROCESS INIT] {process_data}")


def thread_init(prefix):
    """线程初始化函数"""
    global thread_init_count
    thread_init_count += 1
    thread_data.value = f"{prefix}-{threading.current_thread().name}"
    print(f"[THREAD INIT] Thread {threading.current_thread().name} initialized as {thread_data.value}")


def worker(x):
    """工作函数"""
    # 验证进程初始化
    if not process_init_called:
        raise RuntimeError("Process not initialized!")
    
    # 验证线程初始化
    if not hasattr(thread_data, 'value'):
        raise RuntimeError("Thread not initialized!")
    
    print(f"[WORKER] Task {x} running on {thread_data.value}")
    time.sleep(0.1)
    return x * 2


def main():
    print("=== Testing MPMS Initializer Functions ===\n")
    
    # 创建 MPMS 实例
    m = MPMS(
        worker,
        processes=2,
        threads=2,
        process_initializer=process_init,
        process_initargs=("Hello from main",),
        thread_initializer=thread_init,
        thread_initargs=("TestWorker",),
    )
    
    # 启动
    print("Starting MPMS...")
    m.start()
    
    # 提交任务
    print("\nSubmitting tasks...")
    for i in range(10):
        m.put(i)
    
    # 等待完成
    m.join()
    
    print(f"\nAll tasks completed!")
    print(f"Total tasks: {m.total_count}")
    print(f"Finished tasks: {m.finish_count}")
    
    # 测试初始化函数异常处理
    print("\n=== Testing Initializer Error Handling ===\n")
    
    def bad_process_init():
        raise ValueError("Process init failed!")
    
    def bad_thread_init():
        raise ValueError("Thread init failed!")
    
    # 测试进程初始化失败
    try:
        m2 = MPMS(
            worker,
            processes=1,
            threads=1,
            process_initializer=bad_process_init,
        )
        m2.start()
        time.sleep(1)  # 给进程一些时间来初始化
        print("Trying to submit task to failed process...")
        m2.put(1)
        m2.join()
    except Exception as e:
        print(f"Expected error caught: {e}")
    
    print("\nTest completed!")


if __name__ == '__main__':
    main() 