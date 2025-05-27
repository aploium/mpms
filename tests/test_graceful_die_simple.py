#!/usr/bin/env python3
# coding=utf-8
import time
import os
import logging
from mpms import MPMS, WorkerGracefulDie

# 设置日志级别以查看调试信息
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def test_graceful_die_basic():
    """基本的优雅退出测试"""
    print("\n=== Test Graceful Die Basic ===")
    results = []
    
    def worker(index):
        print(f"Worker processing task {index} in PID {os.getpid()}")
        if index == 2:
            print(f"Task {index} triggering graceful die!")
            raise WorkerGracefulDie("Worker wants to die")
        return f"task_{index}"
    
    def collector(meta, result):
        print(f"Collector received: {result}")
        results.append((meta.taskid, result))
    
    start_time = time.time()
    
    m = MPMS(
        worker,
        collector,
        processes=1,
        threads=1,
        worker_graceful_die_timeout=3,  # 3秒超时
    )
    
    m.start()
    
    # 提交5个任务
    for i in range(5):
        m.put(i)
    
    m.join()
    
    elapsed = time.time() - start_time
    print(f"\nTotal elapsed time: {elapsed:.2f} seconds")
    print(f"Results collected: {len(results)}")
    
    # 检查是否有优雅退出异常
    graceful_die_found = False
    for taskid, result in results:
        if isinstance(result, WorkerGracefulDie):
            graceful_die_found = True
            print(f"Found graceful die exception in {taskid}")
    
    print(f"Graceful die found: {graceful_die_found}")
    print(f"Expected time >= 3s, actual: {elapsed:.2f}s")


def test_graceful_die_with_multiple_threads():
    """多线程优雅退出测试"""
    print("\n\n=== Test Graceful Die with Multiple Threads ===")
    results = []
    
    def worker(index):
        tid = threading.current_thread().name
        print(f"Worker {tid} processing task {index}")
        if index == 2:
            print(f"Task {index} in {tid} triggering graceful die!")
            raise WorkerGracefulDie("Worker wants to die")
        # 模拟一些工作
        time.sleep(0.5)
        return f"task_{index}"
    
    def collector(meta, result):
        results.append((meta.taskid, result))
    
    start_time = time.time()
    
    m = MPMS(
        worker,
        collector,
        processes=1,
        threads=2,  # 2个线程
        worker_graceful_die_timeout=2,  # 2秒超时
    )
    
    m.start()
    
    # 提交任务
    for i in range(6):
        m.put(i)
    
    m.join()
    
    elapsed = time.time() - start_time
    print(f"\nTotal elapsed time: {elapsed:.2f} seconds")
    print(f"Results collected: {len(results)}")


if __name__ == '__main__':
    import threading
    test_graceful_die_basic()
    test_graceful_die_with_multiple_threads() 