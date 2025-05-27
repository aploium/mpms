#!/usr/bin/env python3
# coding=utf-8
"""测试 MPMS 清理函数功能"""

import os
import time
import threading
import multiprocessing
from mpms import MPMS


def worker(index, sleep_time=0.1):
    """工作函数"""
    time.sleep(sleep_time)
    return f"Task {index} completed by {threading.current_thread().name}"


def collector(meta, result):
    """结果收集函数"""
    if isinstance(result, Exception):
        print(f"Error in task {meta.taskid}: {result}")
    else:
        print(f"Result: {result}")


def process_init():
    """进程初始化函数"""
    print(f"[INIT] Process {os.getpid()} initialized")


def thread_init():
    """线程初始化函数"""
    thread_name = threading.current_thread().name
    print(f"[INIT] Thread {thread_name} initialized")


def process_cleanup():
    """进程清理函数"""
    print(f"[CLEANUP] Process {os.getpid()} cleaning up...")
    # 这里可以执行一些清理操作，如关闭连接、释放资源等
    time.sleep(0.1)  # 模拟清理操作
    print(f"[CLEANUP] Process {os.getpid()} cleanup completed")


def thread_cleanup():
    """线程清理函数"""
    thread_name = threading.current_thread().name
    print(f"[CLEANUP] Thread {thread_name} cleaning up...")
    # 这里可以执行一些清理操作
    time.sleep(0.05)  # 模拟清理操作
    print(f"[CLEANUP] Thread {thread_name} cleanup completed")


def test_normal_exit():
    """测试正常退出时的清理函数调用"""
    print("\n=== Test 1: Normal Exit ===")
    
    m = MPMS(
        worker,
        collector,
        processes=2,
        threads=2,
        process_initializer=process_init,
        thread_initializer=thread_init,
        process_finalizer=process_cleanup,
        thread_finalizer=thread_cleanup,
    )
    
    m.start()
    
    # 提交一些任务
    for i in range(10):
        m.put(i, 0.01)
    
    m.join()
    print("Test 1 completed\n")


def test_lifecycle_exit():
    """测试因生命周期限制退出时的清理函数调用"""
    print("\n=== Test 2: Lifecycle Exit ===")
    
    m = MPMS(
        worker,
        collector,
        processes=2,
        threads=2,
        lifecycle=3,  # 每个线程处理3个任务后退出
        process_initializer=process_init,
        thread_initializer=thread_init,
        process_finalizer=process_cleanup,
        thread_finalizer=thread_cleanup,
    )
    
    m.start()
    
    # 提交更多任务，触发生命周期限制
    for i in range(20):
        m.put(i, 0.01)
    
    m.join()
    print("Test 2 completed\n")


def test_lifecycle_duration_exit():
    """测试因生命周期时间限制退出时的清理函数调用"""
    print("\n=== Test 3: Lifecycle Duration Exit ===")
    
    m = MPMS(
        worker,
        collector,
        processes=1,
        threads=2,
        lifecycle_duration=2.0,  # 线程运行2秒后退出
        process_initializer=process_init,
        thread_initializer=thread_init,
        process_finalizer=process_cleanup,
        thread_finalizer=thread_cleanup,
    )
    
    m.start()
    
    # 持续提交任务
    for i in range(100):
        m.put(i, 0.1)
        time.sleep(0.05)
        if i > 50:  # 避免无限等待
            break
    
    m.join()
    print("Test 3 completed\n")


def test_cleanup_error_handling():
    """测试清理函数抛出异常时的处理"""
    print("\n=== Test 4: Cleanup Error Handling ===")
    
    def bad_thread_cleanup():
        """会抛出异常的线程清理函数"""
        thread_name = threading.current_thread().name
        print(f"[CLEANUP] Thread {thread_name} cleanup starting...")
        raise ValueError("Simulated cleanup error")
    
    def bad_process_cleanup():
        """会抛出异常的进程清理函数"""
        print(f"[CLEANUP] Process {os.getpid()} cleanup starting...")
        raise RuntimeError("Simulated process cleanup error")
    
    m = MPMS(
        worker,
        collector,
        processes=1,
        threads=2,
        process_initializer=process_init,
        thread_initializer=thread_init,
        process_finalizer=bad_process_cleanup,
        thread_finalizer=bad_thread_cleanup,
    )
    
    m.start()
    
    # 提交少量任务
    for i in range(5):
        m.put(i, 0.01)
    
    m.join()
    print("Test 4 completed (errors should be logged but not crash)\n")


def main():
    """运行所有测试"""
    print("Testing MPMS Finalizer Functions")
    print("================================")
    
    # 设置日志级别以查看调试信息
    import logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    test_normal_exit()
    time.sleep(1)
    
    test_lifecycle_exit()
    time.sleep(1)
    
    test_lifecycle_duration_exit()
    time.sleep(1)
    
    test_cleanup_error_handling()
    
    print("\nAll tests completed!")


if __name__ == '__main__':
    main() 