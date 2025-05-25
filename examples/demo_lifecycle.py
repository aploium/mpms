#!/usr/bin/env python3
# coding=utf-8
"""
MPMS lifecycle 和 lifecycle_duration 功能演示

这个例子展示了如何使用生命周期功能来控制工作线程的轮转
"""

import time
import logging
from mpms import MPMS

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def long_running_worker(task_id):
    """模拟长时间运行的任务"""
    print(f"[Worker] Processing task {task_id}")
    time.sleep(1)  # 模拟耗时操作
    return f"Task {task_id} completed"


def result_collector(meta, result):
    """收集并处理结果"""
    if isinstance(result, Exception):
        print(f"[Collector] Task failed: {result}")
    else:
        print(f"[Collector] {result}")


def demo_count_based_lifecycle():
    """演示基于任务计数的生命周期"""
    print("\n" + "="*50)
    print("Demo 1: Count-based lifecycle")
    print("每个工作线程处理3个任务后会自动退出并重启")
    print("="*50)
    
    m = MPMS(
        worker=long_running_worker,
        collector=result_collector,
        processes=1,
        threads=2,
        lifecycle=3  # 每个线程处理3个任务后退出
    )
    
    m.start()
    
    # 提交10个任务
    for i in range(10):
        m.put(i)
    
    m.join()
    print(f"\n总计：提交 {m.total_count} 个任务，完成 {m.finish_count} 个任务")


def demo_time_based_lifecycle():
    """演示基于时间的生命周期"""
    print("\n" + "="*50)
    print("Demo 2: Time-based lifecycle")
    print("每个工作线程运行5秒后会自动退出并重启")
    print("="*50)
    
    m = MPMS(
        worker=long_running_worker,
        collector=result_collector,
        processes=1,
        threads=2,
        lifecycle_duration=5.0  # 每个线程运行5秒后退出
    )
    
    m.start()
    
    # 持续提交任务8秒
    start_time = time.time()
    task_id = 0
    while time.time() - start_time < 8:
        m.put(task_id)
        task_id += 1
        time.sleep(0.5)
    
    m.join()
    print(f"\n总计：提交 {m.total_count} 个任务，完成 {m.finish_count} 个任务")


def demo_combined_lifecycle():
    """演示同时使用两种生命周期"""
    print("\n" + "="*50)
    print("Demo 3: Combined lifecycle")
    print("工作线程会在处理5个任务或运行3秒后退出（以先到者为准）")
    print("="*50)
    
    m = MPMS(
        worker=long_running_worker,
        collector=result_collector,
        processes=1,
        threads=1,  # 使用单线程便于观察
        lifecycle=5,         # 5个任务
        lifecycle_duration=3.0  # 或3秒
    )
    
    m.start()
    
    # 快速提交任务（每个任务需要1秒，所以3秒内只能完成约3个任务）
    for i in range(10):
        m.put(i)
    
    m.join()
    print(f"\n总计：提交 {m.total_count} 个任务，完成 {m.finish_count} 个任务")
    print("（由于每个任务需要1秒，3秒的时间限制会先触发）")


if __name__ == '__main__':
    demo_count_based_lifecycle()
    demo_time_based_lifecycle()
    demo_combined_lifecycle()
    
    print("\n" + "="*50)
    print("所有演示完成！")
    print("="*50) 