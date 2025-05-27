#!/usr/bin/env python3
# coding=utf-8
"""
优雅退出机制演示

这个示例展示了如何使用优雅退出机制来处理各种异常情况，
让worker进程能够主动标记自己为不健康状态。
"""
import time
import os
import random
import psutil
from mpms import MPMS, WorkerGracefulDie


# 自定义异常，用于触发优雅退出
class ResourceExhausted(Exception):
    """资源耗尽异常"""
    pass


def demo_memory_monitoring():
    """演示：监控内存使用，超过阈值时触发优雅退出"""
    print("\n=== Demo: Memory Monitoring ===")
    results = []
    
    def worker(index):
        # 模拟内存使用检查
        process = psutil.Process(os.getpid())
        memory_percent = process.memory_percent()
        
        print(f"Task {index}: Memory usage {memory_percent:.2f}%")
        
        # 假设内存使用超过某个阈值
        if memory_percent > 50:  # 实际应用中可能设置更高的阈值
            raise MemoryError(f"Memory usage too high: {memory_percent:.2f}%")
        
        # 模拟一些内存密集型工作
        data = [random.random() for _ in range(100000)]
        result = sum(data) / len(data)
        
        return f"task_{index}_result_{result:.4f}"
    
    def collector(meta, result):
        if isinstance(result, Exception):
            print(f"Error in {meta.taskid}: {type(result).__name__} - {result}")
        else:
            print(f"Success: {result}")
        results.append(result)
    
    m = MPMS(
        worker,
        collector,
        processes=2,
        threads=1,
        worker_graceful_die_timeout=5,
        worker_graceful_die_exceptions=(MemoryError,)  # MemoryError 触发优雅退出
    )
    
    m.start()
    
    # 提交任务
    for i in range(10):
        m.put(i)
        time.sleep(0.1)
    
    m.join()
    
    print(f"\nCompleted {len(results)} tasks")


def demo_health_check():
    """演示：定期健康检查，失败时触发优雅退出"""
    print("\n\n=== Demo: Health Check ===")
    results = []
    health_status = {"healthy": True}  # 模拟健康状态
    
    def worker(index):
        # 执行健康检查
        if not health_status["healthy"]:
            raise WorkerGracefulDie("Health check failed")
        
        # 模拟某些任务会导致健康状态变差
        if index == 5:
            health_status["healthy"] = False
            print(f"Task {index}: Marking process as unhealthy")
        
        # 正常处理任务
        time.sleep(0.2)
        return f"task_{index}_completed"
    
    def collector(meta, result):
        if isinstance(result, WorkerGracefulDie):
            print(f"Worker graceful die: {result}")
        elif isinstance(result, Exception):
            print(f"Error: {type(result).__name__} - {result}")
        else:
            print(f"Completed: {result}")
        results.append(result)
    
    m = MPMS(
        worker,
        collector,
        processes=1,  # 单进程以便演示
        threads=2,
        worker_graceful_die_timeout=3,
    )
    
    m.start()
    
    for i in range(10):
        m.put(i)
        time.sleep(0.1)
    
    m.join()
    
    print(f"\nProcessed {len(results)} tasks")


def demo_resource_limits():
    """演示：资源限制检查"""
    print("\n\n=== Demo: Resource Limits ===")
    results = []
    task_counter = {"count": 0, "max_tasks": 5}  # 每个进程最多处理5个任务
    
    def worker(index):
        # 检查是否达到资源限制
        task_counter["count"] += 1
        
        if task_counter["count"] > task_counter["max_tasks"]:
            raise ResourceExhausted(
                f"Process reached task limit: {task_counter['count']}/{task_counter['max_tasks']}"
            )
        
        print(f"Task {index}: Processing ({task_counter['count']}/{task_counter['max_tasks']})")
        
        # 模拟任务处理
        time.sleep(0.3)
        return f"task_{index}_done"
    
    def collector(meta, result):
        if isinstance(result, Exception):
            print(f"Exception: {type(result).__name__} - {result}")
        else:
            print(f"Result: {result}")
        results.append(result)
    
    m = MPMS(
        worker,
        collector,
        processes=2,
        threads=1,
        worker_graceful_die_timeout=2,
        worker_graceful_die_exceptions=(ResourceExhausted, WorkerGracefulDie)
    )
    
    m.start()
    
    # 提交超过限制的任务数
    for i in range(15):
        m.put(i)
    
    m.join()
    
    print(f"\nTotal results: {len(results)}")


def demo_graceful_shutdown():
    """演示：优雅关闭"""
    print("\n\n=== Demo: Graceful Shutdown ===")
    results = []
    shutdown_signal = {"shutdown": False}
    
    def worker(index):
        # 检查关闭信号
        if shutdown_signal["shutdown"]:
            raise WorkerGracefulDie("Received shutdown signal")
        
        # 模拟长时间运行的任务
        print(f"Task {index}: Starting long operation...")
        
        # 在任务中间设置关闭信号
        if index == 3:
            shutdown_signal["shutdown"] = True
            print("Shutdown signal set!")
        
        time.sleep(0.5)
        return f"task_{index}_finished"
    
    def collector(meta, result):
        results.append((meta.taskid, result))
        if isinstance(result, WorkerGracefulDie):
            print(f"Graceful shutdown: {result}")
        else:
            print(f"Completed: {meta.taskid}")
    
    m = MPMS(
        worker,
        collector,
        processes=1,
        threads=2,
        worker_graceful_die_timeout=1,  # 短超时以快速关闭
    )
    
    m.start()
    
    for i in range(8):
        m.put(i)
    
    m.join()
    
    print(f"\nProcessed {len(results)} tasks before shutdown")


if __name__ == '__main__':
    print("MPMS Graceful Die Mechanism Demonstrations")
    print("=" * 50)
    
    # 运行各种演示
    demo_memory_monitoring()
    demo_health_check()
    demo_resource_limits()
    demo_graceful_shutdown()
    
    print("\n" + "=" * 50)
    print("All demonstrations completed!") 