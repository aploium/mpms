#!/usr/bin/env python3
# coding=utf-8
"""
MPMS iter_results 功能演示

这个示例展示了如何使用 iter_results() 方法以迭代器的方式获取任务执行结果。
"""

import time
import random
from mpms import MPMS, WorkerGracefulDie


def worker_func(index, sleep_time=0.1):
    """模拟一个耗时的工作函数"""
    print(f"Worker处理任务 {index}, 休眠 {sleep_time} 秒...")
    time.sleep(sleep_time)
    
    # 模拟一些任务可能失败
    if index % 10 == 5:
        raise ValueError(f"任务 {index} 模拟失败")
    
    # 模拟一些任务触发优雅退出（只在index为50时触发，避免在demo中触发）
    if index == 50:
        raise WorkerGracefulDie(f"任务 {index} 触发优雅退出")
    
    return index * 2, f"result_{index}"


def demo_basic_iter_results():
    """基本的 iter_results 使用示例"""
    print("=== 基本 iter_results 示例 ===")
    
    # 创建 MPMS 实例，不指定 collector
    m = MPMS(worker_func, processes=2, threads=3)
    m.start()
    
    # 提交一些任务
    task_count = 20
    for i in range(task_count):
        m.put(i, sleep_time=random.uniform(0.05, 0.2))
    
    # 关闭任务队列
    m.close()
    
    # 使用 iter_results 获取结果
    success_count = 0
    error_count = 0
    
    for meta, result in m.iter_results():
        if isinstance(result, Exception):
            error_count += 1
            print(f"❌ 任务 {meta.args[0]} (ID: {meta.taskid}) 失败: {type(result).__name__}: {result}")
        else:
            success_count += 1
            doubled, text = result
            print(f"✅ 任务 {meta.args[0]} (ID: {meta.taskid}) 成功: doubled={doubled}, text={text}")
    
    print(f"\n总结: 成功 {success_count} 个, 失败 {error_count} 个")
    
    # 等待所有进程结束
    m.join(close=False)


def demo_iter_results_with_timeout():
    """带超时的 iter_results 示例"""
    print("\n=== 带超时的 iter_results 示例 ===")
    
    m = MPMS(worker_func, processes=1, threads=2)
    m.start()
    
    # 提交任务
    for i in range(5):
        m.put(i, sleep_time=i * 0.5)  # 任务耗时递增
    
    m.close()
    
    # 使用带超时的 iter_results
    for meta, result in m.iter_results(timeout=1.0):
        if isinstance(result, Exception):
            print(f"任务 {meta.args[0]} 失败: {result}")
        else:
            print(f"任务 {meta.args[0]} 完成: {result}")
    
    m.join(close=False)


def demo_iter_results_with_meta():
    """使用自定义 meta 信息的示例"""
    print("\n=== 使用自定义 meta 信息的示例 ===")
    
    # 创建带有自定义 meta 的 MPMS
    custom_meta = {
        'project': 'demo_project',
        'version': '1.0'
    }
    m = MPMS(worker_func, processes=2, threads=2, meta=custom_meta)
    m.start()
    
    # 提交任务
    for i in range(5):
        m.put(i, sleep_time=0.1)
    
    m.close()
    
    # 获取结果时可以访问自定义 meta
    for meta, result in m.iter_results():
        if not isinstance(result, Exception):
            print(f"任务 {meta.args[0]} 完成 - 项目: {meta.get('project')}, 版本: {meta.get('version')}")
    
    m.join(close=False)


def demo_lifecycle_with_iter_results():
    """结合生命周期功能的示例"""
    print("\n=== 结合生命周期功能的示例 ===")
    
    # 设置线程生命周期：每个线程处理3个任务后退出
    m = MPMS(
        worker_func,
        processes=1,
        threads=2,
        lifecycle=3,  # 每个线程处理3个任务后退出
        lifecycle_duration=5.0  # 或运行5秒后退出
    )
    m.start()
    
    # 提交多个任务
    for i in range(10):
        m.put(i, sleep_time=0.2)
    
    m.close()
    
    # 获取结果
    count = 0
    for meta, result in m.iter_results():
        if not isinstance(result, Exception):
            count += 1
            print(f"任务 {meta.args[0]} 完成 (已完成 {count} 个)")
    
    m.join(close=False)


def demo_error_handling():
    """错误处理示例"""
    print("\n=== 错误处理示例 ===")
    
    def error_prone_worker(index):
        """一个可能出错的工作函数"""
        if index == 0:
            raise ZeroDivisionError("不能除以零")
        elif index == 1:
            raise KeyError("找不到键")
        elif index == 2:
            raise WorkerGracefulDie("触发优雅退出")
        else:
            return f"成功处理 {index}"
    
    m = MPMS(error_prone_worker, processes=2, threads=2)
    m.start()
    
    # 提交任务
    for i in range(5):
        m.put(i)
    
    m.close()
    
    # 处理不同类型的错误
    for meta, result in m.iter_results():
        if isinstance(result, ZeroDivisionError):
            print(f"⚠️  任务 {meta.args[0]}: 数学错误 - {result}")
        elif isinstance(result, KeyError):
            print(f"⚠️  任务 {meta.args[0]}: 键错误 - {result}")
        elif isinstance(result, WorkerGracefulDie):
            print(f"⚠️  任务 {meta.args[0]}: 优雅退出 - {result}")
        elif isinstance(result, Exception):
            print(f"❌ 任务 {meta.args[0]}: 未知错误 - {type(result).__name__}: {result}")
        else:
            print(f"✅ 任务 {meta.args[0]}: {result}")
    
    m.join(close=False)


if __name__ == '__main__':
    # 运行所有示例
    demo_basic_iter_results()
    demo_iter_results_with_timeout()
    demo_iter_results_with_meta()
    demo_lifecycle_with_iter_results()
    demo_error_handling()
    
    print("\n所有示例运行完成！") 