#!/usr/bin/env python3
# coding=utf-8
"""
MPMS iter_results 简单示例
展示如何使用 iter_results() 替代 collector 函数
"""

import time
import threading
from mpms import MPMS


def process_data(item_id, delay=0.1):
    """模拟数据处理任务"""
    print(f"处理任务 {item_id}...")
    time.sleep(delay)
    
    # 模拟偶尔出错
    if item_id % 7 == 0:
        raise ValueError(f"任务 {item_id} 处理失败")
    
    return {
        'id': item_id,
        'result': item_id ** 2,
        'message': f'任务 {item_id} 处理完成'
    }


def demo_iter_results_after_close():
    """演示在close()之后使用iter_results（传统方式）"""
    print("=== 演示：在close()之后使用iter_results ===")
    
    # 创建 MPMS 实例，不需要提供 collector
    m = MPMS(process_data, processes=2, threads=3)
    m.start()
    
    # 提交一批任务
    task_count = 10
    print(f"提交 {task_count} 个任务...")
    for i in range(task_count):
        m.put(i, delay=0.05)
    
    # 先关闭任务队列
    m.close()
    
    # 使用 iter_results 获取并处理结果
    print("\n处理结果:")
    success_count = 0
    error_count = 0
    
    for meta, result in m.iter_results():
        # meta 包含任务的元信息
        task_id = meta.args[0]
        
        if isinstance(result, Exception):
            # 处理失败的任务
            error_count += 1
            print(f"  ❌ 任务 {task_id} 失败: {result}")
        else:
            # 处理成功的任务
            success_count += 1
            print(f"  ✅ 任务 {task_id} 成功: {result['message']}, 结果={result['result']}")
    
    # 等待所有进程结束
    m.join(close=False)  # 已经调用过 close()
    
    # 打印统计信息
    print(f"\n任务完成统计:")
    print(f"  成功: {success_count}")
    print(f"  失败: {error_count}")
    print(f"  总计: {task_count}")


def demo_iter_results_before_close():
    """演示在close()之前使用iter_results（新功能）"""
    print("\n=== 演示：在close()之前使用iter_results（实时处理） ===")
    
    # 创建 MPMS 实例
    m = MPMS(process_data, processes=2, threads=3)
    m.start()
    
    # 提交一些初始任务
    initial_tasks = 5
    print(f"提交 {initial_tasks} 个初始任务...")
    for i in range(initial_tasks):
        m.put(i, delay=0.1)
    
    # 在另一个线程中继续提交任务
    def submit_more_tasks():
        time.sleep(0.2)  # 等待一下
        print("继续提交更多任务...")
        for i in range(initial_tasks, initial_tasks + 5):
            m.put(i, delay=0.1)
            time.sleep(0.05)  # 逐个提交
        
        time.sleep(0.3)  # 等待一下再关闭
        print("关闭任务队列...")
        m.close()
    
    # 启动提交任务的线程
    submit_thread = threading.Thread(target=submit_more_tasks)
    submit_thread.start()
    
    # 实时处理结果（在close之前开始）
    print("\n实时处理结果:")
    success_count = 0
    error_count = 0
    processed_count = 0
    
    for meta, result in m.iter_results(timeout=1.0):  # 设置超时避免无限等待
        task_id = meta.args[0]
        processed_count += 1
        
        if isinstance(result, Exception):
            error_count += 1
            print(f"  ❌ 任务 {task_id} 失败: {result}")
        else:
            success_count += 1
            print(f"  ✅ 任务 {task_id} 成功: {result['message']}, 结果={result['result']}")
        
        # 当处理完所有任务后退出
        if processed_count >= 10:
            break
    
    # 等待提交线程结束
    submit_thread.join()
    
    # 等待所有进程结束
    m.join(close=False)
    
    # 打印统计信息
    print(f"\n实时处理统计:")
    print(f"  成功: {success_count}")
    print(f"  失败: {error_count}")
    print(f"  总计: {processed_count}")


def demo_streaming_processing():
    """演示流式处理：边提交边处理"""
    print("\n=== 演示：流式处理（边提交边处理） ===")
    
    m = MPMS(process_data, processes=2, threads=2)
    m.start()
    
    # 在另一个线程中持续提交任务
    def continuous_submit():
        for i in range(15):
            print(f"提交任务 {i}")
            m.put(i, delay=0.05)
            time.sleep(0.1)  # 模拟任务间隔
        
        print("所有任务提交完成，关闭队列...")
        m.close()
    
    submit_thread = threading.Thread(target=continuous_submit)
    submit_thread.start()
    
    # 实时处理结果
    print("开始流式处理结果...")
    results_processed = 0
    
    for meta, result in m.iter_results(timeout=2.0):
        task_id = meta.args[0]
        results_processed += 1
        
        if isinstance(result, Exception):
            print(f"  🔴 任务 {task_id} 处理失败")
        else:
            print(f"  🟢 任务 {task_id} 处理成功，结果: {result['result']}")
        
        # 模拟结果处理时间
        time.sleep(0.02)
    
    submit_thread.join()
    m.join(close=False)
    
    print(f"流式处理完成，共处理 {results_processed} 个结果")


def main():
    """运行所有演示"""
    demo_iter_results_after_close()
    demo_iter_results_before_close()
    demo_streaming_processing()


if __name__ == '__main__':
    main() 