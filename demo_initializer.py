#!/usr/bin/env python3
# coding=utf-8
"""
MPMS 初始化函数示例

演示如何使用 process_initializer 和 thread_initializer 来初始化工作进程和线程
"""

import os
import time
import logging
import threading
import multiprocessing
from mpms import MPMS

# 设置日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(processName)s[%(process)d] - %(threadName)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 全局变量，用于存储每个进程的资源
process_resource = None

# 线程本地存储，用于存储每个线程的资源
thread_local = threading.local()


def process_init(shared_config):
    """
    进程初始化函数
    在每个工作进程启动时调用一次
    
    Args:
        shared_config: 共享配置信息
    """
    global process_resource
    
    pid = os.getpid()
    logger.info(f"Initializing process {pid} with config: {shared_config}")
    
    # 模拟初始化一些进程级别的资源
    # 例如：数据库连接池、缓存客户端等
    process_resource = {
        'pid': pid,
        'config': shared_config,
        'db_pool': f'DBPool-{pid}',  # 模拟数据库连接池
        'cache': f'Cache-{pid}',      # 模拟缓存客户端
        'start_time': time.time()
    }
    
    logger.info(f"Process {pid} initialized successfully")


def thread_init(thread_prefix, thread_config):
    """
    线程初始化函数
    在每个工作线程启动时调用一次
    
    Args:
        thread_prefix: 线程名称前缀
        thread_config: 线程配置
    """
    thread_name = threading.current_thread().name
    logger.info(f"Initializing thread {thread_name} with prefix: {thread_prefix}, config: {thread_config}")
    
    # 初始化线程本地存储
    thread_local.name = f"{thread_prefix}-{thread_name}"
    thread_local.config = thread_config
    thread_local.connection = f"Connection-{thread_name}"  # 模拟每个线程的独立连接
    thread_local.counter = 0
    
    logger.info(f"Thread {thread_name} initialized successfully")


def worker(task_id, task_data):
    """
    工作函数
    使用初始化时创建的资源
    """
    thread_name = threading.current_thread().name
    
    # 使用进程级别的资源
    logger.info(f"Task {task_id} using process resource: {process_resource['db_pool']}")
    
    # 使用线程级别的资源
    thread_local.counter += 1
    logger.info(f"Task {task_id} on thread {thread_local.name}, counter: {thread_local.counter}")
    
    # 模拟任务处理
    time.sleep(0.1)
    
    result = {
        'task_id': task_id,
        'task_data': task_data,
        'process_pid': process_resource['pid'],
        'thread_name': thread_local.name,
        'thread_counter': thread_local.counter,
        'timestamp': time.time()
    }
    
    return result


def collector(meta, result):
    """
    结果收集函数
    """
    if isinstance(result, Exception):
        logger.error(f"Task {meta.taskid} failed: {result}")
        return
    
    logger.info(f"Collected result: task_id={result['task_id']}, "
                f"process={result['process_pid']}, "
                f"thread={result['thread_name']}, "
                f"counter={result['thread_counter']}")


def main():
    # 配置信息
    shared_config = {
        'db_host': 'localhost',
        'db_port': 5432,
        'cache_host': 'localhost',
        'cache_port': 6379
    }
    
    thread_config = {
        'timeout': 30,
        'retry': 3
    }
    
    # 创建 MPMS 实例
    m = MPMS(
        worker,
        collector,
        processes=2,
        threads=3,
        process_initializer=process_init,
        process_initargs=(shared_config,),
        thread_initializer=thread_init,
        thread_initargs=('Worker', thread_config),
    )
    
    # 启动
    m.start()
    
    # 提交任务
    logger.info("Submitting tasks...")
    for i in range(20):
        m.put(i, f"data-{i}")
    
    # 等待完成
    m.join()
    
    logger.info(f"All tasks completed. Total: {m.total_count}, Finished: {m.finish_count}")


if __name__ == '__main__':
    main() 