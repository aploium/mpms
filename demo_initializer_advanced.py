#!/usr/bin/env python3
# coding=utf-8
"""
MPMS 初始化函数高级示例

演示实际应用场景：
1. 进程级别初始化数据库连接池
2. 线程级别初始化独立的HTTP会话
3. 错误处理和资源清理
"""

import os
import time
import logging
import threading
import multiprocessing
from mpms import MPMS

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(processName)s[%(process)d] - %(threadName)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 模拟的数据库连接池类
class DatabasePool:
    def __init__(self, host, port, pool_size=5):
        self.host = host
        self.port = port
        self.pool_size = pool_size
        self.pid = os.getpid()
        logger.info(f"Created database pool for process {self.pid}: {host}:{port}")
    
    def get_connection(self):
        return f"DBConnection-{self.pid}-{threading.current_thread().name}"
    
    def close(self):
        logger.info(f"Closing database pool for process {self.pid}")

# 模拟的HTTP会话类
class HTTPSession:
    def __init__(self, timeout=30):
        self.timeout = timeout
        self.thread_name = threading.current_thread().name
        self.session_id = f"Session-{self.thread_name}-{time.time()}"
        logger.info(f"Created HTTP session {self.session_id}")
    
    def request(self, url):
        return f"Response from {url} via {self.session_id}"
    
    def close(self):
        logger.info(f"Closing HTTP session {self.session_id}")

# 全局变量
db_pool = None
thread_local = threading.local()

def process_init(db_config, app_config):
    """
    进程初始化函数
    初始化数据库连接池和其他进程级资源
    """
    global db_pool
    
    try:
        # 初始化数据库连接池
        db_pool = DatabasePool(
            host=db_config['host'],
            port=db_config['port'],
            pool_size=db_config.get('pool_size', 5)
        )
        
        # 可以在这里初始化其他进程级资源
        # 例如：Redis连接、消息队列连接等
        
        logger.info(f"Process {os.getpid()} initialized with app config: {app_config}")
        
    except Exception as e:
        logger.error(f"Failed to initialize process: {e}")
        raise  # 重新抛出异常，让进程退出

def thread_init(api_config):
    """
    线程初始化函数
    为每个线程创建独立的HTTP会话
    """
    try:
        # 初始化线程本地的HTTP会话
        thread_local.http_session = HTTPSession(
            timeout=api_config.get('timeout', 30)
        )
        
        # 初始化其他线程本地资源
        thread_local.api_config = api_config
        thread_local.request_count = 0
        thread_local.error_count = 0
        
        logger.info(f"Thread {threading.current_thread().name} initialized")
        
    except Exception as e:
        logger.error(f"Failed to initialize thread: {e}")
        raise

def worker(user_id, action):
    """
    工作函数
    模拟处理用户请求
    """
    thread_name = threading.current_thread().name
    
    try:
        # 使用数据库连接
        db_conn = db_pool.get_connection()
        logger.debug(f"Processing user {user_id} action {action} with {db_conn}")
        
        # 使用HTTP会话发送请求
        thread_local.request_count += 1
        api_url = f"https://api.example.com/users/{user_id}/{action}"
        response = thread_local.http_session.request(api_url)
        
        # 模拟一些处理
        time.sleep(0.05)
        
        # 返回处理结果
        result = {
            'user_id': user_id,
            'action': action,
            'db_connection': db_conn,
            'api_response': response,
            'thread_requests': thread_local.request_count,
            'process_pid': os.getpid(),
            'thread_name': thread_name
        }
        
        return result
        
    except Exception as e:
        thread_local.error_count += 1
        logger.error(f"Error processing user {user_id}: {e}")
        raise

def collector(meta, result):
    """
    结果收集函数
    """
    if isinstance(result, Exception):
        logger.error(f"Task {meta.taskid} failed: {result}")
        return
    
    logger.info(f"Completed: user={result['user_id']}, "
                f"action={result['action']}, "
                f"thread_requests={result['thread_requests']}")

def cleanup_process():
    """
    清理进程资源（这个函数需要在进程退出时手动调用）
    """
    global db_pool
    if db_pool:
        db_pool.close()

def cleanup_thread():
    """
    清理线程资源（这个函数需要在线程退出时手动调用）
    """
    if hasattr(thread_local, 'http_session'):
        thread_local.http_session.close()

def main():
    # 数据库配置
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'pool_size': 10
    }
    
    # 应用配置
    app_config = {
        'app_name': 'UserService',
        'version': '1.0.0'
    }
    
    # API配置
    api_config = {
        'timeout': 30,
        'retry': 3,
        'base_url': 'https://api.example.com'
    }
    
    # 创建 MPMS 实例
    m = MPMS(
        worker,
        collector,
        processes=3,
        threads=4,
        process_initializer=process_init,
        process_initargs=(db_config, app_config),
        thread_initializer=thread_init,
        thread_initargs=(api_config,),
    )
    
    # 启动
    logger.info("Starting MPMS...")
    m.start()
    
    # 模拟用户操作
    users = range(1, 101)  # 100个用户
    actions = ['login', 'view', 'update', 'logout']
    
    logger.info("Submitting tasks...")
    for user_id in users:
        for action in actions:
            m.put(user_id, action)
    
    # 等待完成
    m.join()
    
    logger.info(f"All tasks completed. Total: {m.total_count}, Finished: {m.finish_count}")

if __name__ == '__main__':
    main() 