#!/usr/bin/env python3
# coding=utf-8
import pytest
import time
import os
import multiprocessing
import threading
from mpms import MPMS, WorkerGracefulDie


# 定义在模块级别以便可以被pickle
class CustomError(Exception):
    pass


class TestGracefulDie:
    """测试优雅退出机制"""
    
    def test_worker_graceful_die_exception(self):
        """测试 WorkerGracefulDie 异常触发优雅退出"""
        results = []
        process_pids = set()
        
        def worker(index):
            # 记录进程PID
            pid = os.getpid()
            process_pids.add(pid)
            
            if index == 5:
                # 第5个任务触发优雅退出
                raise WorkerGracefulDie("Worker decided to die")
            elif index > 5 and pid == os.getpid():
                # 同一进程的后续任务应该不会被执行
                # 但由于多进程，其他进程可能会接收这些任务
                pass
            return f"task_{index}_pid_{pid}"
        
        def collector(meta, result):
            if isinstance(result, Exception):
                results.append(('error', meta.taskid, type(result).__name__, str(result)))
            else:
                results.append(('success', meta.taskid, result))
        
        # 使用较短的优雅退出超时时间以加快测试
        m = MPMS(
            worker, 
            collector,
            processes=2,
            threads=2,
            worker_graceful_die_timeout=2,  # 2秒超时
            worker_graceful_die_exceptions=(WorkerGracefulDie,)
        )
        
        m.start()
        
        # 提交任务
        for i in range(10):
            m.put(i)
            time.sleep(0.1)  # 稍微延迟以确保任务分布
        
        m.join()
        
        # 验证结果
        error_found = False
        for result in results:
            if result[0] == 'error' and result[2] == 'WorkerGracefulDie':
                error_found = True
                break
        
        assert error_found, "WorkerGracefulDie exception should be reported"
        assert len(results) == 10, f"All tasks should be processed, got {len(results)}"
    
    def test_custom_graceful_die_exceptions(self):
        """测试自定义优雅退出异常"""
        results = []
        
        def worker(index):
            if index == 3:
                raise MemoryError("Out of memory")
            elif index == 6:
                raise CustomError("Custom error")
            return f"task_{index}"
        
        def collector(meta, result):
            if isinstance(result, Exception):
                results.append(('error', meta.taskid, type(result).__name__))
            else:
                results.append(('success', meta.taskid, result))
        
        # 只有 MemoryError 会触发优雅退出
        m = MPMS(
            worker,
            collector,
            processes=2,
            threads=1,
            worker_graceful_die_timeout=1,
            worker_graceful_die_exceptions=(MemoryError,)  # 只有 MemoryError 触发优雅退出
        )
        
        m.start()
        
        for i in range(10):
            m.put(i)
        
        m.join()
        
        # 验证结果
        memory_error_found = False
        custom_error_found = False
        
        for result in results:
            if result[0] == 'error':
                if result[2] == 'MemoryError':
                    memory_error_found = True
                elif result[2] == 'CustomError':
                    custom_error_found = True
        
        assert memory_error_found, "MemoryError should be found"
        assert custom_error_found, "CustomError should be found"
        assert len(results) == 10, f"All tasks should be processed, got {len(results)}"
    
    def test_graceful_die_timeout(self):
        """测试优雅退出超时机制"""
        start_time = time.time()
        results = []
        process_exit_times = []
        
        def worker(index):
            if index == 0:
                # 第一个任务触发优雅退出
                raise WorkerGracefulDie("Trigger graceful die")
            # 其他任务正常执行
            return f"task_{index}"
        
        def collector(meta, result):
            results.append((meta.taskid, result, time.time()))
        
        m = MPMS(
            worker,
            collector,
            processes=1,  # 单进程以便更好地控制
            threads=1,    # 单线程以便更精确地测试
            worker_graceful_die_timeout=2,  # 2秒超时
        )
        
        m.start()
        
        # 只提交少量任务
        for i in range(3):
            m.put(i)
        
        m.join()
        
        elapsed = time.time() - start_time
        
        # 验证优雅退出超时生效
        # 由于第一个任务触发优雅退出后会等待2秒，总时间应该至少2秒
        assert elapsed >= 2, f"Graceful die timeout should wait at least 2 seconds, got {elapsed:.2f}"
        # 但不应该太长（考虑到任务执行时间和一些开销）
        assert elapsed < 4, f"Should not take too long, got {elapsed:.2f}"
    
    def test_graceful_die_with_hanging_task(self):
        """测试优雅退出时有挂起任务的情况"""
        results = []
        hang_event = threading.Event()
        
        def worker(index):
            if index == 1:
                # 这个任务会挂起
                hang_event.wait(timeout=10)  # 等待很长时间
                return "hung_task"
            elif index == 2:
                # 触发优雅退出
                raise WorkerGracefulDie("Die with hanging task")
            return f"task_{index}"
        
        def collector(meta, result):
            results.append((meta.taskid, result))
        
        m = MPMS(
            worker,
            collector,
            processes=1,
            threads=2,  # 两个线程，一个会挂起
            worker_graceful_die_timeout=2,
        )
        
        m.start()
        
        # 提交任务
        for i in range(3):
            m.put(i)
            time.sleep(0.1)
        
        # 等待一会儿让优雅退出触发
        time.sleep(3)
        
        # 释放挂起的任务
        hang_event.set()
        
        m.join()
        
        # 验证优雅退出异常被记录
        graceful_die_found = False
        for taskid, result in results:
            if isinstance(result, WorkerGracefulDie):
                graceful_die_found = True
                break
        
        assert graceful_die_found, "WorkerGracefulDie should be recorded"
    
    def test_graceful_die_process_exit(self):
        """测试优雅退出导致进程退出"""
        results = []
        process_pids = []
        
        def worker(index):
            pid = os.getpid()
            if pid not in process_pids:
                process_pids.append(pid)
            
            if index == 2:
                # 触发优雅退出
                raise WorkerGracefulDie("Process should exit")
            
            # 记录哪个进程处理了哪个任务
            return f"task_{index}_pid_{pid}"
        
        def collector(meta, result):
            if isinstance(result, Exception):
                results.append(('error', type(result).__name__, str(result)))
            else:
                results.append(('success', result))
        
        m = MPMS(
            worker,
            collector,
            processes=2,
            threads=1,
            worker_graceful_die_timeout=1,  # 1秒超时
        )
        
        m.start()
        
        # 提交任务
        for i in range(6):
            m.put(i)
            time.sleep(0.2)  # 给一些时间让任务分布到不同进程
        
        m.join()
        
        # 验证优雅退出异常被记录
        graceful_die_found = False
        for result in results:
            if result[0] == 'error' and result[1] == 'WorkerGracefulDie':
                graceful_die_found = True
                break
        
        assert graceful_die_found, "WorkerGracefulDie should be recorded"
        # 所有任务都应该被处理（可能由其他进程处理）
        assert len(results) == 6, f"All tasks should be processed, got {len(results)}"


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s']) 