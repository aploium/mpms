#!/usr/bin/env python3
# coding=utf-8
"""
测试MPMS zombie进程修复
"""

import pytest
import time
import os
import multiprocessing
import threading
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from mpms import MPMS


class TestZombieFix:
    """测试zombie进程修复"""
    
    def test_process_crash_recovery(self):
        """测试进程崩溃后的恢复和zombie清理"""
        results = []
        
        def crash_worker(task_id):
            """会崩溃的worker"""
            if task_id == 0:
                # 第一个任务导致进程崩溃
                os._exit(1)
            time.sleep(0.1)
            return f"Task {task_id} done"
        
        def collector(meta, result):
            results.append(result)
        
        # 创建MPMS实例
        m = MPMS(
            worker=crash_worker,
            collector=collector,
            processes=2,
            threads=1,
            subproc_check_interval=1,  # 1秒检查一次
        )
        
        m.start()
        
        # 提交会导致崩溃的任务
        m.put(0)
        
        # 提交正常任务
        for i in range(1, 5):
            m.put(i)
        
        # 等待任务处理和进程恢复
        time.sleep(5)  # 增加等待时间
        
        # 手动触发检查确保进程恢复
        m._subproc_check()
        time.sleep(1)
        
        # 检查进程池状态
        alive_count = sum(1 for p in m.worker_processes_pool.values() if p.is_alive())
        assert alive_count >= 1, f"Expected at least 1 alive process, got {alive_count}"
        
        # 关闭并等待
        m.close()
        m.join()
        
        # 验证正常任务都完成了
        assert len(results) >= 4, f"Expected at least 4 results, got {len(results)}"
    
    def test_join_called_on_dead_process(self):
        """测试死亡进程是否调用了join"""
        join_called = threading.Event()
        original_join = multiprocessing.Process.join
        
        def mock_join(self, timeout=None):
            """Mock join方法"""
            if not self.is_alive():
                join_called.set()
            return original_join(self, timeout)
        
        # 临时替换join方法
        multiprocessing.Process.join = mock_join
        
        try:
            def crash_worker(x):
                os._exit(1)
            
            m = MPMS(
                worker=crash_worker,
                processes=1,
                threads=1,
                subproc_check_interval=0.5,
            )
            
            m.start()
            m.put(1)
            
            # 等待进程崩溃和检查
            time.sleep(2)
            
            # 验证join被调用了
            assert join_called.is_set(), "join() was not called on dead process"
            
            m.close()
            m.join()
            
        finally:
            # 恢复原始方法
            multiprocessing.Process.join = original_join
    
    def test_process_restart_maintains_count(self):
        """测试进程重启后维持配置的进程数"""
        def sometimes_crash_worker(task_id):
            if task_id % 10 == 0:
                os._exit(1)
            time.sleep(0.05)
            return task_id
        
        m = MPMS(
            worker=sometimes_crash_worker,
            processes=4,
            threads=2,
            subproc_check_interval=1,
        )
        
        m.start()
        
        # 初始检查
        assert len(m.worker_processes_pool) == 4
        
        # 提交一些会导致崩溃的任务
        for i in range(30):
            m.put(i)
        
        # 等待一些进程崩溃和恢复
        time.sleep(3)
        
        # 检查进程数是否维持
        assert len(m.worker_processes_pool) == 4, f"Expected 4 processes, got {len(m.worker_processes_pool)}"
        
        # 检查所有进程都是活的
        alive_count = sum(1 for p in m.worker_processes_pool.values() if p.is_alive())
        assert alive_count == 4, f"Expected 4 alive processes, got {alive_count}"
        
        m.close()
        m.join()
    
    def test_graceful_shutdown(self):
        """测试优雅关闭功能"""
        task_count = 0
        
        def slow_worker(x):
            nonlocal task_count
            time.sleep(0.1)
            task_count += 1
            return x
        
        m = MPMS(
            worker=slow_worker,
            processes=2,
            threads=2,
        )
        
        m.start()
        
        # 提交任务
        for i in range(20):
            m.put(i)
        
        # 优雅关闭
        start_time = time.time()
        success = m.graceful_shutdown(timeout=5.0)
        elapsed = time.time() - start_time
        
        assert success, "Graceful shutdown failed"
        assert elapsed < 5.0, f"Graceful shutdown took too long: {elapsed}s"
        assert task_count == 20, f"Not all tasks completed: {task_count}/20"
        
        # 验证所有进程都被清理了
        assert len(m.worker_processes_pool) == 0
    
    def test_collector_handles_timeout_tasks(self):
        """测试collector正确处理超时任务"""
        results = []
        errors = []
        
        def hang_worker(x):
            if x == 0:
                time.sleep(10)  # 会超时的任务
            return x
        
        def collector(meta, result):
            if isinstance(result, Exception):
                errors.append(result)
            else:
                results.append(result)
        
        m = MPMS(
            worker=hang_worker,
            collector=collector,
            processes=1,
            threads=1,
            lifecycle_duration_hard=1,  # 1秒超时
        )
        
        m.start()
        
        # 提交会超时的任务
        m.put(0)
        # 提交正常任务
        m.put(1)
        
        # 等待超时
        time.sleep(2)
        
        # 手动触发检查
        m._subproc_check()
        
        # 等待collector处理
        time.sleep(0.5)
        
        # 验证超时任务被正确处理
        assert len(errors) >= 1, "Timeout error not reported"
        assert any(isinstance(e, TimeoutError) for e in errors), "No TimeoutError found"
        
        m.close()
        m.join()
    
    def test_close_wait_for_empty(self):
        """测试wait_for_empty参数"""
        processed = []
        
        def worker(x):
            time.sleep(0.1)
            processed.append(x)
            return x
        
        m = MPMS(worker=worker, processes=2, threads=2)
        m.start()
        
        # 提交任务
        for i in range(10):
            m.put(i)
        
        # 立即关闭，等待队列清空
        start_time = time.time()
        m.close(wait_for_empty=True)
        elapsed = time.time() - start_time
        
        # 验证等待了一段时间
        assert elapsed >= 0.1, f"Did not wait for queue to empty: {elapsed}s"
        
        m.join()
        
        # 验证所有任务都被处理了
        assert len(processed) == 10, f"Not all tasks processed: {len(processed)}/10"


if __name__ == "__main__":
    """直接运行测试"""
    test = TestZombieFix()
    
    print("运行测试: test_process_crash_recovery")
    test.test_process_crash_recovery()
    print("✅ 通过")
    
    print("\n运行测试: test_join_called_on_dead_process")
    test.test_join_called_on_dead_process()
    print("✅ 通过")
    
    print("\n运行测试: test_process_restart_maintains_count")
    test.test_process_restart_maintains_count()
    print("✅ 通过")
    
    print("\n运行测试: test_graceful_shutdown")
    test.test_graceful_shutdown()
    print("✅ 通过")
    
    print("\n运行测试: test_collector_handles_timeout_tasks")
    test.test_collector_handles_timeout_tasks()
    print("✅ 通过")
    
    print("\n运行测试: test_close_wait_for_empty")
    test.test_close_wait_for_empty()
    print("✅ 通过")
    
    print("\n🎉 所有测试通过！") 