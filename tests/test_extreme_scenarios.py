#!/usr/bin/env python3
# coding=utf-8
"""
MPMS 极端场景测试
模拟各种恶劣的生产环境情况
"""

import pytest
import time
import threading
import multiprocessing
import random
import os
import signal
import gc
import sys
import logging
import tempfile
import subprocess
import psutil
from typing import List, Dict, Any
import queue

# 导入被测试的模块
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from mpms import MPMS, WorkerGracefulDie, Meta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ExtremeTestCollector:
    """极端测试收集器"""
    
    def __init__(self):
        self.results = []
        self.exceptions = []
        self.lock = threading.Lock()
        self.process_deaths = []
        self.timeline = []  # 记录事件时间线
        
    def collect(self, meta: Meta, result: Any):
        current_time = time.time()
        with self.lock:
            task_id = meta.args[0] if meta.args else 'unknown'
            
            if isinstance(result, Exception):
                self.exceptions.append({
                    'task_id': task_id,
                    'exception': result,
                    'exception_type': type(result).__name__,
                    'timestamp': current_time
                })
                self.timeline.append(f"{current_time:.2f}: 任务 {task_id} 异常: {type(result).__name__}")
            else:
                self.results.append(result)
                self.timeline.append(f"{current_time:.2f}: 任务 {task_id} 完成")
    
    def get_stats(self) -> Dict[str, Any]:
        with self.lock:
            return {
                'total_tasks': len(self.results) + len(self.exceptions),
                'successful_tasks': len(self.results),
                'failed_tasks': len(self.exceptions),
                'success_rate': len(self.results) / max(1, len(self.results) + len(self.exceptions)),
                'exception_types': list(set(e['exception_type'] for e in self.exceptions)),
                'timeline_events': len(self.timeline)
            }


def crash_prone_worker(task_id: int, crash_rate: float = 0.05) -> Dict[str, Any]:
    """容易崩溃的worker"""
    if random.random() < crash_rate:
        # 模拟segfault (在Python中用exit模拟)
        logger.warning(f"任务 {task_id} 触发进程崩溃")
        os._exit(1)  # 强制退出，模拟崩溃
    
    # 正常工作
    time.sleep(random.uniform(0.01, 0.1))
    return {
        'task_id': task_id,
        'pid': os.getpid(),
        'status': 'success'
    }


def memory_leak_worker(task_id: int, leak_size: int = 1024*1024) -> Dict[str, Any]:
    """内存泄漏worker"""
    # 模拟内存泄漏 - 创建大对象但不释放
    global _leaked_memory
    if '_leaked_memory' not in globals():
        _leaked_memory = []
    
    # 每次分配一些内存并"泄漏"
    data = bytearray(leak_size)
    _leaked_memory.append(data)
    
    # 定期清理，避免测试机器崩溃
    if len(_leaked_memory) > 50:  # 保持50MB左右的泄漏
        _leaked_memory.pop(0)
    
    return {
        'task_id': task_id,
        'leaked_size': leak_size,
        'total_leaked': len(_leaked_memory) * leak_size
    }


def resource_exhaustion_worker(task_id: int) -> Dict[str, Any]:
    """资源耗尽worker"""
    temp_files = []
    try:
        # 创建大量临时文件
        for i in range(100):
            fd, path = tempfile.mkstemp()
            temp_files.append((fd, path))
            os.write(fd, b'x' * 1024)  # 写入1KB数据
        
        # 模拟一些工作
        time.sleep(0.05)
        
        return {
            'task_id': task_id,
            'files_created': len(temp_files),
            'status': 'success'
        }
    except Exception as e:
        return {
            'task_id': task_id,
            'error': str(e),
            'status': 'failed'
        }
    finally:
        # 清理文件
        for fd, path in temp_files:
            try:
                os.close(fd)
                os.unlink(path)
            except:
                pass


def signal_sensitive_worker(task_id: int, duration: float = 0.5) -> Dict[str, Any]:
    """对信号敏感的worker"""
    start_time = time.time()
    
    # 安装信号处理器
    original_sigterm = signal.signal(signal.SIGTERM, signal.SIG_DFL)
    original_sigint = signal.signal(signal.SIGINT, signal.SIG_DFL)
    
    try:
        # 执行工作，期间可能收到信号
        while time.time() - start_time < duration:
            time.sleep(0.01)
        
        return {
            'task_id': task_id,
            'duration': time.time() - start_time,
            'status': 'completed'
        }
    except KeyboardInterrupt:
        return {
            'task_id': task_id,
            'status': 'interrupted',
            'duration': time.time() - start_time
        }
    finally:
        # 恢复信号处理器
        signal.signal(signal.SIGTERM, original_sigterm)
        signal.signal(signal.SIGINT, original_sigint)


def deadlock_prone_worker(task_id: int, shared_locks: List[threading.Lock]) -> Dict[str, Any]:
    """容易死锁的worker"""
    if len(shared_locks) < 2:
        return {'task_id': task_id, 'status': 'no_locks'}
    
    # 随机选择两个锁，可能导致死锁
    lock1, lock2 = random.sample(shared_locks, 2)
    
    # 随机决定获取锁的顺序，可能导致死锁
    if random.random() < 0.5:
        first_lock, second_lock = lock1, lock2
    else:
        first_lock, second_lock = lock2, lock1
    
    try:
        acquired_first = first_lock.acquire(timeout=0.1)
        if not acquired_first:
            return {'task_id': task_id, 'status': 'lock1_timeout'}
        
        time.sleep(random.uniform(0.01, 0.05))  # 持有第一个锁一段时间
        
        acquired_second = second_lock.acquire(timeout=0.1)
        if not acquired_second:
            first_lock.release()
            return {'task_id': task_id, 'status': 'lock2_timeout'}
        
        # 模拟工作
        time.sleep(0.01)
        
        second_lock.release()
        first_lock.release()
        
        return {
            'task_id': task_id,
            'status': 'success',
            'locks_used': [id(first_lock), id(second_lock)]
        }
    
    except Exception as e:
        # 确保释放锁
        try:
            first_lock.release()
        except:
            pass
        try:
            second_lock.release()
        except:
            pass
        return {
            'task_id': task_id,
            'status': 'error',
            'error': str(e)
        }


def unstable_initializer():
    """不稳定的初始化函数"""
    if random.random() < 0.3:  # 30% 失败率
        raise RuntimeError(f"进程 {os.getpid()} 初始化失败")
    
    # 模拟初始化工作
    time.sleep(random.uniform(0.1, 0.5))
    logger.info(f"进程 {os.getpid()} 初始化成功")
    
    # 创建一些全局状态
    global _process_state
    _process_state = {
        'pid': os.getpid(),
        'start_time': time.time(),
        'task_count': 0
    }


def unstable_finalizer():
    """不稳定的清理函数"""
    global _process_state
    if '_process_state' in globals():
        logger.info(f"进程 {_process_state['pid']} 处理了 {_process_state.get('task_count', 0)} 个任务")
    
    if random.random() < 0.2:  # 20% 失败率
        raise RuntimeError(f"进程 {os.getpid()} 清理失败")
    
    logger.info(f"进程 {os.getpid()} 清理成功")


class TestMPMSExtremeScenarios:
    """MPMS极端场景测试类"""
    
    def test_process_crash_recovery(self):
        """测试进程崩溃恢复"""
        logger.info("开始进程崩溃恢复测试")
        collector = ExtremeTestCollector()
        
        mpms = MPMS(
            worker=crash_prone_worker,
            collector=collector.collect,
            processes=4,
            threads=2,
            subproc_check_interval=1.0,  # 快速检查
            lifecycle_duration_hard=30.0
        )
        
        mpms.start()
        
        # 提交任务，预期有一些会导致进程崩溃
        task_count = 100
        for i in range(task_count):
            mpms.put(i, 0.1)  # 10% 崩溃率
            time.sleep(0.02)  # 控制提交速度
        
        mpms.join()
        
        stats = collector.get_stats()
        logger.info(f"进程崩溃恢复测试结果: {stats}")
        
        # 验证系统能够从崩溃中恢复
        assert stats['total_tasks'] > 0
        assert stats['success_rate'] > 0.5  # 至少50%成功率
    
    def test_memory_pressure_survival(self):
        """测试内存压力下的生存能力"""
        logger.info("开始内存压力生存测试")
        collector = ExtremeTestCollector()
        
        # 获取当前内存使用情况
        initial_memory = psutil.virtual_memory().percent
        logger.info(f"初始内存使用率: {initial_memory:.1f}%")
        
        mpms = MPMS(
            worker=memory_leak_worker,
            collector=collector.collect,
            processes=2,
            threads=2,
            lifecycle=10,  # 频繁重启进程以控制内存泄漏
            lifecycle_duration_hard=60.0,
            subproc_check_interval=2.0
        )
        
        mpms.start()
        
        # 提交内存密集型任务
        task_count = 50
        for i in range(task_count):
            mpms.put(i, 512*1024)  # 每个任务泄漏512KB
            time.sleep(0.1)
        
        mpms.join()
        
        final_memory = psutil.virtual_memory().percent
        memory_increase = final_memory - initial_memory
        
        stats = collector.get_stats()
        logger.info(f"内存压力测试结果: {stats}")
        logger.info(f"内存使用变化: {initial_memory:.1f}% -> {final_memory:.1f}% (增长: {memory_increase:.1f}%)")
        
        # 验证系统能够处理内存压力
        assert stats['total_tasks'] > 0
        assert memory_increase < 20  # 内存增长不超过20%
    
    def test_resource_exhaustion_resilience(self):
        """测试资源耗尽场景下的韧性"""
        logger.info("开始资源耗尽韧性测试")
        collector = ExtremeTestCollector()
        
        mpms = MPMS(
            worker=resource_exhaustion_worker,
            collector=collector.collect,
            processes=3,
            threads=2,
            lifecycle_duration_hard=45.0
        )
        
        mpms.start()
        
        # 提交资源密集型任务
        task_count = 30
        for i in range(task_count):
            mpms.put(i)
            time.sleep(0.1)
        
        mpms.join()
        
        stats = collector.get_stats()
        logger.info(f"资源耗尽测试结果: {stats}")
        
        # 验证系统能够处理资源压力
        assert stats['total_tasks'] > 0
        assert stats['success_rate'] > 0.7  # 至少70%成功率
    
    def test_signal_interruption_handling(self):
        """测试信号中断处理"""
        logger.info("开始信号中断处理测试")
        collector = ExtremeTestCollector()
        
        mpms = MPMS(
            worker=signal_sensitive_worker,
            collector=collector.collect,
            processes=2,
            threads=2,
            lifecycle_duration_hard=30.0
        )
        
        mpms.start()
        
        # 在后台发送信号的函数
        def send_signals():
            time.sleep(2)  # 等待任务开始
            for _ in range(3):
                try:
                    # 向worker进程发送信号
                    for name, process in mpms.worker_processes_pool.items():
                        if process.is_alive():
                            os.kill(process.pid, signal.SIGUSR1)
                            time.sleep(0.5)
                except:
                    pass
        
        # 启动信号发送线程
        signal_thread = threading.Thread(target=send_signals)
        signal_thread.daemon = True
        signal_thread.start()
        
        # 提交任务
        task_count = 20
        for i in range(task_count):
            mpms.put(i, 0.3)  # 300ms任务
            time.sleep(0.1)
        
        mpms.join()
        
        stats = collector.get_stats()
        logger.info(f"信号中断测试结果: {stats}")
        
        # 验证系统能够处理信号中断
        assert stats['total_tasks'] > 0
        assert stats['success_rate'] > 0.6  # 至少60%成功率
    
    def test_initialization_failure_recovery(self):
        """测试初始化失败恢复"""
        logger.info("开始初始化失败恢复测试")
        collector = ExtremeTestCollector()
        
        def simple_worker(task_id: int) -> Dict[str, Any]:
            global _process_state
            if '_process_state' in globals():
                _process_state['task_count'] += 1
            
            return {
                'task_id': task_id,
                'pid': os.getpid(),
                'status': 'success'
            }
        
        mpms = MPMS(
            worker=simple_worker,
            collector=collector.collect,
            processes=4,
            threads=2,
            process_initializer=unstable_initializer,
            process_finalizer=unstable_finalizer,
            lifecycle_duration_hard=45.0,
            subproc_check_interval=1.0
        )
        
        mpms.start()
        
        # 提交任务
        task_count = 80
        for i in range(task_count):
            mpms.put(i)
            time.sleep(0.05)
        
        mpms.join()
        
        stats = collector.get_stats()
        logger.info(f"初始化失败恢复测试结果: {stats}")
        
        # 验证系统能够从初始化失败中恢复
        assert stats['total_tasks'] > 0
        assert stats['success_rate'] > 0.6  # 考虑到初始化失败
    
    def test_mixed_extreme_conditions(self):
        """混合极端条件测试"""
        logger.info("开始混合极端条件测试")
        collector = ExtremeTestCollector()
        
        def extreme_chaos_worker(task_type: str, task_id: int) -> Dict[str, Any]:
            """极端混沌worker"""
            workers = {
                'crash': lambda: crash_prone_worker(task_id, 0.05),
                'memory': lambda: memory_leak_worker(task_id, 256*1024),
                'resource': lambda: resource_exhaustion_worker(task_id),
                'signal': lambda: signal_sensitive_worker(task_id, 0.2),
            }
            
            return workers[task_type]()
        
        mpms = MPMS(
            worker=extreme_chaos_worker,
            collector=collector.collect,
            processes=4,
            threads=3,
            process_initializer=unstable_initializer,
            process_finalizer=unstable_finalizer,
            lifecycle=15,  # 频繁重启
            lifecycle_duration=10.0,
            lifecycle_duration_hard=30.0,
            worker_graceful_die_timeout=2.0,
            subproc_check_interval=1.0
        )
        
        mpms.start()
        
        # 随机提交不同类型的极端任务
        task_types = ['crash', 'memory', 'resource', 'signal']
        task_count = 100
        
        for i in range(task_count):
            task_type = random.choice(task_types)
            mpms.put(task_type, i)
            if i % 20 == 0:
                time.sleep(0.5)  # 偶尔暂停让系统恢复
            else:
                time.sleep(0.05)
        
        mpms.join()
        
        stats = collector.get_stats()
        logger.info(f"混合极端条件测试结果: {stats}")
        
        # 验证系统在极端混合条件下的韧性
        assert stats['total_tasks'] > 0
        assert stats['success_rate'] > 0.3  # 极端条件下，30%成功率可接受
        assert len(stats['exception_types']) > 0  # 应该遇到各种异常
    
    def test_graceful_die_under_pressure(self):
        """测试压力下的优雅退出"""
        logger.info("开始压力下优雅退出测试")
        collector = ExtremeTestCollector()
        
        def pressure_graceful_worker(task_id: int) -> Dict[str, Any]:
            # 创建一些负载
            data = bytearray(1024*1024)  # 1MB
            
            # 随机触发优雅退出
            if random.random() < 0.1:
                raise WorkerGracefulDie(f"压力下优雅退出 from task {task_id}")
            
            # 模拟工作
            time.sleep(random.uniform(0.05, 0.2))
            
            return {
                'task_id': task_id,
                'data_size': len(data),
                'status': 'success'
            }
        
        mpms = MPMS(
            worker=pressure_graceful_worker,
            collector=collector.collect,
            processes=4,
            threads=3,
            worker_graceful_die_timeout=3.0,
            worker_graceful_die_exceptions=(WorkerGracefulDie,),
            lifecycle_duration_hard=45.0,
            subproc_check_interval=1.0
        )
        
        mpms.start()
        
        # 快速提交大量任务
        task_count = 200
        for i in range(task_count):
            mpms.put(i)
            if i % 50 == 0:
                time.sleep(0.1)  # 偶尔暂停
        
        mpms.join()
        
        stats = collector.get_stats()
        logger.info(f"压力下优雅退出测试结果: {stats}")
        
        # 检查优雅退出是否正常工作
        graceful_die_count = sum(1 for e in collector.exceptions 
                               if isinstance(e['exception'], WorkerGracefulDie))
        
        logger.info(f"检测到 {graceful_die_count} 次优雅退出")
        
        # 验证系统能够处理压力下的优雅退出
        assert stats['total_tasks'] > 0
        assert graceful_die_count > 0  # 应该有优雅退出
        assert stats['success_rate'] > 0.7  # 至少70%成功率
    
    def test_rapid_configuration_changes(self):
        """测试快速配置变化的适应性"""
        logger.info("开始快速配置变化适应性测试")
        
        # 创建多个不同配置的MPMS实例，快速切换
        configurations = [
            {'processes': 1, 'threads': 2},
            {'processes': 2, 'threads': 1},
            {'processes': 3, 'threads': 3},
            {'processes': 1, 'threads': 4},
        ]
        
        overall_stats = {
            'total_instances': 0,
            'total_tasks': 0,
            'successful_tasks': 0
        }
        
        for i, config in enumerate(configurations):
            logger.info(f"测试配置 {i+1}: {config}")
            collector = ExtremeTestCollector()
            
            def config_worker(task_id: int) -> Dict[str, Any]:
                # 模拟一些工作
                time.sleep(random.uniform(0.01, 0.05))
                return {
                    'task_id': task_id,
                    'config': config,
                    'pid': os.getpid()
                }
            
            mpms = MPMS(
                worker=config_worker,
                collector=collector.collect,
                **config,
                lifecycle_duration_hard=20.0
            )
            
            mpms.start()
            
            # 快速提交少量任务
            for j in range(20):
                mpms.put(j + i * 20)
            
            mpms.join()
            
            stats = collector.get_stats()
            overall_stats['total_instances'] += 1
            overall_stats['total_tasks'] += stats['total_tasks']
            overall_stats['successful_tasks'] += stats['successful_tasks']
            
            logger.info(f"配置 {i+1} 结果: {stats['successful_tasks']}/{stats['total_tasks']} 成功")
        
        logger.info(f"快速配置变化测试总结: {overall_stats}")
        
        # 验证所有配置都能正常工作
        assert overall_stats['total_instances'] == len(configurations)
        assert overall_stats['successful_tasks'] > overall_stats['total_tasks'] * 0.8


if __name__ == "__main__":
    # 可以直接运行这个文件进行极端场景测试
    import sys
    pytest.main([__file__, "-v", "-s"] + sys.argv[1:]) 