#!/usr/bin/env python3
# coding=utf-8
"""
MPMS 性能基准测试
测量在不同负载下的性能表现
"""

import pytest
import time
import threading
import multiprocessing
import os
import sys
import logging
import statistics
from typing import List, Dict, Any, Tuple
import psutil

# 导入被测试的模块
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from mpms import MPMS, Meta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PerformanceCollector:
    """性能测试专用收集器"""
    
    def __init__(self):
        self.results = []
        self.start_times = {}
        self.end_times = {}
        self.lock = threading.Lock()
        self.task_latencies = []
        self.throughput_samples = []
        self.last_sample_time = time.time()
        self.last_sample_count = 0
        
    def collect(self, meta: Meta, result: Any):
        current_time = time.time()
        with self.lock:
            task_id = meta.args[0] if meta.args else 'unknown'
            
            # 记录任务延迟（从提交到完成的时间）
            if len(meta.args) >= 2 and isinstance(meta.args[1], float):
                submit_time = meta.args[1]
                latency = current_time - submit_time
                self.task_latencies.append(latency)
            
            # 记录吞吐量样本
            if current_time - self.last_sample_time >= 1.0:  # 每秒采样一次
                current_count = len(self.results) + 1
                throughput = (current_count - self.last_sample_count) / (current_time - self.last_sample_time)
                self.throughput_samples.append(throughput)
                self.last_sample_time = current_time
                self.last_sample_count = current_count
            
            if isinstance(result, Exception):
                logger.warning(f"任务 {task_id} 失败: {result}")
            else:
                self.results.append(result)
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """获取性能统计信息"""
        with self.lock:
            total_tasks = len(self.results)
            
            if not self.task_latencies:
                return {'error': 'No latency data available'}
            
            # 延迟统计
            latency_stats = {
                'min': min(self.task_latencies),
                'max': max(self.task_latencies),
                'mean': statistics.mean(self.task_latencies),
                'median': statistics.median(self.task_latencies),
                'p95': self._percentile(self.task_latencies, 95),
                'p99': self._percentile(self.task_latencies, 99),
                'stddev': statistics.stdev(self.task_latencies) if len(self.task_latencies) > 1 else 0
            }
            
            # 吞吐量统计
            throughput_stats = {}
            if self.throughput_samples:
                throughput_stats = {
                    'min': min(self.throughput_samples),
                    'max': max(self.throughput_samples),
                    'mean': statistics.mean(self.throughput_samples),
                    'median': statistics.median(self.throughput_samples)
                }
            
            return {
                'total_tasks': total_tasks,
                'latency_ms': {k: v * 1000 for k, v in latency_stats.items()},  # 转换为毫秒
                'throughput_tps': throughput_stats,  # tasks per second
                'latency_samples': len(self.task_latencies),
                'throughput_samples': len(self.throughput_samples)
            }
    
    @staticmethod
    def _percentile(data: List[float], percentile: int) -> float:
        """计算百分位数"""
        sorted_data = sorted(data)
        k = (len(sorted_data) - 1) * percentile / 100
        f = int(k)
        c = k - f
        if f == len(sorted_data) - 1:
            return sorted_data[f]
        return sorted_data[f] * (1 - c) + sorted_data[f + 1] * c


class SystemMonitor:
    """系统资源监控器"""
    
    def __init__(self):
        self.cpu_samples = []
        self.memory_samples = []
        self.monitoring = False
        self.monitor_thread = None
        
    def start_monitoring(self):
        """开始监控"""
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        
    def stop_monitoring(self):
        """停止监控"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=1)
    
    def _monitor_loop(self):
        """监控循环"""
        while self.monitoring:
            try:
                # CPU使用率
                cpu_percent = psutil.cpu_percent()
                self.cpu_samples.append(cpu_percent)
                
                # 内存使用率
                memory = psutil.virtual_memory()
                self.memory_samples.append({
                    'percent': memory.percent,
                    'available_gb': memory.available / (1024**3),
                    'used_gb': memory.used / (1024**3)
                })
                
                time.sleep(0.5)  # 每0.5秒采样一次
            except Exception as e:
                logger.warning(f"监控采样失败: {e}")
                
    def get_stats(self) -> Dict[str, Any]:
        """获取监控统计"""
        if not self.cpu_samples:
            return {'error': 'No monitoring data'}
            
        return {
            'cpu_percent': {
                'min': min(self.cpu_samples),
                'max': max(self.cpu_samples),
                'mean': statistics.mean(self.cpu_samples),
                'samples': len(self.cpu_samples)
            },
            'memory_percent': {
                'min': min(s['percent'] for s in self.memory_samples),
                'max': max(s['percent'] for s in self.memory_samples),
                'mean': statistics.mean(s['percent'] for s in self.memory_samples),
                'samples': len(self.memory_samples)
            },
            'memory_peak_used_gb': max(s['used_gb'] for s in self.memory_samples)
        }


def lightweight_worker(task_id: int, submit_time: float) -> Dict[str, Any]:
    """轻量级工作任务"""
    # 模拟极轻量的计算
    result = sum(range(100))
    return {
        'task_id': task_id,
        'submit_time': submit_time,
        'complete_time': time.time(),
        'result': result
    }


def medium_worker(task_id: int, submit_time: float, work_duration: float = 0.01) -> Dict[str, Any]:
    """中等负载工作任务"""
    start_time = time.time()
    result = 0
    while time.time() - start_time < work_duration:
        result += sum(range(1000))
    
    return {
        'task_id': task_id,
        'submit_time': submit_time,
        'complete_time': time.time(),
        'result': result,
        'actual_duration': time.time() - start_time
    }


def heavy_worker(task_id: int, submit_time: float, work_duration: float = 0.1) -> Dict[str, Any]:
    """重负载工作任务"""
    start_time = time.time()
    result = 0
    while time.time() - start_time < work_duration:
        result += sum(range(10000))
    
    return {
        'task_id': task_id,
        'submit_time': submit_time,
        'complete_time': time.time(),
        'result': result,
        'actual_duration': time.time() - start_time
    }


class TestMPMSPerformance:
    """MPMS性能测试类"""
    
    def test_baseline_performance(self):
        """基线性能测试"""
        logger.info("开始基线性能测试")
        
        collector = PerformanceCollector()
        monitor = SystemMonitor()
        
        mpms = MPMS(
            worker=lightweight_worker,
            collector=collector.collect,
            processes=2,
            threads=2
        )
        
        monitor.start_monitoring()
        mpms.start()
        
        # 提交1000个轻量级任务
        task_count = 1000
        start_time = time.time()
        
        for i in range(task_count):
            mpms.put(i, time.time())
        
        mpms.join()
        end_time = time.time()
        monitor.stop_monitoring()
        
        # 计算总体性能指标
        total_duration = end_time - start_time
        overall_throughput = task_count / total_duration
        
        performance_stats = collector.get_performance_stats()
        system_stats = monitor.get_stats()
        
        logger.info(f"基线性能测试结果:")
        logger.info(f"  总任务数: {task_count}")
        logger.info(f"  总耗时: {total_duration:.2f}秒")
        logger.info(f"  整体吞吐量: {overall_throughput:.2f} tasks/sec")
        logger.info(f"  延迟统计(ms): {performance_stats.get('latency_ms', {})}")
        logger.info(f"  系统负载: {system_stats}")
        
        # 验证基线性能
        assert performance_stats['total_tasks'] == task_count
        assert overall_throughput > 50  # 至少50 tasks/sec
        assert performance_stats['latency_ms']['mean'] < 1000  # 平均延迟小于1秒
    
    def test_scaling_performance(self):
        """扩展性能测试 - 测试不同进程/线程配置的性能"""
        logger.info("开始扩展性能测试")
        
        configurations = [
            (1, 1),   # 1进程1线程
            (1, 4),   # 1进程4线程
            (2, 2),   # 2进程2线程
            (4, 2),   # 4进程2线程
            (2, 4),   # 2进程4线程
        ]
        
        results = {}
        task_count = 500
        
        for processes, threads in configurations:
            logger.info(f"测试配置: {processes}进程 x {threads}线程")
            
            collector = PerformanceCollector()
            monitor = SystemMonitor()
            
            mpms = MPMS(
                worker=medium_worker,
                collector=collector.collect,
                processes=processes,
                threads=threads
            )
            
            monitor.start_monitoring()
            start_time = time.time()
            mpms.start()
            
            # 提交任务
            for i in range(task_count):
                mpms.put(i, time.time(), 0.01)  # 10ms工作负载
            
            mpms.join()
            end_time = time.time()
            monitor.stop_monitoring()
            
            total_duration = end_time - start_time
            throughput = task_count / total_duration
            
            performance_stats = collector.get_performance_stats()
            system_stats = monitor.get_stats()
            
            results[f"{processes}p{threads}t"] = {
                'throughput': throughput,
                'duration': total_duration,
                'latency_mean': performance_stats.get('latency_ms', {}).get('mean', 0),
                'cpu_peak': system_stats.get('cpu_percent', {}).get('max', 0)
            }
            
            logger.info(f"  结果: {throughput:.2f} tasks/sec, 延迟: {performance_stats.get('latency_ms', {}).get('mean', 0):.2f}ms")
        
        # 分析扩展性
        logger.info("扩展性能测试结果汇总:")
        for config, stats in results.items():
            logger.info(f"  {config}: {stats['throughput']:.2f} tasks/sec, {stats['latency_mean']:.2f}ms, CPU峰值: {stats['cpu_peak']:.1f}%")
        
        # 验证扩展性
        single_thread_throughput = results['1p1t']['throughput']
        multi_config_throughput = results['2p2t']['throughput']
        
        # 多核配置应该有性能提升
        assert multi_config_throughput > single_thread_throughput * 1.5
    
    def test_load_capacity(self):
        """负载容量测试 - 测试系统在高负载下的表现"""
        logger.info("开始负载容量测试")
        
        load_levels = [
            (100, 0.005),   # 轻负载：100任务，5ms each
            (500, 0.01),    # 中负载：500任务，10ms each  
            (1000, 0.02),   # 重负载：1000任务，20ms each
            (2000, 0.05),   # 超重负载：2000任务，50ms each
        ]
        
        for task_count, work_duration in load_levels:
            logger.info(f"测试负载: {task_count}任务，每任务{work_duration*1000:.0f}ms")
            
            collector = PerformanceCollector()
            monitor = SystemMonitor()
            
            mpms = MPMS(
                worker=medium_worker,
                collector=collector.collect,
                processes=4,
                threads=3,
                lifecycle_duration_hard=120.0  # 2分钟硬超时
            )
            
            monitor.start_monitoring()
            start_time = time.time()
            mpms.start()
            
            # 提交任务
            for i in range(task_count):
                mpms.put(i, time.time(), work_duration)
                if i % 100 == 0 and i > 0:
                    time.sleep(0.01)  # 稍微控制提交速度
            
            mpms.join()
            end_time = time.time()
            monitor.stop_monitoring()
            
            total_duration = end_time - start_time
            throughput = task_count / total_duration
            
            performance_stats = collector.get_performance_stats()
            system_stats = monitor.get_stats()
            
            logger.info(f"  结果: {throughput:.2f} tasks/sec")
            logger.info(f"  延迟: 平均{performance_stats.get('latency_ms', {}).get('mean', 0):.2f}ms, P95: {performance_stats.get('latency_ms', {}).get('p95', 0):.2f}ms")
            logger.info(f"  系统: CPU峰值{system_stats.get('cpu_percent', {}).get('max', 0):.1f}%, 内存峰值{system_stats.get('memory_peak_used_gb', 0):.2f}GB")
            
            # 验证系统在负载下仍能正常工作
            assert performance_stats['total_tasks'] > task_count * 0.9  # 至少90%任务完成
            assert performance_stats.get('latency_ms', {}).get('mean', 0) < 5000  # 平均延迟小于5秒
    
    def test_sustained_load(self):
        """持续负载测试 - 测试长时间运行下的性能稳定性"""
        logger.info("开始持续负载测试")
        
        collector = PerformanceCollector()
        monitor = SystemMonitor()
        
        mpms = MPMS(
            worker=medium_worker,
            collector=collector.collect,
            processes=3,
            threads=2,
            lifecycle=50,  # 每50个任务重启进程，测试进程重启的影响
            lifecycle_duration_hard=300.0  # 5分钟硬超时
        )
        
        monitor.start_monitoring()
        mpms.start()
        
        # 持续提交任务60秒
        start_time = time.time()
        test_duration = 60  # 60秒测试
        task_id = 0
        
        while time.time() - start_time < test_duration:
            # 以恒定速率提交任务
            batch_start = time.time()
            for _ in range(10):  # 每批10个任务
                mpms.put(task_id, time.time(), 0.02)  # 20ms工作负载
                task_id += 1
            
            # 控制提交速率（约50 tasks/sec）
            batch_duration = time.time() - batch_start
            sleep_time = max(0, 0.2 - batch_duration)  # 每批200ms
            if sleep_time > 0:
                time.sleep(sleep_time)
        
        mpms.join()
        end_time = time.time()
        monitor.stop_monitoring()
        
        total_duration = end_time - start_time
        throughput = task_id / total_duration
        
        performance_stats = collector.get_performance_stats()
        system_stats = monitor.get_stats()
        
        logger.info(f"持续负载测试结果:")
        logger.info(f"  测试时长: {total_duration:.2f}秒")
        logger.info(f"  提交任务数: {task_id}")
        logger.info(f"  完成任务数: {performance_stats['total_tasks']}")
        logger.info(f"  平均吞吐量: {throughput:.2f} tasks/sec")
        logger.info(f"  延迟稳定性: 标准差{performance_stats.get('latency_ms', {}).get('stddev', 0):.2f}ms")
        logger.info(f"  系统资源: CPU平均{system_stats.get('cpu_percent', {}).get('mean', 0):.1f}%, 内存峰值{system_stats.get('memory_peak_used_gb', 0):.2f}GB")
        
        # 验证长期稳定性
        completion_rate = performance_stats['total_tasks'] / task_id
        assert completion_rate > 0.95  # 95%以上完成率
        assert throughput > 30  # 维持30+ tasks/sec
        
        # 验证延迟稳定性（标准差不能太大）
        latency_stddev = performance_stats.get('latency_ms', {}).get('stddev', 0)
        latency_mean = performance_stats.get('latency_ms', {}).get('mean', 0)
        if latency_mean > 0:
            cv = latency_stddev / latency_mean  # 变异系数
            assert cv < 2.0  # 变异系数小于2
    
    def test_memory_efficiency(self):
        """内存效率测试"""
        logger.info("开始内存效率测试")
        
        def memory_worker(task_id: int, submit_time: float, data_size: int) -> Dict[str, Any]:
            """内存使用测试worker"""
            # 分配指定大小的内存
            data = bytearray(data_size)
            data[:100] = b'x' * 100  # 写入一些数据
            
            # 简单处理
            checksum = sum(data[::1000]) if data_size > 1000 else sum(data)
            
            return {
                'task_id': task_id,
                'data_size': data_size,
                'checksum': checksum
            }
        
        collector = PerformanceCollector()
        monitor = SystemMonitor()
        
        mpms = MPMS(
            worker=memory_worker,
            collector=collector.collect,
            processes=2,
            threads=2,
            lifecycle_duration_hard=120.0
        )
        
        monitor.start_monitoring()
        initial_memory = psutil.virtual_memory().used / (1024**3)  # GB
        
        mpms.start()
        
        # 提交不同内存需求的任务
        data_sizes = [1024, 10*1024, 100*1024, 1024*1024]  # 1KB to 1MB
        task_id = 0
        
        for size in data_sizes:
            for _ in range(50):  # 每种大小50个任务
                mpms.put(task_id, time.time(), size)
                task_id += 1
        
        mpms.join()
        monitor.stop_monitoring()
        
        final_memory = psutil.virtual_memory().used / (1024**3)  # GB
        memory_increase = final_memory - initial_memory
        
        performance_stats = collector.get_performance_stats()
        system_stats = monitor.get_stats()
        
        logger.info(f"内存效率测试结果:")
        logger.info(f"  处理任务数: {performance_stats['total_tasks']}")
        logger.info(f"  内存增长: {memory_increase:.2f}GB")
        logger.info(f"  内存峰值: {system_stats.get('memory_peak_used_gb', 0):.2f}GB")
        logger.info(f"  内存使用率峰值: {system_stats.get('memory_percent', {}).get('max', 0):.1f}%")
        
        # 验证内存效率
        assert memory_increase < 1.0  # 内存增长小于1GB
        assert system_stats.get('memory_percent', {}).get('max', 0) < 90  # 内存使用率小于90%


if __name__ == "__main__":
    # 可以直接运行这个文件进行性能测试
    import sys
    pytest.main([__file__, "-v", "-s"] + sys.argv[1:]) 