#!/usr/bin/env python3
# coding=utf-8
"""Performance and stress tests for MPMS"""

import pytest
import time
import threading
import random
import sys
import os
import gc
import psutil
from concurrent.futures import ThreadPoolExecutor
import multiprocessing

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mpms import MPMS

# Skip performance tests on Windows due to multiprocessing limitations
skip_windows = pytest.mark.skipif(sys.platform == "win32", reason="Performance tests not supported on Windows due to multiprocessing limitations")

class TestPerformance:
    """Performance-related tests"""
    
    @skip_windows
    @pytest.mark.slow
    def test_high_throughput(self):
        """Test MPMS with high throughput workload"""
        results_count = 0
        lock = threading.Lock()
        
        def worker(x):
            # Minimal work to test throughput
            return x * 2
        
        def collector(meta, result):
            nonlocal results_count
            with lock:
                results_count += 1
        
        m = MPMS(
            worker,
            collector,
            processes=multiprocessing.cpu_count(),
            threads=4
        )
        m.start()
        
        start_time = time.time()
        num_tasks = 10000
        
        # Submit many tasks quickly
        for i in range(num_tasks):
            m.put(i)
        
        m.join()
        end_time = time.time()
        
        # All tasks should complete
        assert results_count == num_tasks
        
        # Calculate throughput
        duration = end_time - start_time
        throughput = num_tasks / duration
        print(f"\nThroughput: {throughput:.2f} tasks/second")
        print(f"Duration: {duration:.2f} seconds for {num_tasks} tasks")
        
        # Basic performance assertion (adjust based on your needs)
        assert throughput > 100  # At least 100 tasks/second
    
    @skip_windows
    def test_memory_efficiency(self):
        """Test that MPMS doesn't leak memory with many tasks"""
        import tracemalloc
        
        tracemalloc.start()
        
        def worker(x):
            # Create some temporary data
            data = [i for i in range(100)]
            return sum(data) + x
        
        def collector(meta, result):
            pass  # Just discard results
        
        m = MPMS(worker, collector, processes=2, threads=2)
        m.start()
        
        # Get initial memory
        gc.collect()
        initial_memory = tracemalloc.get_traced_memory()[0]
        
        # Process many tasks
        for batch in range(10):
            for i in range(100):
                m.put(i)
            time.sleep(0.1)  # Allow some processing
        
        m.join()
        
        # Get final memory
        gc.collect()
        final_memory = tracemalloc.get_traced_memory()[0]
        tracemalloc.stop()
        
        # Memory increase should be reasonable (not growing indefinitely)
        memory_increase = final_memory - initial_memory
        print(f"\nMemory increase: {memory_increase / 1024 / 1024:.2f} MB")
        
        # Assert memory didn't grow too much (adjust threshold as needed)
        assert memory_increase < 50 * 1024 * 1024  # Less than 50MB increase
    
    @skip_windows
    def test_queue_blocking_behavior(self):
        """Test behavior when task queue is full"""
        slow_task_started = threading.Event()
        all_puts_done = threading.Event()
        
        def slow_worker(x):
            if x == 0:
                slow_task_started.set()
                time.sleep(0.5)  # Block the worker
            return x
        
        # Small queue size to test blocking
        m = MPMS(
            slow_worker,
            processes=1,
            threads=1,
            task_queue_maxsize=5
        )
        m.start()
        
        # Put first task that will block worker
        m.put(0)
        slow_task_started.wait()
        
        # Try to fill the queue
        start_time = time.time()
        for i in range(10):
            m.put(i + 1)
        all_puts_done.set()
        put_duration = time.time() - start_time
        
        m.join()
        
        # Putting should have blocked since queue was full
        assert put_duration > 0.3  # Should have waited for worker
    
    @skip_windows
    @pytest.mark.slow
    def test_scalability(self):
        """Test MPMS scalability with different process/thread counts"""
        def cpu_bound_worker(x):
            # CPU-bound work
            total = 0
            for i in range(x, x + 1000):
                total += i * i
            return total
        
        results = []
        configurations = [
            (1, 1),  # 1 process, 1 thread
            (1, 4),  # 1 process, 4 threads
            (2, 2),  # 2 processes, 2 threads
            (4, 1),  # 4 processes, 1 thread
        ]
        
        num_tasks = 1000
        
        for processes, threads in configurations:
            result_count = 0
            
            def collector(meta, result):
                nonlocal result_count
                result_count += 1
            
            m = MPMS(
                cpu_bound_worker,
                collector,
                processes=processes,
                threads=threads
            )
            m.start()
            
            start_time = time.time()
            for i in range(num_tasks):
                m.put(i)
            m.join()
            duration = time.time() - start_time
            
            results.append({
                'config': f"{processes}p{threads}t",
                'duration': duration,
                'throughput': num_tasks / duration
            })
            
            assert result_count == num_tasks
        
        # Print results
        print("\nScalability test results:")
        for r in results:
            print(f"{r['config']}: {r['duration']:.2f}s, {r['throughput']:.2f} tasks/s")
        
        # Multi-process should be faster than single process for CPU-bound work
        single_process_time = results[0]['duration']
        multi_process_time = results[2]['duration']
        assert multi_process_time < single_process_time * 0.9  # At least 10% faster


class TestStress:
    """Stress tests for MPMS"""
    
    @skip_windows
    def test_rapid_lifecycle_rotation(self):
        """Test rapid worker rotation with small lifecycle values"""
        completed_tasks = []
        lock = threading.Lock()
        
        def worker(x):
            return x
        
        def collector(meta, result):
            with lock:
                completed_tasks.append(result)
        
        # Very short lifecycle to force frequent rotation
        m = MPMS(
            worker,
            collector,
            processes=2,
            threads=4,
            lifecycle=2,  # Exit after just 2 tasks
            lifecycle_duration=0.1  # Or after 0.1 seconds
        )
        m.start()
        
        # Submit many tasks
        for i in range(100):
            m.put(i)
        
        m.join()
        
        # Despite rotation, all tasks should complete
        assert len(completed_tasks) == 100
        assert sorted(completed_tasks) == list(range(100))
    
    @skip_windows
    def test_concurrent_put_get_stress(self):
        """Stress test with many concurrent puts"""
        results = []
        lock = threading.Lock()
        
        def worker(x):
            time.sleep(random.uniform(0.001, 0.01))  # Random small delay
            return x
        
        def collector(meta, result):
            with lock:
                results.append(result)
        
        m = MPMS(worker, collector, processes=4, threads=4)
        m.start()
        
        # Launch many threads that put tasks concurrently
        def put_worker(start, count):
            for i in range(start, start + count):
                m.put(i)
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for i in range(10):
                future = executor.submit(put_worker, i * 100, 100)
                futures.append(future)
            
            # Wait for all puts to complete
            for future in futures:
                future.result()
        
        m.join()
        
        # All 1000 tasks should complete
        assert len(results) == 1000
        assert sorted(results) == list(range(1000))
    
    @skip_windows
    def test_error_recovery_stress(self):
        """Test MPMS behavior under high error rate"""
        success_count = 0
        error_count = 0
        lock = threading.Lock()
        
        def unreliable_worker(x):
            # 30% chance of error
            if random.random() < 0.3:
                raise Exception(f"Random error for task {x}")
            return x
        
        def collector(meta, result):
            nonlocal success_count, error_count
            with lock:
                if isinstance(result, Exception):
                    error_count += 1
                else:
                    success_count += 1
        
        m = MPMS(
            unreliable_worker,
            collector,
            processes=2,
            threads=4
        )
        m.start()
        
        num_tasks = 1000
        for i in range(num_tasks):
            m.put(i)
        
        m.join()
        
        # All tasks should be accounted for
        assert success_count + error_count == num_tasks
        
        # Error rate should be roughly 30%
        error_rate = error_count / num_tasks
        assert 0.25 < error_rate < 0.35  # Allow some variance
        
        print(f"\nError recovery test: {error_count} errors, {success_count} successes")
    
    @skip_windows
    @pytest.mark.slow
    def test_long_running_tasks(self):
        """Test with tasks that take a long time"""
        completed = []
        lock = threading.Lock()
        
        def long_worker(x):
            time.sleep(0.5)  # Each task takes 0.5 seconds
            return x
        
        def collector(meta, result):
            with lock:
                completed.append(result)
        
        m = MPMS(
            long_worker,
            collector,
            processes=2,
            threads=5  # 10 total workers
        )
        m.start()
        
        # Submit 20 tasks (should take ~1 second with 10 workers)
        start_time = time.time()
        for i in range(20):
            m.put(i)
        
        m.join()
        duration = time.time() - start_time
        
        assert len(completed) == 20
        # With 10 workers and 0.5s per task, 20 tasks should take ~1 second
        assert 0.8 < duration < 1.5  # Allow some overhead


if __name__ == '__main__':
    # Run with markers: pytest test_mpms_performance.py -v -m "not slow"
    pytest.main([__file__, '-v']) 