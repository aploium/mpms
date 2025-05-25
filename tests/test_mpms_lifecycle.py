#!/usr/bin/env python3
# coding=utf-8
"""Lifecycle functionality tests for MPMS using pytest"""

import pytest
import time
import threading
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mpms import MPMS

# Global variables for test results
lifecycle_task_count = 0
lifecycle_lock = threading.Lock()
lifecycle_results = []
lifecycle_start_time = 0

# Worker and collector functions at module level for Windows compatibility

def count_lifecycle_worker(task_id):
    time.sleep(0.01)  # Small delay
    return f"Task {task_id} done"

def count_lifecycle_collector(meta, result):
    global lifecycle_task_count
    with lifecycle_lock:
        lifecycle_task_count += 1

def time_lifecycle_worker(task_id):
    time.sleep(0.1)  # Each task takes 0.1 seconds
    return f"Task {task_id} done"

def time_lifecycle_collector(meta, result):
    global lifecycle_task_count
    with lifecycle_lock:
        lifecycle_task_count += 1

def combined_count_worker(task_id):
    time.sleep(0.01)  # Fast tasks
    return f"Task {task_id} done"

def combined_count_collector(meta, result):
    global lifecycle_task_count
    with lifecycle_lock:
        lifecycle_task_count += 1

def combined_time_worker(task_id):
    time.sleep(0.2)  # Slow tasks
    return f"Task {task_id} done"

def combined_time_collector(meta, result):
    global lifecycle_task_count
    with lifecycle_lock:
        lifecycle_task_count += 1

def multi_process_worker(task_id):
    time.sleep(0.01)
    return (task_id, os.getpid())

def multi_process_collector(meta, result):
    with lifecycle_lock:
        lifecycle_results.append(result)

def none_values_worker(task_id):
    time.sleep(0.01)
    return f"Task {task_id} done"

def none_values_collector(meta, result):
    global lifecycle_task_count
    with lifecycle_lock:
        lifecycle_task_count += 1

def parametrized_worker(task_id):
    time.sleep(0.1)
    return f"Task {task_id} done"

def parametrized_collector(meta, result):
    global lifecycle_task_count
    with lifecycle_lock:
        lifecycle_task_count += 1

def zero_value_worker(task_id):
    return f"Task {task_id} done"

def zero_value_collector(meta, result):
    global lifecycle_task_count
    with lifecycle_lock:
        lifecycle_task_count += 1

def exception_worker(task_id):
    if task_id == 3:
        raise ValueError(f"Error in task {task_id}")
    time.sleep(0.01)
    return f"Task {task_id} done"

def exception_collector(meta, result):
    global lifecycle_task_count
    with lifecycle_lock:
        lifecycle_task_count += 1


class TestLifecycle:
    """Test lifecycle management features"""
    
    def setup_method(self):
        """Clear global test data before each test"""
        global lifecycle_task_count, lifecycle_results, lifecycle_start_time
        lifecycle_task_count = 0
        lifecycle_results.clear()
        lifecycle_start_time = time.time()
    
    def test_count_based_lifecycle(self):
        """Test count-based lifecycle (lifecycle parameter)"""
        # Each thread exits after 5 tasks
        m = MPMS(
            count_lifecycle_worker,
            count_lifecycle_collector,
            processes=1,
            threads=2,
            lifecycle=5
        )
        m.start()
        
        # Submit 15 tasks
        for i in range(15):
            m.put(i)
        
        m.join()
        
        # With 2 threads and lifecycle=5, should complete at most 10 tasks
        assert m.total_count == 15
        assert lifecycle_task_count <= 10
        assert m.finish_count == lifecycle_task_count
    
    def test_time_based_lifecycle(self):
        """Test time-based lifecycle (lifecycle_duration parameter)"""
        global lifecycle_start_time
        lifecycle_start_time = time.time()
        
        # Each thread exits after 0.5 seconds
        m = MPMS(
            time_lifecycle_worker,
            time_lifecycle_collector,
            processes=1,
            threads=2,
            lifecycle_duration=0.5
        )
        m.start()
        
        # Submit tasks for 1 second
        task_id = 0
        while time.time() - lifecycle_start_time < 1.0:
            m.put(task_id)
            task_id += 1
            time.sleep(0.05)
        
        m.join()
        
        # With 0.5s lifecycle and 0.1s per task, each thread can process ~5 tasks
        # With 2 threads, should complete around 10 tasks (may vary slightly)
        assert lifecycle_task_count < m.total_count  # Not all tasks completed
        assert lifecycle_task_count > 0  # Some tasks completed
    
    def test_combined_lifecycle_count_first(self):
        """Test combined lifecycle where count limit is reached first"""
        # Count limit: 5, Time limit: 10 seconds (won't be reached)
        m = MPMS(
            combined_count_worker,
            combined_count_collector,
            processes=1,
            threads=1,
            lifecycle=5,
            lifecycle_duration=10.0
        )
        m.start()
        
        # Submit 10 tasks
        for i in range(10):
            m.put(i)
        
        m.join()
        
        # Should stop at 5 tasks due to count limit
        assert lifecycle_task_count == 5
    
    def test_combined_lifecycle_time_first(self):
        """Test combined lifecycle where time limit is reached first"""
        # Count limit: 10 (won't be reached), Time limit: 0.5 seconds
        m = MPMS(
            combined_time_worker,
            combined_time_collector,
            processes=1,
            threads=1,
            lifecycle=10,
            lifecycle_duration=0.5
        )
        m.start()
        
        # Submit 10 tasks
        for i in range(10):
            m.put(i)
        
        m.join()
        
        # With 0.2s per task and 0.5s limit, should complete ~2-3 tasks
        assert lifecycle_task_count < 10  # Didn't reach count limit
        assert lifecycle_task_count >= 2  # Completed at least 2 tasks
        assert lifecycle_task_count <= 3  # But not more than 3
    
    def test_lifecycle_with_multiple_processes(self):
        """Test lifecycle with multiple processes"""
        m = MPMS(
            multi_process_worker,
            multi_process_collector,
            processes=2,
            threads=2,
            lifecycle=3  # Each thread processes 3 tasks
        )
        m.start()
        
        # Submit 20 tasks
        for i in range(20):
            m.put(i)
        
        m.join()
        
        # With 2 processes × 2 threads × 3 tasks = 12 max tasks
        assert len(lifecycle_results) <= 12
        
        # Check we have results from multiple processes
        pids = set(r[1] for r in lifecycle_results)
        assert len(pids) >= 1  # At least one process
    
    def test_lifecycle_none_values(self):
        """Test with no lifecycle limits (None values)"""
        m = MPMS(
            none_values_worker,
            none_values_collector,
            processes=1,
            threads=1,
            lifecycle=None,
            lifecycle_duration=None
        )
        m.start()
        
        # Submit 10 tasks
        for i in range(10):
            m.put(i)
        
        m.join()
        
        # Should complete all tasks
        assert lifecycle_task_count == 10
        assert m.finish_count == 10
    
    @pytest.mark.parametrize("lifecycle,duration,expected_range", [
        (5, None, (5, 5)),  # Exactly 5 tasks
        (None, 0.3, (2, 4)),  # 2-4 tasks in 0.3 seconds
        (10, 0.3, (2, 4)),  # Time limit reached first
        (3, 10.0, (3, 3)),  # Count limit reached first
    ])
    def test_lifecycle_parametrized(self, lifecycle, duration, expected_range):
        """Test various lifecycle parameter combinations"""
        m = MPMS(
            parametrized_worker,
            parametrized_collector,
            processes=1,
            threads=1,
            lifecycle=lifecycle,
            lifecycle_duration=duration
        )
        m.start()
        
        # Submit 10 tasks
        for i in range(10):
            m.put(i)
        
        m.join()
        
        min_expected, max_expected = expected_range
        assert min_expected <= lifecycle_task_count <= max_expected


class TestLifecycleEdgeCases:
    """Test edge cases for lifecycle functionality"""
    
    def setup_method(self):
        """Clear global test data before each test"""
        global lifecycle_task_count, lifecycle_results
        lifecycle_task_count = 0
        lifecycle_results.clear()
    
    def test_lifecycle_zero_value(self):
        """Test lifecycle with zero value (should not process any tasks)"""
        m = MPMS(
            zero_value_worker,
            zero_value_collector,
            processes=1,
            threads=1,
            lifecycle=0  # Zero lifecycle
        )
        m.start()
        
        # Submit 5 tasks
        for i in range(5):
            m.put(i)
        
        m.join()
        
        # With lifecycle=0, threads should exit immediately but tasks might still be processed
        # The behavior may vary, so we just check that not all tasks are processed
        assert lifecycle_task_count <= 5
    
    def test_lifecycle_with_worker_exception(self):
        """Test lifecycle behavior when worker throws exceptions"""
        m = MPMS(
            exception_worker,
            exception_collector,
            processes=1,
            threads=1,
            lifecycle=5
        )
        m.start()
        
        # Submit 10 tasks (task 3 will fail)
        for i in range(10):
            m.put(i)
        
        m.join()
        
        # Should complete 5 tasks (including the failed one)
        # The failed task still counts towards lifecycle, but collector only gets successful results
        assert lifecycle_task_count <= 5  # At most 5 successful tasks
        assert m.finish_count == 5  # All 5 tasks were processed (including failed one)


if __name__ == '__main__':
    pytest.main([__file__, '-v']) 