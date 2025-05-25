#!/usr/bin/env python3
# coding=utf-8
"""Advanced functionality tests for MPMS"""

import pytest
import time
import threading
import sys
import os
import random
import multiprocessing

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mpms import MPMS, Meta

# Skip markers for Windows
skip_windows = pytest.mark.skipif(sys.platform == "win32", reason="Test not supported on Windows")
skip_nested = pytest.mark.skipif(sys.platform == "win32", reason="Windows does not support nested MPMS due to daemon process limitation.")

# 全局结果收集变量
results_memory = []
results_concurrent = []
results_long = []
results_overflow = []
results_external = []
results_fail = []
failures_fail = []
results_dynamic = []
state_shared = {'counter': 0}
results_custom = []

# Worker/collector 必须为顶层函数

def memory_intensive_worker(x):
    large_list = [i for i in range(1000000)]
    return sum(large_list)

def memory_intensive_collector(meta, result):
    results_memory.append(result)

def concurrent_worker(x):
    time.sleep(0.01)
    return x * 2

def concurrent_collector(meta, result):
    results_concurrent.append((meta.args[0], result))

def long_running_worker(x):
    time.sleep(0.5)
    return x * 2

def long_running_collector(meta, result):
    results_long.append(result)

def queue_overflow_worker(x):
    time.sleep(0.1)
    return x

def queue_overflow_collector(meta, result):
    results_overflow.append(result)

def external_resource_worker(x):
    time.sleep(0.01)
    return x * 2

def external_resource_collector(meta, result):
    results_external.append((meta.args[0], result, time.time()))

def random_failure_worker(x):
    if random.random() < 0.3:
        raise ValueError(f"Random failure for task {x}")
    return x * 2

def random_failure_collector(meta, result):
    if isinstance(result, Exception):
        failures_fail.append((meta.args[0], str(result)))
    else:
        results_fail.append((meta.args[0], result))

def dynamic_task_worker(x, mpms=None):
    if x < 5:
        mpms.put(x + 1, mpms=mpms)
    return x * 2

def dynamic_task_collector(meta, result):
    results_dynamic.append(result)

def shared_state_worker(x):
    state_shared['counter'] += 1
    return state_shared['counter']

def shared_state_collector(meta, result):
    pass

def custom_meta_worker(x, meta=None):
    results_custom.append((x, getattr(meta, 'custom_data', None)))
    return (x, getattr(meta, 'custom_data', None))

def custom_meta_collector(meta, result):
    pass

def outer_worker(x):
    inner_results = []
    def inner_collector(meta, result):
        inner_results.append(result)
    inner_mpms = MPMS(inner_worker, inner_collector, processes=1, threads=1)
    inner_mpms.start()
    inner_mpms.put(x)
    inner_mpms.join()
    return inner_results[0]

def inner_worker(x):
    return x * 2

class TestMPMSAdvanced:
    def setup_method(self):
        results_memory.clear()
        results_concurrent.clear()
        results_long.clear()
        results_overflow.clear()
        results_external.clear()
        results_fail.clear()
        failures_fail.clear()
        results_dynamic.clear()
        state_shared['counter'] = 0
        results_custom.clear()

    @skip_windows
    def test_worker_with_memory_intensive_task(self):
        m = MPMS(memory_intensive_worker, memory_intensive_collector, processes=2, threads=2)
        m.start()
        for _ in range(5):
            m.put(1)
        m.join()
        assert len(results_memory) == 5
        assert all(r == 499999500000 for r in results_memory)

    def test_concurrent_collector_calls(self):
        m = MPMS(concurrent_worker, concurrent_collector, processes=4, threads=4)
        m.start()
        for i in range(40):
            m.put(i)
        m.join()
        assert len(results_concurrent) == 40
        results = sorted(results_concurrent, key=lambda x: x[0])
        for i, (input_val, output_val) in enumerate(results):
            assert input_val == i
            assert output_val == i * 2

    def test_worker_with_long_running_task(self):
        m = MPMS(long_running_worker, long_running_collector, processes=2, threads=2)
        m.start()
        start_time = time.time()
        for i in range(10):
            m.put(i)
        m.join()
        duration = time.time() - start_time
        assert len(results_long) == 10
        assert duration < 3.0
        assert all(r == i * 2 for i, r in enumerate(sorted(results_long)))

    def test_task_queue_overflow_handling(self):
        m = MPMS(queue_overflow_worker, queue_overflow_collector, processes=1, threads=1, task_queue_maxsize=2)
        m.start()
        for i in range(5):
            m.put(i)
        m.join()
        assert len(results_overflow) == 5
        assert sorted(results_overflow) == list(range(5))

    def test_worker_with_external_resource(self):
        m = MPMS(external_resource_worker, external_resource_collector, processes=2, threads=2)
        m.start()
        for i in range(10):
            m.put(i)
        m.join()
        assert len(results_external) == 10
        times = [t for _, _, t in sorted(results_external)]
        assert all(t2 - t1 >= -0.001 for t1, t2 in zip(times[:-1], times[1:]))

    def test_worker_with_random_failures(self):
        m = MPMS(random_failure_worker, random_failure_collector, processes=2, threads=2)
        m.start()
        for i in range(20):
            m.put(i)
        m.join()
        assert len(results_fail) + len(failures_fail) == 20
        assert len(failures_fail) > 0
        assert len(results_fail) > 0
        results = sorted(results_fail, key=lambda x: x[0])
        for input_val, output_val in results:
            assert output_val == input_val * 2

    @skip_nested
    def test_worker_with_nested_mpms(self):
        results = []
        def collector(meta, result):
            results.append(result)
        m = MPMS(outer_worker, collector, processes=2, threads=2)
        m.start()
        for i in range(5):
            m.put(i)
        m.join()
        assert len(results) == 5
        assert all(r == i * 2 for i, r in enumerate(sorted(results)))

    @skip_windows
    def test_worker_with_dynamic_task_generation(self):
        m = MPMS(dynamic_task_worker, dynamic_task_collector, processes=2, threads=2)
        m.start()
        m.put(0, mpms=m)
        m.join()
        assert len(results_dynamic) == 6
        assert sorted(results_dynamic) == [i * 2 for i in range(6)]

    @skip_windows
    def test_worker_with_shared_state(self):
        m = MPMS(shared_state_worker, shared_state_collector, processes=2, threads=2)
        m.start()
        for _ in range(10):
            m.put(1)
        m.join()
        assert state_shared['counter'] == 10

    @skip_windows
    def test_worker_with_custom_meta(self):
        m = MPMS(custom_meta_worker, custom_meta_collector, processes=2, threads=2)
        m.start()
        for i in range(5):
            meta = Meta(m)
            meta.args = (i,)
            meta.custom_data = str(i)
            m.put(i, meta=meta)
        m.join()
        assert len(results_custom) == 5
        results = sorted(results_custom, key=lambda x: x[0])
        for i, (input_val, custom_data) in enumerate(results):
            assert input_val == i
            assert custom_data == str(i) 