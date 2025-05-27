#!/usr/bin/env python3
# coding=utf-8
"""验证 GracefulDie 重命名是否成功"""

try:
    from mpms import MPMS, WorkerGracefulDie
    print("✓ Successfully imported MPMS and WorkerGracefulDie")
except ImportError as e:
    print(f"✗ Import error: {e}")
    exit(1)

# 测试基本功能
def test_basic():
    results = []
    
    def worker(index):
        if index == 2:
            raise WorkerGracefulDie("Test graceful die")
        return f"task_{index}"
    
    def collector(meta, result):
        results.append((meta.taskid, result))
    
    m = MPMS(
        worker,
        collector,
        processes=1,
        threads=1,
        worker_graceful_die_timeout=1,
    )
    
    print("✓ MPMS instance created with graceful die parameters")
    
    # 检查属性是否存在
    assert hasattr(m, 'worker_graceful_die_timeout'), "Missing worker_graceful_die_timeout attribute"
    assert hasattr(m, 'worker_graceful_die_exceptions'), "Missing worker_graceful_die_exceptions attribute"
    assert m.worker_graceful_die_timeout == 1, "Incorrect timeout value"
    assert WorkerGracefulDie in m.worker_graceful_die_exceptions, "WorkerGracefulDie not in exceptions"
    
    print("✓ All attributes correctly set")
    print("\nRename verification successful! All tests passed.")

if __name__ == '__main__':
    test_basic() 