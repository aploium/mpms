# mpms
Simple python Multiprocesses-Multithreads queue  
简易Python多进程-多线程任务队列  
(自用, ap不为生产环境下造成的任何损失和灵异现象负责)
  
在多个进程的多个线程的 worker 中完成耗时的任务, 并在主进程的 collector 中处理结果

支持python 3.8+

### Install

```shell
pip install mpms
```

### Quick Start

```python
import requests
from mpms import MPMS

def worker(i, j=None):
    r = requests.get('http://example.com', params={"q": i})
    return r.elapsed

def collector(meta, result):
    print(meta.args[0], result)

def main():
    m = MPMS(
        worker,
        collector,  # optional
        processes=2,
        threads=10,  # 每进程的线程数
    )
    m.start()
    for i in range(100):  # 你可以自行控制循环条件
        m.put(i, j=i + 1)  # 这里的参数列表就是worker接受的参数
    m.join()

if __name__ == '__main__':
    main()
```

### New Features (v2.2.0)

#### Lifecycle Management

MPMS now supports automatic worker thread rotation with two lifecycle control methods:

1. **Count-based lifecycle** (`lifecycle` parameter): Worker threads exit after processing a specified number of tasks
2. **Time-based lifecycle** (`lifecycle_duration` parameter): Worker threads exit after running for a specified duration (in seconds)

Both parameters can be used together - threads will exit when either condition is met first.

```python
# Count-based lifecycle
m = MPMS(worker, lifecycle=100)  # Each thread exits after 100 tasks

# Time-based lifecycle  
m = MPMS(worker, lifecycle_duration=3600)  # Each thread exits after 1 hour

# Combined lifecycle
m = MPMS(worker, lifecycle=100, lifecycle_duration=3600)  # Exit on 100 tasks OR 1 hour
```

#### Iterator-based Result Collection (v2.5.0)

MPMS now supports an alternative way to collect results using the `iter_results()` method. This provides a more Pythonic way to process results without defining a separate collector function.

```python
from mpms import MPMS

def worker(i):
    # 你的处理逻辑
    return i * 2

# 使用 iter_results 获取结果
m = MPMS(worker, processes=2, threads=4)
m.start()

# 提交任务
for i in range(10):
    m.put(i)

# 关闭任务队列（必须在使用 iter_results 之前）
m.close()

# 迭代获取结果
for meta, result in m.iter_results():
    if isinstance(result, Exception):
        print(f"任务 {meta.taskid} 失败: {result}")
    else:
        print(f"任务 {meta.taskid} 结果: {result}")

m.join(close=False)  # 注意：已经调用过 close()
```

**注意事项：**
- `iter_results()` 不能与 `collector` 参数同时使用
- 必须在调用 `close()` 之后才能使用 `iter_results()`
- 迭代器会自动结束当所有任务完成时
- 如果 worker 函数抛出异常，`result` 将是该异常对象

**带超时的迭代：**
```python
# 设置单个结果的获取超时（秒）
for meta, result in m.iter_results(timeout=1.0):
    # 处理结果
    pass
```

### Examples

See the `examples/` directory for complete examples:
- `examples/demo.py` - Basic usage demonstration
- `examples/demo_lifecycle.py` - Lifecycle management features
- `demo_iter_results.py` - Iterator-based result collection examples

### Tests

See the `tests/` directory for test scripts:
- `tests/test_lifecycle.py` - Tests for lifecycle management features
- `test_iter_results.py` - Tests for iterator-based result collection
