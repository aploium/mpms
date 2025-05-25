# MPMS 初始化函数功能

MPMS 现在支持在创建子进程和子线程时执行自定义的初始化函数，这个功能参考了 Python 标准库 `concurrent.futures` 中 `ProcessPoolExecutor` 和 `ThreadPoolExecutor` 的设计。

## 功能概述

### 1. 进程初始化函数 (process_initializer)
- 在每个工作进程启动时调用一次
- 用于初始化进程级别的资源，如数据库连接池、缓存客户端等
- 如果初始化失败（抛出异常），该进程将退出，不会处理任何任务

### 2. 线程初始化函数 (thread_initializer)
- 在每个工作线程启动时调用一次
- 用于初始化线程级别的资源，如 HTTP 会话、线程本地存储等
- 如果初始化失败（抛出异常），该线程将退出，不会处理任何任务

## 使用方法

```python
from mpms import MPMS

def process_init(config):
    """进程初始化函数"""
    # 初始化进程级资源
    print(f"Process {os.getpid()} initialized with config: {config}")

def thread_init(name):
    """线程初始化函数"""
    # 初始化线程级资源
    print(f"Thread {threading.current_thread().name} initialized with name: {name}")

def worker(x):
    """工作函数"""
    return x * 2

# 创建 MPMS 实例
m = MPMS(
    worker,
    processes=2,
    threads=3,
    process_initializer=process_init,
    process_initargs=({'db_host': 'localhost'},),
    thread_initializer=thread_init,
    thread_initargs=('Worker',),
)

# 启动并使用
m.start()
for i in range(10):
    m.put(i)
m.join()
```

## 参数说明

- `process_initializer`: 可调用对象，在每个工作进程启动时调用
- `process_initargs`: 元组，传递给 process_initializer 的参数
- `thread_initializer`: 可调用对象，在每个工作线程启动时调用
- `thread_initargs`: 元组，传递给 thread_initializer 的参数

## 示例文件

1. **demo_initializer.py**: 基础示例，展示如何使用初始化函数
2. **demo_initializer_advanced.py**: 高级示例，展示实际应用场景（数据库连接池、HTTP会话等）
3. **test_initializer.py**: 测试脚本，验证初始化功能和错误处理

## 应用场景

1. **数据库连接池初始化**
   - 在进程级别创建连接池，避免每个任务都创建新连接
   - 所有线程共享同一个连接池

2. **HTTP 会话管理**
   - 为每个线程创建独立的 HTTP 会话
   - 支持会话级别的认证、cookie 等

3. **日志配置**
   - 为每个进程/线程配置独立的日志记录器
   - 支持不同的日志级别和输出目标

4. **资源预加载**
   - 加载大型模型或数据文件
   - 初始化第三方库或服务连接

## 注意事项

1. 初始化函数应该是幂等的，即多次调用结果相同
2. 初始化函数中的异常会导致对应的进程或线程退出
3. 进程级资源应该在全局变量中存储
4. 线程级资源应该使用 `threading.local()` 存储
5. 考虑资源清理问题（虽然 MPMS 退出时会自动清理进程和线程）

## 与 concurrent.futures 的对比

MPMS 的初始化函数设计与 `concurrent.futures` 类似，但有以下特点：

1. **分离的进程和线程初始化**: MPMS 支持分别为进程和线程设置初始化函数
2. **多进程多线程架构**: MPMS 的每个进程可以包含多个线程
3. **更灵活的错误处理**: 初始化失败只影响单个进程或线程，不会导致整个池失败

## 版本要求

- Python 3.7+（使用了类型注解）
- 无其他外部依赖 