# MPMS 优雅退出机制 (Graceful Die Mechanism)

## 概述

优雅退出机制允许 worker 进程在检测到自身处于不健康状态时主动退出，而不是等待硬超时。这提供了一种更快速、更优雅的方式来淘汰有问题的进程。

## 功能特性

1. **主动健康管理**：Worker 可以主动检测并报告自身的健康状态
2. **可配置的异常类型**：支持自定义触发优雅退出的异常类型
3. **超时保护**：配置优雅退出超时时间，确保进程最终会退出
4. **线程协调**：当一个线程触发优雅退出时，同进程的所有线程都会停止接收新任务

## 使用方法

### 基本用法

```python
from mpms import MPMS, WorkerGracefulDie

def worker(index):
    # 检测到不健康状态
    if some_unhealthy_condition:
        raise WorkerGracefulDie("Worker is unhealthy")
    
    # 正常处理任务
    return process_task(index)

m = MPMS(
    worker,
    collector,
    worker_graceful_die_timeout=5,  # 5秒超时
    worker_graceful_die_exceptions=(WorkerGracefulDie,)  # 默认值
)
```

### 自定义异常

```python
# 使用内置异常
m = MPMS(
    worker,
    collector,
    worker_graceful_die_exceptions=(WorkerGracefulDie, MemoryError)
)

# 使用自定义异常
class ResourceExhausted(Exception):
    pass

m = MPMS(
    worker,
    collector,
    worker_graceful_die_exceptions=(WorkerGracefulDie, ResourceExhausted)
)
```

## 参数说明

- `worker_graceful_die_timeout` (float): 优雅退出超时时间（秒），默认为 5 秒
- `worker_graceful_die_exceptions` (tuple[type[Exception], ...]): 触发优雅退出的异常类型元组，默认为 `(WorkerGracefulDie,)`

## 工作原理

1. 当 worker 函数抛出配置的优雅退出异常时，该异常会被捕获并设置优雅退出事件
2. 同进程的所有工作线程检测到优雅退出事件后会停止接收新任务
3. 进程会等待配置的超时时间（`worker_graceful_die_timeout`）
4. 超时后，进程会调用 `os._exit(1)` 强制退出
5. 主进程检测到子进程退出后，会根据需要启动新的进程

## 应用场景

### 1. 内存监控
```python
def worker(index):
    if psutil.Process().memory_percent() > 80:
        raise MemoryError("Memory usage too high")
    return process_task(index)
```

### 2. 健康检查
```python
def worker(index):
    if not health_check():
        raise WorkerGracefulDie("Health check failed")
    return process_task(index)
```

### 3. 资源限制
```python
class ResourceExhausted(Exception):
    pass

task_count = 0
MAX_TASKS = 100

def worker(index):
    global task_count
    task_count += 1
    if task_count > MAX_TASKS:
        raise ResourceExhausted("Task limit reached")
    return process_task(index)
```

### 4. 优雅关闭
```python
shutdown_requested = False

def worker(index):
    if shutdown_requested:
        raise WorkerGracefulDie("Shutdown requested")
    return process_task(index)
```

## 注意事项

1. **异常仍会被报告**：优雅退出异常仍然会通过 collector 报告，以便追踪和调试
2. **未完成的任务**：触发优雅退出时，进程中未完成的任务可能会丢失或超时
3. **进程级别**：优雅退出影响整个进程，包括该进程的所有线程
4. **与硬超时配合**：优雅退出机制与 `lifecycle_duration_hard` 配合使用，提供多层保护

## 与其他生命周期机制的关系

- **lifecycle**: 基于任务计数的生命周期，正常退出
- **lifecycle_duration**: 基于时间的生命周期，正常退出
- **worker_graceful_die**: 基于健康状态的生命周期，优雅退出
- **lifecycle_duration_hard**: 强制超时，最后的保护机制

这些机制可以同时使用，提供全面的进程生命周期管理。 