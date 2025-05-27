# lifecycle_duration_hard 功能实现总结

## 功能概述

`lifecycle_duration_hard` 是 MPMS 框架的一个新参数，用于设置进程和任务的硬性时间限制，防止任务 hang 死导致 worker 无法接收新任务的情况。

## 实现细节

### 1. 参数定义

- **参数名**: `lifecycle_duration_hard`
- **类型**: `float | None`
- **单位**: 秒
- **默认值**: `None`（不启用硬性超时）

### 2. 功能实现

#### 2.1 进程超时检测

- 在 `_subproc_check` 方法中检查每个进程的存活时间
- 如果进程运行时间超过 `lifecycle_duration_hard`，则：
  1. 先尝试 `terminate()` 终止进程
  2. 等待1秒，如果进程仍然存活，使用 `kill()` 强制杀死
  3. 根据需要启动新的进程替代

#### 2.2 任务超时检测

- 每个任务在入队时记录时间戳（在 TaskTuple 中添加了 `enqueue_time` 字段）
- 在 `_subproc_check` 方法中检查所有运行中的任务
- 如果任务运行时间超过 `lifecycle_duration_hard`，则：
  1. 生成 `TimeoutError` 异常
  2. 将异常放入结果队列
  3. 让 collector 处理超时任务

### 3. 关键修改

1. **TaskTuple 类型扩展**：
   ```python
   TaskTuple = tuple[t.Any, tuple[t.Any, ...], dict[str, t.Any], float]  # (taskid, args, kwargs, enqueue_time)
   ```

2. **进程启动时间记录**：
   ```python
   worker_processes_start_time: dict[str, float]  # 记录每个进程的启动时间
   ```

3. **join 方法改进**：
   - 在等待进程结束时也调用 `_subproc_check`
   - 确保即使没有新任务提交，超时检测仍然能够执行

### 4. 使用示例

```python
from mpms import MPMS

def worker(index):
    if index == 5:
        time.sleep(100)  # 模拟 hang 住的任务
    return f"Task {index} done"

def collector(meta, result):
    if isinstance(result, Exception):
        print(f"Task failed: {result}")
    else:
        print(f"Task completed: {result}")

m = MPMS(
    worker,
    collector,
    processes=2,
    threads=2,
    lifecycle_duration_hard=5.0,  # 5秒硬性超时
    subproc_check_interval=0.5    # 每0.5秒检查一次
)
m.start()
for i in range(10):
    m.put(i)
m.join()
```

### 5. 注意事项

1. **任务丢失**: 当进程被强制终止时，正在该进程中执行的任务会丢失，但会被标记为超时错误
2. **检查间隔**: `subproc_check_interval` 决定了超时检测的精度，设置过小会增加开销
3. **collector 必需**: 任务超时检测只在有 collector 的情况下工作，因为需要记录运行中的任务

### 6. 与其他生命周期参数的区别

- `lifecycle`: 基于任务计数的软性限制，线程处理指定数量任务后正常退出
- `lifecycle_duration`: 基于时间的软性限制，线程运行指定时间后正常退出
- `lifecycle_duration_hard`: 基于时间的硬性限制，进程超时会被强制终止，任务超时会被标记为错误

这三个参数可以同时使用，提供多层次的生命周期管理。 