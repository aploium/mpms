# MPMS Zombie进程问题修复总结

## 问题描述

在线上环境中，MPMS出现以下症状：
- 运行足够多次数后所有子worker都变成zombie状态
- 只有一个worker进程在工作
- 主进程仍然能够将任务put进去
- zombie worker没有被回收和产生新的worker
- 维持这种状态很多个小时

## 根本原因

1. **主要原因：zombie进程未被正确回收**
   - 在`_subproc_check`方法中，当检测到进程已死亡时，只调用了`p.terminate()`和`p.close()`
   - **没有调用`p.join()`来回收zombie进程**
   - 导致死亡的子进程变成zombie状态，占用系统资源

2. **次要原因：进程重启逻辑缺陷**
   - 使用`len(self.running_tasks)`计算需要的进程数
   - 如果任务已完成，可能导致不启动足够的进程

3. **并发问题：任务超时处理逻辑**
   - 超时任务的处理可能导致collector中的KeyError

## 修复方案

### 1. 修复zombie进程回收（核心修复）

```python
# 在_subproc_check中
elif not p.is_alive():
    # 进程已死亡的正常处理
    logger.info('mpms subprocess %s dead, restarting', name)
    # 重要修复：必须调用join()来回收zombie进程
    try:
        p.join(timeout=0.5)  # 等待0.5秒回收zombie进程
    except:
        pass  # 如果join失败，继续处理
    p.terminate()  # 确保进程终止（虽然已经死了，但这是个好习惯）
    p.close()
    processes_to_remove.append(name)
    need_restart = True
```

### 2. 修复进程重启逻辑

```python
# 修复：始终维持配置的进程数，除非任务队列已关闭
needed_process_count = self.processes_count
```

### 3. 修复collector中的并发问题

```python
# 检查任务是否还在running_tasks中
if taskid in self.running_tasks:
    _, self.meta.args, self.meta.kwargs, _ = self.running_tasks.pop(taskid)
    # ... 处理任务
else:
    # 任务已经被处理（可能是超时任务）
    logger.debug("mpms collector received result for already processed task: %s", taskid)
    self.finish_count += 1
```

### 4. 新增优雅关闭方法

```python
def graceful_shutdown(self, timeout: float = 30.0) -> bool:
    """优雅关闭MPMS实例，适用于轮转场景"""
    # 1. 关闭任务队列
    # 2. 等待所有任务完成或超时
    # 3. 强制终止剩余进程
    # 4. 清理资源
```

## 使用建议

### 1. 轮转场景的正确用法

```python
# 不推荐的方式
threading.Thread(target=self.cloner.close, daemon=True).start()
time.sleep(30)

# 推荐的方式
old_cloner = self.cloner
self.cloner = None  # 先断开引用

# 使用graceful_shutdown进行优雅关闭
def close_old():
    success = old_cloner.graceful_shutdown(timeout=60)
    logger.info(f"旧实例关闭{'成功' if success else '失败'}")
    
threading.Thread(target=close_old).start()

# 等待足够的时间确保旧实例完全关闭
time.sleep(5)
gc.collect()

# 创建新实例
self.cloner = Cloner(...)
self.cloner.start()
```

### 2. 监控和诊断

- 定期检查系统中的zombie进程数量
- 监控MPMS的进程健康状态日志
- 设置合理的`lifecycle_duration_hard`防止进程hang死

### 3. 配置建议

```python
mpms.MPMS(
    worker=worker_func,
    collector=collector_func,
    processes=16,
    threads=2,
    lifecycle_duration=900,  # 15分钟软限制
    lifecycle_duration_hard=1800,  # 30分钟硬限制
    worker_graceful_die_timeout=30,  # 优雅退出30秒超时
    subproc_check_interval=3,  # 3秒检查一次进程状态
)
```

## 测试验证

修复已通过以下测试：
1. 直接的zombie进程回收测试
2. MPMS进程崩溃和恢复测试
3. 轮转场景测试

## 注意事项

1. **系统限制**：确保系统的最大进程数限制足够高
2. **资源清理**：在程序退出前务必调用`join()`确保所有资源被正确清理
3. **错误处理**：worker函数中的异常应该被正确捕获和处理
4. **日志监控**：关注"mpms process health check"相关的警告日志 