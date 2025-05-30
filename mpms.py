#!/usr/bin/env python3
# coding=utf-8
from __future__ import absolute_import, division, unicode_literals

import multiprocessing
import os
import queue
import threading
import logging
import weakref
import time
import typing as t
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from multiprocessing import Queue as MPQueue
else:
    MPQueue = t.Any

try:
    import setproctitle
    _setproctitle_available = True
except ImportError:
    _setproctitle_available = False

__ALL__ = ["MPMS", "Meta", "WorkerGracefulDie"]

VERSION = (2, 5, 0, 0)
VERSION_STR = "{}.{}.{}.{}".format(*VERSION)

logger = logging.getLogger(__name__)


class WorkerGracefulDie(Exception):
    """
    当worker函数抛出此异常时，本worker进程的所有线程都将停止接收新任务，
    等待一段时间后，此worker进程优雅退出。
    """
    pass


# Type aliases for clarity
TaskTuple = tuple[t.Any, tuple[t.Any, ...], dict[str, t.Any], float]  # (taskid, args, kwargs, enqueue_time)
WorkerFunc = t.Callable[..., t.Any]
CollectorFunc = t.Callable[['Meta', t.Any], None]
InitializerFunc = t.Callable[..., None]  # initializer function type
FinalizerFunc = t.Callable[..., None]  # finalizer function type


def _validate_config(
    processes: int | None = None,
    threads: int = 2,
    lifecycle: int | None = None,
    lifecycle_duration: float | None = None,
    lifecycle_duration_hard: float | None = None,
    subproc_check_interval: float = 3,
    worker_graceful_die_timeout: float = 5,
    **kwargs
) -> None:
    """
    验证MPMS配置参数的有效性
    
    Raises:
        ValueError: 当配置参数无效时抛出异常
    """
    errors = []
    
    # 验证进程数
    if processes is not None and (not isinstance(processes, int) or processes <= 0):
        errors.append("processes must be a positive integer")
    
    # 验证线程数
    if not isinstance(threads, int) or threads <= 0:
        errors.append("threads must be a positive integer")
    
    # 验证生命周期参数
    if lifecycle is not None and (not isinstance(lifecycle, int) or lifecycle < 0):
        errors.append("lifecycle must be a non-negative integer")
    
    if lifecycle_duration is not None and (
        not isinstance(lifecycle_duration, (int, float)) or lifecycle_duration <= 0
    ):
        errors.append("lifecycle_duration must be a positive number")
    
    if lifecycle_duration_hard is not None and (
        not isinstance(lifecycle_duration_hard, (int, float)) or lifecycle_duration_hard <= 0
    ):
        errors.append("lifecycle_duration_hard must be a positive number")
    
    # 验证检查间隔
    if not isinstance(subproc_check_interval, (int, float)) or subproc_check_interval <= 0:
        errors.append("subproc_check_interval must be a positive number")
    
    # 验证优雅退出超时
    if not isinstance(worker_graceful_die_timeout, (int, float)) or worker_graceful_die_timeout <= 0:
        errors.append("worker_graceful_die_timeout must be a positive number")
    
    if errors:
        raise ValueError(f"配置参数验证失败: {', '.join(errors)}")


def _worker_container(
    task_q: MPQueue,
    result_q: MPQueue | None,
    func: WorkerFunc,
    counter: dict[str, int],
    lifecycle: int | None,
    lifecycle_duration: float | None,
    initializer: InitializerFunc | None,
    initargs: tuple[t.Any, ...],
    finalizer: FinalizerFunc | None,
    finargs: tuple[t.Any, ...],
    graceful_die_event: threading.Event | None,
    worker_graceful_die_exceptions: tuple[type[Exception], ...]
) -> None:
    """
    Args:
        result_q (multiprocessing.Queue|None)
        initializer (InitializerFunc|None): 可选的初始化函数
        initargs (tuple): 初始化函数的参数
        finalizer (FinalizerFunc|None): 可选的清理函数
        finargs (tuple): 清理函数的参数
        graceful_die_event (threading.Event|None): 优雅退出事件标志
        worker_graceful_die_exceptions (tuple): 触发优雅退出的异常类型
    """
    _th_name = threading.current_thread().name
    if _setproctitle_available:
        try:
            setproctitle.setthreadtitle(_th_name)
        except Exception as e:
            # Log and continue if setthreadtitle fails for some reason
            logger.warning("mpms worker %s failed to set thread title: %s", _th_name, e)

    # maybe fix some logging deadlock?
    try:
        logging._after_at_fork_child_reinit_locks()
    except:
        pass
    try:
        logging._releaseLock()
    except:
        pass

    logger.debug('mpms worker %s starting', _th_name)
    
    # 调用初始化函数
    if initializer is not None:
        try:
            initializer(*initargs)
            logger.debug('mpms worker %s initialized successfully', _th_name)
        except Exception as e:
            logger.error('mpms worker %s initialization failed: %s', _th_name, repr(e), exc_info=True)
            return  # 初始化失败，退出线程
    
    # 记录线程开始时间
    start_time = time.time() if lifecycle_duration else None
    # 本地任务计数器
    local_task_count = 0

    while True:
        # 检查优雅退出事件
        if graceful_die_event and graceful_die_event.is_set():
            logger.debug('mpms worker thread %s exiting due to graceful die event', _th_name)
            break
            
        # 检查基于时间的生命周期（在获取任务前检查）
        if lifecycle_duration and time.time() - start_time >= lifecycle_duration:
            logger.debug('mpms worker thread %s reach lifecycle duration %.2fs, exit', _th_name, lifecycle_duration)
            break

        # 使用非阻塞方式获取任务，以便能够定期检查优雅退出事件
        try:
            taskid, args, kwargs, enqueue_time = task_q.get(timeout=0.5)
        except queue.Empty:
            continue
            
        # logger.debug("mpms worker %s got taskid:%s", _th_name, taskid)

        if taskid is StopIteration:
            logger.debug("mpms worker %s got stop signal", _th_name)
            break

        try:
            result = func(*args, **kwargs)
        except worker_graceful_die_exceptions as e:
            # 触发优雅退出
            logger.warning("mpms worker %s caught graceful die exception %s in taskid %s, triggering graceful die", 
                         _th_name, type(e).__name__, taskid)
            if graceful_die_event:
                graceful_die_event.set()
            # 仍然需要报告这个异常
            if result_q is not None:
                result_q.put_nowait((taskid, e))
        except Exception as e:
            logger.error("Unhandled error %s in worker thread, taskid: %s", repr(e), taskid, exc_info=True)
            if result_q is not None:
                result_q.put_nowait((taskid, e))
        else:
            # logger.debug("done %s", taskid)
            if result_q is not None:
                result_q.put_nowait((taskid, result))
        
        # 任务成功完成后才增加计数器
        local_task_count += 1
        
        # 检查基于任务计数的生命周期（在任务完成后检查）
        if lifecycle and local_task_count >= lifecycle:
            logger.debug('mpms worker thread %s reach lifecycle count %d, exit', _th_name, lifecycle)
            break

    # 调用清理函数
    if finalizer is not None:
        try:
            finalizer(*finargs)
            logger.debug('mpms worker %s finalized successfully', _th_name)
        except Exception as e:
            logger.error('mpms worker %s finalization failed: %s', _th_name, e, exc_info=True)


def _slaver(
    task_q: MPQueue,
    result_q: MPQueue | None,
    threads: int,
    func: WorkerFunc,
    lifecycle: int | None,
    lifecycle_duration: float | None,
    process_initializer: InitializerFunc | None,
    process_initargs: tuple[t.Any, ...],
    thread_initializer: InitializerFunc | None,
    thread_initargs: tuple[t.Any, ...],
    process_finalizer: FinalizerFunc | None,
    process_finargs: tuple[t.Any, ...],
    thread_finalizer: FinalizerFunc | None,
    thread_finargs: tuple[t.Any, ...],
    process_name_for_titles: str,  # Added for naming
    worker_graceful_die_timeout: float,
    worker_graceful_die_exceptions: tuple[type[Exception], ...]
) -> None:
    """
    接收与多进程任务并分发给子线程

    Args:
        task_q (multiprocessing.Queue)
        result_q (multiprocessing.Queue|None)
        threads (int)
        func (Callable)
        lifecycle (int|None): 基于任务计数的生命周期
        lifecycle_duration (float|None): 基于时间的生命周期（秒）
        process_initializer (InitializerFunc|None): 进程初始化函数
        process_initargs (tuple): 进程初始化函数的参数
        thread_initializer (InitializerFunc|None): 线程初始化函数
        thread_initargs (tuple): 线程初始化函数的参数
        process_finalizer (FinalizerFunc|None): 进程清理函数
        process_finargs (tuple): 进程清理函数的参数
        thread_finalizer (FinalizerFunc|None): 线程清理函数
        thread_finargs (tuple): 线程清理函数的参数
        process_name_for_titles (str): Base name for process and thread titles.
        worker_graceful_die_timeout (float): 优雅退出超时时间（秒）
        worker_graceful_die_exceptions (tuple): 触发优雅退出的异常类型
    """
    # Set process title using setproctitle if available
    # multiprocessing.current_process().name is already set by the 'name' argument in Process()
    # process_name_for_titles is passed to ensure consistency and for logging.
    if _setproctitle_available:
        try:
            setproctitle.setproctitle(process_name_for_titles)
        except Exception as e:
            logger.warning("mpms subprocess %s failed to set process title: %s", process_name_for_titles, e)

    logger.debug("mpms subprocess %s (PID:%s) start. threads:%s", 
                 process_name_for_titles, multiprocessing.current_process().pid, threads)
    
    # 调用进程初始化函数
    if process_initializer is not None:
        try:
            process_initializer(*process_initargs)
            logger.debug('mpms subprocess %s initialized successfully', process_name_for_titles)
        except Exception as e:
            logger.error('mpms subprocess %s initialization failed: %s', process_name_for_titles, e, exc_info=True)
            return  # 初始化失败，退出进程

    # 创建优雅退出事件
    graceful_die_event = threading.Event()
    
    pool = []
    for i in range(threads):
        thread_name = f"{process_name_for_titles}#t{i + 1}"
        th = threading.Thread(target=_worker_container,
                              args=(task_q, result_q, func, None, lifecycle, lifecycle_duration, 
                                    thread_initializer, thread_initargs, thread_finalizer, thread_finargs,
                                    graceful_die_event, worker_graceful_die_exceptions),
                              name=thread_name
                              )
        th.daemon = True
        pool.append(th)
    for th in pool:
        th.start()

    # 监控优雅退出事件
    graceful_die_triggered_time = None
    while True:
        # 检查是否所有线程都已退出
        all_threads_dead = not any(th.is_alive() for th in pool)
        
        # 如果优雅退出事件被触发
        if graceful_die_event.is_set():
            if graceful_die_triggered_time is None:
                graceful_die_triggered_time = time.time()
                logger.warning('mpms subprocess %s graceful die triggered, waiting %.2fs before exit', 
                             process_name_for_titles, worker_graceful_die_timeout)
            
            # 检查优雅退出超时
            if time.time() - graceful_die_triggered_time >= worker_graceful_die_timeout:
                logger.warning('mpms subprocess %s graceful die timeout reached, exiting', process_name_for_titles)
                # 强制退出进程
                os._exit(1)
        
        # 如果没有触发优雅退出且所有线程都退出了，可以正常退出
        if all_threads_dead and graceful_die_triggered_time is None:
            break
        
        # 短暂休眠以避免忙等待
        time.sleep(0.1)

    # 调用进程清理函数
    if process_finalizer is not None:
        try:
            process_finalizer(*process_finargs)
            logger.debug('mpms subprocess %s finalized successfully', process_name_for_titles)
        except Exception as e:
            logger.error('mpms subprocess %s finalization failed: %s', process_name_for_titles, e, exc_info=True)

    logger.debug("mpms subprocess %s exiting", process_name_for_titles)


def get_cpu_count() -> int:
    try:
        if hasattr(os, "cpu_count"):
            return os.cpu_count()
        else:
            return multiprocessing.cpu_count()
    except:
        return 0


class Meta(dict[str, t.Any]):
    """
    用于存储单次任务信息以供 collector 使用

    Args:
        mpms (MPMS): 此task对应的 MPMS 实例
        args (tuple): 此任务的 args, 对应 .put() 的 args
        kwargs (dict): 此任务的 kwargs, 对应 .put() 的 kwargs
        taskid (str): 一个自动生成的 taskid

    Notes:
        除了上面的参数以外, 还可以用 meta['name'] 来存取任意自定义参数,
            行为就跟一个普通的dict一样
        可以用于在主程序中传递一些环境变量给 collector
    """

    if TYPE_CHECKING:
        mpms: weakref.ProxyType['MPMS']
    else:
        mpms: t.Any
    args: tuple[t.Any, ...]
    kwargs: dict[str, t.Any]
    taskid: str | None

    def __init__(self, mpms: 'MPMS') -> None:
        super(Meta, self).__init__()
        self.mpms = weakref.proxy(mpms)  # type: MPMS
        self.args = ()
        self.kwargs = {}
        self.taskid = None

    @property
    def self(self) -> 'MPMS':
        """
        an alias for .mpms

        Returns:
            MPMS
        """
        return self.mpms


class MPMS(object):
    """
    简易的多进程-多线程任务队列

    Examples:
        # 完整用例请看 demo.py
        def worker(index, t=None):
            return 'foo', 'bar'

        def collector(meta, result):
            if isinstance(result, Exception): # 当worker出错时的exception会在result中返回
                return
            foo, bar = result
            
        def process_init():
            # 在每个进程启动时执行
            print(f"Process {os.getpid()} initialized")
            
        def thread_init(process_name):
            # 在每个线程启动时执行
            print(f"Thread {threading.current_thread().name} in process {process_name} initialized")
            
        def process_cleanup():
            # 在每个进程退出前执行
            print(f"Process {os.getpid()} cleaning up")
            
        def thread_cleanup(process_name):
            # 在每个线程退出前执行
            print(f"Thread {threading.current_thread().name} in process {process_name} cleaning up")

        def main():
            m = MPMS(
                worker,
                collector, # optional
                processes=2, threads=2, # optional
                process_initializer=process_init,  # optional
                thread_initializer=thread_init,  # optional
                thread_initargs=(multiprocessing.current_process().name,),  # optional
                process_finalizer=process_cleanup,  # optional
                thread_finalizer=thread_cleanup,  # optional
                thread_finargs=(multiprocessing.current_process().name,),  # optional
                worker_graceful_die_timeout=10,  # optional
                worker_graceful_die_exceptions=(WorkerGracefulDie, MemoryError),  # optional
                )
            m.start()
            for i in range(100):
                m.put(i, 2333)
            m.join()
        
        # 使用iter_results的例子
        def main_with_iter():
            m = MPMS(worker, processes=2, threads=2)
            m.start()
            
            # 提交任务
            for i in range(100):
                m.put(i, t=i*2)
            
            # 关闭任务队列
            m.close()
            
            # 迭代获取结果
            for meta, result in m.iter_results():
                if isinstance(result, Exception):
                    print(f"任务 {meta.taskid} 失败: {result}")
                else:
                    foo, bar = result
                    print(f"任务 {meta.taskid} 成功: foo={foo}, bar={bar}")
            
            # 等待所有进程结束
            m.join(close=False)  # 已经close过了

        if __name__ == '__main__':
            main()

    Args:
        worker (Callable): 工作函数
        collector (Callable[[Meta, Any], None]): 结果处理函数, 可选
        processes (int): 进程数, 若不指定则为CPU核心数
        threads (int): 每个进程下多少个线程
        meta (Meta|dict): meta信息, 请看上面 Meta 的说明
        lifecycle (int|None): 基于任务计数的生命周期，工作线程处理指定数量任务后退出
        lifecycle_duration (float|None): 基于时间的生命周期（秒），工作线程运行指定时间后退出
        lifecycle_duration_hard (float|None): 进程和任务的硬性时间限制（秒），超时将被强制终止
        subproc_check_interval (float): 子进程检查间隔（秒）
        process_initializer (Callable|None): 进程初始化函数，在每个工作进程启动时调用一次
        process_initargs (tuple): 传递给进程初始化函数的参数
        thread_initializer (Callable|None): 线程初始化函数，在每个工作线程启动时调用一次
        thread_initargs (tuple): 传递给线程初始化函数的参数
        process_finalizer (Callable|None): 进程清理函数，在每个工作进程退出前调用一次
        process_finargs (tuple): 传递给进程清理函数的参数
        thread_finalizer (Callable|None): 线程清理函数，在每个工作线程退出前调用一次
        thread_finargs (tuple): 传递给线程清理函数的参数
        name (str|None): Optional base name for processes and threads. Defaults to worker function's name.
        worker_graceful_die_timeout (float): 优雅退出超时时间（秒），默认为5秒
        worker_graceful_die_exceptions (tuple[type[Exception], ...]): 触发优雅退出的异常类型，默认为 (WorkerGracefulDie,)

        total_count (int): 总任务数
        finish_count (int): 已完成的任务数

    Notes:
        lifecycle 和 lifecycle_duration 同时生效，满足任一条件都会触发工作线程退出
        lifecycle_duration_hard 提供了进程和任务的硬性时间限制：
            - 当进程运行时间超过此限制时，主进程会强制终止该进程
            - 当任务运行时间超过此限制时，该任务会被标记为超时错误
            - 这是为了防止任务hang死导致worker无法接收新任务的情况
        worker_graceful_die_exceptions 定义了触发优雅退出的异常类型：
            - 当worker函数抛出这些异常时，进程会停止接收新任务
            - 等待 worker_graceful_die_timeout 秒后，进程会优雅退出
            - 这允许worker主动标记自己为不健康状态
        如果初始化函数抛出异常，对应的进程或线程将退出，不会处理任何任务
        清理函数在进程或线程正常退出前调用（包括因生命周期限制而退出的情况）
        如果清理函数抛出异常，将记录错误日志但不影响退出流程

    """

    worker: WorkerFunc
    collector: CollectorFunc | None
    processes_count: int
    threads_count: int
    total_count: int
    finish_count: int
    processes_pool: dict[str, t.Any]  # Not used in the code, might be legacy
    task_queue_maxsize: int
    task_queue_closed: bool
    meta: Meta
    task_q: MPQueue
    _process_count: int
    result_q: MPQueue
    collector_thread: threading.Thread | None
    worker_processes_pool: dict[str, multiprocessing.Process]
    worker_processes_start_time: dict[str, float]  # 记录每个进程的启动时间
    running_tasks: dict[str, TaskTuple]
    lifecycle: int | None
    lifecycle_duration: float | None
    lifecycle_duration_hard: float | None
    subproc_check_interval: float
    _subproc_last_check: float
    process_initializer: InitializerFunc | None
    process_initargs: tuple[t.Any, ...]
    thread_initializer: InitializerFunc | None
    thread_initargs: tuple[t.Any, ...]
    process_finalizer: FinalizerFunc | None
    process_finargs: tuple[t.Any, ...]
    thread_finalizer: FinalizerFunc | None
    thread_finargs: tuple[t.Any, ...]
    name: str # Base name for processes/threads
    worker_graceful_die_timeout: float
    worker_graceful_die_exceptions: tuple[type[Exception], ...]
    _use_iter_results: bool

    def __init__(
            self,
            worker: WorkerFunc,
            collector: CollectorFunc | None = None,
            processes: int | None = None,
            threads: int = 2,
            task_queue_maxsize: int | None = None,
            meta: Meta | dict[str, t.Any] | None = None,
            lifecycle: int | None = None,
            lifecycle_duration: float | None = None,
            lifecycle_duration_hard: float | None = None,
            subproc_check_interval: float = 3,
            process_initializer: InitializerFunc | None = None,
            process_initargs: tuple[t.Any, ...] = (),
            thread_initializer: InitializerFunc | None = None,
            thread_initargs: tuple[t.Any, ...] = (),
            process_finalizer: FinalizerFunc | None = None,
            process_finargs: tuple[t.Any, ...] = (),
            thread_finalizer: FinalizerFunc | None = None,
            thread_finargs: tuple[t.Any, ...] = (),
            name: str | None = None,
            worker_graceful_die_timeout: float = 5,
            worker_graceful_die_exceptions: tuple[type[Exception], ...] = (WorkerGracefulDie,),
    ) -> None:

        # 验证配置参数
        _validate_config(
            processes=processes,
            threads=threads,
            lifecycle=lifecycle,
            lifecycle_duration=lifecycle_duration,
            lifecycle_duration_hard=lifecycle_duration_hard,
            subproc_check_interval=subproc_check_interval,
            worker_graceful_die_timeout=worker_graceful_die_timeout
        )

        self.worker = worker
        self.collector = collector

        if name is None:
            try:
                # Attempt to create a descriptive name from the worker function
                self.name = f"{worker.__module__}.{worker.__name__}"
            except AttributeError:
                # Fallback if worker is not a standard function (e.g., a lambda or callable class)
                self.name = "mpms_worker"
        else:
            self.name = name
        
        self.processes_count = processes or get_cpu_count() or 1
        self.threads_count = threads

        self.total_count = 0  # 总任务数
        self.finish_count = 0  # 已完成的任务数

        self.processes_pool = {}

        self.task_queue_maxsize = max(self.processes_count * self.threads_count * 3 + 30, task_queue_maxsize or 0)
        self.task_queue_closed = False

        self.meta = Meta(self)
        if meta is not None:
            self.meta.update(meta)

        self.task_q = multiprocessing.Queue(maxsize=self.task_queue_maxsize)
        self._process_count = 0

        # 始终创建result_q，以支持collector和iter_results两种模式
        self.result_q = multiprocessing.Queue()

        self.collector_thread = None

        self.worker_processes_pool = {}
        self.worker_processes_start_time = {}

        self.running_tasks = {}
        self.lifecycle = lifecycle
        self.lifecycle_duration = lifecycle_duration
        self.lifecycle_duration_hard = lifecycle_duration_hard
        self.subproc_check_interval = subproc_check_interval
        self._subproc_last_check = time.time()
        
        # 初始化函数相关属性
        self.process_initializer = process_initializer
        self.process_initargs = process_initargs
        self.thread_initializer = thread_initializer
        self.thread_initargs = thread_initargs
        self.process_finalizer = process_finalizer
        self.process_finargs = process_finargs
        self.thread_finalizer = thread_finalizer
        self.thread_finargs = thread_finargs
        self.worker_graceful_die_timeout = worker_graceful_die_timeout
        self.worker_graceful_die_exceptions = worker_graceful_die_exceptions

        self._taskid_lock = threading.Lock()
        self._process_management_lock = threading.Lock()
        self._atomic_counter = 0
        self._use_iter_results = False  # 标识是否使用迭代器模式获取结果

    def _start_one_slaver_process(self) -> None:
        with self._process_management_lock:
            self._process_count += 1
            # Use self.name for a more descriptive process name
            process_base_name = f"mpms-{self.name}-p{self._process_count}"
            p = multiprocessing.Process(
                target=_slaver,
                args=(self.task_q, self.result_q,
                      self.threads_count, self.worker,
                      self.lifecycle, self.lifecycle_duration,
                      self.process_initializer, self.process_initargs,
                      self.thread_initializer, self.thread_initargs,
                      self.process_finalizer, self.process_finargs,
                      self.thread_finalizer, self.thread_finargs,
                      process_base_name,  # Pass the base name for titles
                      self.worker_graceful_die_timeout,
                      self.worker_graceful_die_exceptions),
                name=process_base_name  # Set the process name itself
            )
            p.daemon = True
            logger.debug('mpms subprocess %s starting', process_base_name)
            p.start()
            self.worker_processes_pool[process_base_name] = p
            self.worker_processes_start_time[process_base_name] = time.time()

    def _subproc_check(self) -> None:
        with self._process_management_lock:
            if time.time() - self._subproc_last_check < self.subproc_check_interval:
                return
            self._subproc_last_check = time.time()
            
            current_time = time.time()
            
            # 检查任务超时
            if self.lifecycle_duration_hard and (self.collector or self._use_iter_results):
                for taskid, task_tuple in list(self.running_tasks.items()):
                    _, args, kwargs, enqueue_time = task_tuple
                    if current_time - enqueue_time > self.lifecycle_duration_hard:
                        logger.warning('mpms task %s timeout after %.2fs, marking as error', 
                                     taskid, current_time - enqueue_time)
                        # 将超时任务标记为错误
                        timeout_error = TimeoutError(f'Task {taskid} timeout after {current_time - enqueue_time:.2f}s')
                        if self.result_q is not None:
                            self.result_q.put_nowait((taskid, timeout_error))
                        # 不从 running_tasks 中删除，让 collector 或 iter_results 处理
            
            # 检查进程状态和超时，收集需要处理的进程
            processes_to_remove = []
            need_restart = False
            
            for name, p in tuple(self.worker_processes_pool.items()):  # type:str, multiprocessing.Process
                process_start_time = self.worker_processes_start_time.get(name, 0)
                process_age = current_time - process_start_time if process_start_time else 0
                
                # 检查进程是否需要因为硬性超时而被杀死
                if self.lifecycle_duration_hard and process_age > self.lifecycle_duration_hard:
                    logger.warning('mpms subprocess %s exceeded hard timeout %.2fs, terminating', 
                                 name, self.lifecycle_duration_hard)
                    p.terminate()
                    p.join(timeout=1)  # 等待1秒
                    if p.is_alive():
                        p.kill()  # 如果还活着就强制杀死
                    p.close()
                    processes_to_remove.append(name)
                    need_restart = True
                elif not p.is_alive():
                    # 进程已死亡的正常处理
                    logger.info('mpms subprocess %s dead, restarting', name)
                    p.terminate()
                    p.close()
                    processes_to_remove.append(name)
                    need_restart = True
            
            # 清理已终止的进程
            for name in processes_to_remove:
                del self.worker_processes_pool[name]
                if name in self.worker_processes_start_time:
                    del self.worker_processes_start_time[name]
            
        # 如果有进程被终止，可能需要修复日志锁
        if processes_to_remove:
            # maybe fix some logging deadlock?
            try:
                logging._after_at_fork_child_reinit_locks()
            except:
                pass
            try:
                logging._releaseLock()
            except:
                pass
            
            # 如果任务队列已关闭，为新进程发送停止信号
            if self.task_queue_closed:
                for _ in range(len(processes_to_remove) * self.threads_count):
                    self.task_q.put((StopIteration, (), {}, 0.0))
            
        # 根据需要启动新进程
        if need_restart and not self.task_queue_closed:
            # 计算需要的进程数
            current_process_count = len(self.worker_processes_pool)
            needed_process_count = min(
                self.processes_count,  # 不超过配置的最大进程数
                # 根据待处理任务数计算需要的进程数
                (len(self.running_tasks) + self.threads_count - 1) // self.threads_count
            )
            
            # 启动新进程
            for _ in range(needed_process_count - current_process_count):
                if len(self.worker_processes_pool) < self.processes_count:
                    time.sleep(0.1)
                    self._start_one_slaver_process()
                    time.sleep(0.1)

    def start(self) -> None:
        if self.worker_processes_pool:
            raise RuntimeError('You can only start ONCE!')
        logger.debug("mpms starting worker subprocess")

        for i in range(self.processes_count):
            self._start_one_slaver_process()

        if self.collector is not None:
            logger.debug("mpms starting collector thread")
            collector_thread_name = f"mpms-{self.name}-collector"
            self.collector_thread = threading.Thread(target=self._collector_container, name=collector_thread_name)
            self.collector_thread.daemon = True
            self.collector_thread.start()
        else:
            logger.debug("mpms no collector given, skip collector thread")

    def put(self, *args: t.Any, **kwargs: t.Any) -> None:
        """
        put task params into working queue

        """

        if not self.worker_processes_pool:
            raise RuntimeError('you must call .start() before put')
        if self.task_queue_closed:
            raise RuntimeError('you cannot put after task_queue closed')

        taskid = self._gen_taskid()
        task_tuple = (taskid, args, kwargs, time.time())
        
        # 只有在需要时才记录running_tasks（有collector或可能使用iter_results）
        if self.collector or not self.task_queue_closed:
            # 如果有collector，或者任务队列还没关闭（可能会使用iter_results），则记录
            self.running_tasks[taskid] = task_tuple

        self._subproc_check()

        while True:
            try:
                self.task_q.put(task_tuple, timeout=self.subproc_check_interval)
            except queue.Full:
                self._subproc_check()
            else:
                break
        
        with self._taskid_lock:
            self.total_count += 1

    def join(self, close: bool = True) -> None:
        """
        Wait until the works and handlers terminates.

        """
        if close and not self.task_queue_closed:  # 注意: 如果此处不close, 则一定需要在其他地方close, 否则无法结束
            self.close()

        # 等待所有工作进程结束，同时定期检查进程状态
        while self.worker_processes_pool:
            # 调用 _subproc_check 来检查进程超时
            self._subproc_check()
            
            # 检查是否有进程已经结束
            for name, p in list(self.worker_processes_pool.items()):  # type: multiprocessing.Process
                if not p.is_alive():
                    p.join(timeout=0.1)
                    logger.debug("mpms subprocess %s %s closed", p.name, p.pid)
                    del self.worker_processes_pool[name]
                    if name in self.worker_processes_start_time:
                        del self.worker_processes_start_time[name]
            
            # 如果还有进程在运行，稍等一下再检查
            if self.worker_processes_pool:
                time.sleep(0.1)
                
        logger.debug("mpms all worker completed")

        if self.collector:
            self.result_q.put_nowait((StopIteration, None))  # 在结果队列中加入退出指示信号
            self.collector_thread.join()  # 等待处理线程结束

        logger.debug("mpms join completed")

    def _gen_taskid(self) -> str:
        """
        线程安全的taskid生成
        """
        with self._taskid_lock:
            self._atomic_counter += 1
            # 使用时间戳和计数器确保唯一性
            timestamp = int(time.time() * 1000000)  # 微秒时间戳
            return f"mpms-{timestamp}-{self._atomic_counter}"

    def _collector_container(self) -> None:
        """
        接受子进程传入的结果,并把它发送到master_product_handler()中

        """
        _th_name = threading.current_thread().name
        if _setproctitle_available:
            try:
                setproctitle.setthreadtitle(_th_name)
            except Exception as e:
                logger.warning("mpms collector %s failed to set thread title: %s", _th_name, e)
        
        logger.debug("mpms collector %s start", _th_name)

        while True:
            taskid, result = self.result_q.get()

            if taskid is StopIteration:
                logger.debug("mpms collector got stop signal")
                break

            _, self.meta.args, self.meta.kwargs, _ = self.running_tasks.pop(taskid)
            self.meta.taskid = taskid
            self.finish_count += 1

            try:
                self.collector(self.meta, result)
            except:
                # 为了继续运行, 不抛错
                logger.error("an error occurs in collector, task: %s", taskid, exc_info=True)

            # 移除meta中已经使用过的字段
            self.meta.taskid, self.meta.args, self.meta.kwargs = None, (), {}

    def close(self) -> None:
        """
        Close task queue
        """

        # 在任务队列尾部加入结束信号来关闭任务队列
        for i in range(self._process_count * self.threads_count):
            self.task_q.put((StopIteration, (), {}, 0.0))
        self.task_queue_closed = True

    def iter_results(self, timeout: float | None = None) -> t.Iterator[tuple[Meta, t.Any]]:
        """
        迭代器方式获取任务执行结果
        
        Args:
            timeout: 获取单个结果的超时时间（秒）。如果为None，将一直等待。
            
        Yields:
            tuple[Meta, Any]: (meta信息, 执行结果) 的元组
            
        Raises:
            RuntimeError: 如果同时使用了collector或在start之前调用
            
        Examples:
            >>> m = MPMS(worker_func)
            >>> m.start()
            >>> for i in range(10):
            ...     m.put(i)
            >>> # 可以在close()之前或之后调用iter_results
            >>> for meta, result in m.iter_results():
            ...     if isinstance(result, Exception):
            ...         print(f"Task {meta.taskid} failed: {result}")
            ...     else:
            ...         print(f"Task {meta.taskid} result: {result}")
            >>> m.close()  # 可选，也可以在iter_results之前调用
        
        Notes:
            - 不能与collector同时使用
            - 可以在close()之前或之后调用
            - 当所有任务完成后，迭代器自动结束
            - 如果worker抛出异常，result将是该异常对象
            - 如果在任务队列未关闭时调用，迭代器会等待新的结果
        """
        # 检查是否可以使用iter_results
        if self.collector is not None:
            raise RuntimeError("不能同时使用collector和iter_results，请选择其中一种方式处理结果")
        
        if not self.worker_processes_pool:
            raise RuntimeError("必须先调用start()方法启动工作进程")
        
        # 设置使用迭代器模式
        self._use_iter_results = True
        
        # 创建一个新的Meta实例用于返回结果
        result_meta = Meta(self)
        if self.meta:
            # 复制用户自定义的meta信息
            for key, value in self.meta.items():
                if key not in ('args', 'kwargs', 'taskid'):
                    result_meta[key] = value
        
        # 迭代获取结果
        while True:
            # 检查是否应该继续等待结果
            # 如果任务队列已关闭且没有运行中的任务，且已完成所有任务，则退出
            if (self.task_queue_closed and 
                not self.running_tasks and 
                self.finish_count >= self.total_count):
                break
                
            try:
                if timeout is None:
                    taskid, result = self.result_q.get()
                else:
                    taskid, result = self.result_q.get(timeout=timeout)
                    
                if taskid is StopIteration:
                    # 忽略停止信号，继续处理剩余结果
                    continue
                    
                # 从running_tasks获取任务信息
                if taskid in self.running_tasks:
                    _, args, kwargs, _ = self.running_tasks.pop(taskid)
                    result_meta.taskid = taskid
                    result_meta.args = args
                    result_meta.kwargs = kwargs
                    self.finish_count += 1
                    
                    yield result_meta, result
                    
                    # 清理meta信息，为下一次迭代准备
                    result_meta.taskid = None
                    result_meta.args = ()
                    result_meta.kwargs = {}
                else:
                    # 任务已经被处理（可能是超时任务）
                    logger.warning("收到未知任务ID的结果: %s", taskid)
                    
            except queue.Empty:
                # 超时了，检查进程状态
                self._subproc_check()
                # 如果任务队列已关闭且没有运行中的任务，且已完成所有任务，则退出
                if (self.task_queue_closed and 
                    not self.running_tasks and 
                    self.finish_count >= self.total_count):
                    break
                    
        logger.debug("mpms iter_results completed, total: %d, finished: %d", 
                    self.total_count, self.finish_count)
