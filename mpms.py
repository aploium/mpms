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

__ALL__ = ["MPMS", "Meta"]

VERSION = (2, 2, 0, 0)
VERSION_STR = "{}.{}.{}.{}".format(*VERSION)

logger = logging.getLogger(__name__)

# Type aliases for clarity
TaskTuple = tuple[t.Any, tuple[t.Any, ...], dict[str, t.Any]]  # (taskid, args, kwargs)
WorkerFunc = t.Callable[..., t.Any]
CollectorFunc = t.Callable[['Meta', t.Any], None]
InitializerFunc = t.Callable[..., None]  # initializer function type


def _worker_container(
    task_q: MPQueue,
    result_q: MPQueue | None,
    func: WorkerFunc,
    counter: dict[str, int],
    lifecycle: int | None,
    lifecycle_duration: float | None,
    initializer: InitializerFunc | None,
    initargs: tuple[t.Any, ...]
) -> None:
    """
    Args:
        result_q (multiprocessing.Queue|None)
        initializer (InitializerFunc|None): 可选的初始化函数
        initargs (tuple): 初始化函数的参数
    """
    _th_name = threading.current_thread().name

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
            logger.error('mpms worker %s initialization failed: %s', _th_name, e, exc_info=True)
            return  # 初始化失败，退出线程
    
    # 记录线程开始时间
    start_time = time.time() if lifecycle_duration else None
    # 本地任务计数器
    local_task_count = 0

    while True:
        # 检查基于时间的生命周期（在获取任务前检查）
        if lifecycle_duration and time.time() - start_time >= lifecycle_duration:
            logger.debug('mpms worker thread %s reach lifecycle duration %.2fs, exit', _th_name, lifecycle_duration)
            break

        taskid, args, kwargs = task_q.get()
        # logger.debug("mpms worker %s got taskid:%s", _th_name, taskid)

        if taskid is StopIteration:
            logger.debug("mpms worker %s got stop signal", _th_name)
            break

        try:
            result = func(*args, **kwargs)
        except Exception as e:
            logger.error("Unhandled error %s in worker thread, taskid: %s", e, taskid, exc_info=True)
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
    thread_initargs: tuple[t.Any, ...]
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
    """
    _process_name = "{}(PID:{})".format(multiprocessing.current_process().name,
                                        multiprocessing.current_process().pid, )
    logger.debug("mpms subprocess %s start. threads:%s", _process_name, threads)
    
    # 调用进程初始化函数
    if process_initializer is not None:
        try:
            process_initializer(*process_initargs)
            logger.debug('mpms subprocess %s initialized successfully', _process_name)
        except Exception as e:
            logger.error('mpms subprocess %s initialization failed: %s', _process_name, e, exc_info=True)
            return  # 初始化失败，退出进程

    pool = []
    for i in range(threads):
        th = threading.Thread(target=_worker_container,
                              args=(task_q, result_q, func, None, lifecycle, lifecycle_duration, 
                                    thread_initializer, thread_initargs),
                              name="{}#{}".format(_process_name, i + 1)
                              )
        th.daemon = True
        pool.append(th)
    for th in pool:
        th.start()

    for th in pool:
        th.join()

    logger.debug("mpms subprocess %s exiting", _process_name)


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

        def main():
            m = MPMS(
                worker,
                collector, # optional
                processes=2, threads=2, # optional
                process_initializer=process_init,  # optional
                thread_initializer=thread_init,  # optional
                thread_initargs=(multiprocessing.current_process().name,)  # optional
                )
            m.start()
            for i in range(100):
                m.put(i, 2333)
            m.join()

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
        subproc_check_interval (float): 子进程检查间隔（秒）
        process_initializer (Callable|None): 进程初始化函数，在每个工作进程启动时调用一次
        process_initargs (tuple): 传递给进程初始化函数的参数
        thread_initializer (Callable|None): 线程初始化函数，在每个工作线程启动时调用一次
        thread_initargs (tuple): 传递给线程初始化函数的参数

        total_count (int): 总任务数
        finish_count (int): 已完成的任务数

    Notes:
        lifecycle 和 lifecycle_duration 同时生效，满足任一条件都会触发工作线程退出
        如果初始化函数抛出异常，对应的进程或线程将退出，不会处理任何任务

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
    result_q: MPQueue | None
    collector_thread: threading.Thread | None
    worker_processes_pool: dict[str, multiprocessing.Process]
    running_tasks: dict[str, TaskTuple]
    lifecycle: int | None
    lifecycle_duration: float | None
    subproc_check_interval: float
    _subproc_last_check: float
    process_initializer: InitializerFunc | None
    process_initargs: tuple[t.Any, ...]
    thread_initializer: InitializerFunc | None
    thread_initargs: tuple[t.Any, ...]

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
            subproc_check_interval: float = 3,
            process_initializer: InitializerFunc | None = None,
            process_initargs: tuple[t.Any, ...] = (),
            thread_initializer: InitializerFunc | None = None,
            thread_initargs: tuple[t.Any, ...] = (),
    ) -> None:

        self.worker = worker
        self.collector = collector

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

        if self.collector:
            self.result_q = multiprocessing.Queue()
        else:
            self.result_q = None

        self.collector_thread = None

        self.worker_processes_pool = {}

        self.running_tasks = {}
        self.lifecycle = lifecycle
        self.lifecycle_duration = lifecycle_duration
        self.subproc_check_interval = subproc_check_interval
        self._subproc_last_check = time.time()
        
        # 初始化函数相关属性
        self.process_initializer = process_initializer
        self.process_initargs = process_initargs
        self.thread_initializer = thread_initializer
        self.thread_initargs = thread_initargs

    def _start_one_slaver_process(self) -> None:
        self._process_count += 1
        name = "mpms-{}".format(self._process_count)
        p = multiprocessing.Process(
            target=_slaver,
            args=(self.task_q, self.result_q,
                  self.threads_count, self.worker,
                  self.lifecycle, self.lifecycle_duration,
                  self.process_initializer, self.process_initargs,
                  self.thread_initializer, self.thread_initargs
                  ),
            name=name
        )
        p.daemon = True
        logger.debug('mpms subprocess %s starting', name)
        p.start()
        self.worker_processes_pool[name] = p

    def _subproc_check(self) -> None:
        if time.time() - self._subproc_last_check < self.subproc_check_interval:
            return
        self._subproc_last_check = time.time()
        for name, p in tuple(self.worker_processes_pool.items()):  # type:str, multiprocessing.Process
            if p.is_alive():
                continue
            logger.info('mpms subprocess %s dead, restarting', name)
            p.terminate()
            p.close()
            del self.worker_processes_pool[name]
            if len(self.running_tasks) <= len(self.processes_pool):
                continue
            time.sleep(0.1)
            self._start_one_slaver_process()
            time.sleep(0.1)

            # maybe fix some logging deadlock?
            try:
                logging._after_at_fork_child_reinit_locks()
            except:
                pass
            try:
                logging._releaseLock()
            except:
                pass
            if self.task_queue_closed:
                for _ in range(self.threads_count):
                    self.task_q.put((StopIteration, (), {}))
            break

    def start(self) -> None:
        if self.worker_processes_pool:
            raise RuntimeError('You can only start ONCE!')
        logger.debug("mpms starting worker subprocess")

        for i in range(self.processes_count):
            self._start_one_slaver_process()

        if self.collector is not None:
            logger.debug("mpms starting collector thread")
            self.collector_thread = threading.Thread(target=self._collector_container, name='mpms-collector')
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
        task_tuple = (taskid, args, kwargs)
        if self.collector:
            self.running_tasks[taskid] = task_tuple

        self._subproc_check()

        while True:
            try:
                self.task_q.put(task_tuple, timeout=self.subproc_check_interval)
            except queue.Full:
                self._subproc_check()
            else:
                break
        self.total_count += 1

    def join(self, close: bool = True) -> None:
        """
        Wait until the works and handlers terminates.

        """
        if close and not self.task_queue_closed:  # 注意: 如果此处不close, 则一定需要在其他地方close, 否则无法结束
            self.close()

        # 等待所有工作进程结束
        while self.worker_processes_pool:
            for name, p in list(self.worker_processes_pool.items()):  # type: multiprocessing.Process
                p.join()
                logger.debug("mpms subprocess %s %s closed", p.name, p.pid)
                del self.worker_processes_pool[name]
        logger.debug("mpms all worker completed")

        if self.collector:
            self.result_q.put_nowait((StopIteration, None))  # 在结果队列中加入退出指示信号
            self.collector_thread.join()  # 等待处理线程结束

        logger.debug("mpms join completed")

    def _gen_taskid(self) -> str:
        return "mpms{}".format(self.total_count)

    def _collector_container(self) -> None:
        """
        接受子进程传入的结果,并把它发送到master_product_handler()中

        """
        logger.debug("mpms collector start")

        while True:
            taskid, result = self.result_q.get()

            if taskid is StopIteration:
                logger.debug("mpms collector got stop signal")
                break

            _, self.meta.args, self.meta.kwargs = self.running_tasks.pop(taskid)
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
            self.task_q.put((StopIteration, (), {}))
        self.task_queue_closed = True
