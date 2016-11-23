# coding=utf-8
"""
Do parallel python works easily in multithreads in multiprocesses (at least up to 1000 or 2000 total threads!)
多进程 多线程作业框架
至少支持几千个线程
author: aploium@aploium.com

"""

import multiprocessing
import os
import threading
import traceback

try:
    from typing import Callable, Any
except:
    pass

__ALL__ = ["MultiProcessesMultiThreads"]


def _dummy_handler(*args, **kwargs):
    pass


def _producer_multi_threads(queue_task, queue_product, worker_function):
    """
    负责在本进程内分发多线程任务
    :type queue_task: multiprocessing.JoinableQueue
    :type queue_product: multiprocessing.JoinableQueue
    :type worker_function: Callable[[Any], Any]
    """
    while True:
        try:
            task = queue_task.get()
            if isinstance(task, _QueueEndSignal):  # 结束信号
                # finally 里的 task_done() 在break的情况下仍然会被执行
                break
            if isinstance(task, dict):
                result = worker_function(**task)
            elif isinstance(task, (tuple, list)):
                result = worker_function(*task)
            else:
                result = worker_function(task)

            queue_product.put((task, result))
        except:
            traceback.print_exc()
        finally:
            queue_task.task_done()


def _producer_multi_processes(queue_task,
                              queue_product,
                              threads_per_process,
                              worker_function):
    """
    接收与多进程任务并分发给子线程

    :type queue_task: multiprocessing.JoinableQueue
    :type queue_product: multiprocessing.JoinableQueue
    :type threads_per_process: int
    :type worker_function: Callable[[Any], Any]
    """

    pool = [threading.Thread(target=_producer_multi_threads, args=(queue_task, queue_product, worker_function))
            for _ in range(threads_per_process)]
    for t in pool:
        t.daemon = True
        t.start()

    # 等待所有子线程结束
    for t in pool:
        # dbgprint('子线程结束', t.name, multiprocessing.current_process().name, v=4)
        t.join()
        del t

    os._exit(1)  # 暴力退出子进程


class _QueueEndSignal:
    def __init__(self):
        pass


class ParamTransfer(dict):
    def __init__(self, mpmt):
        super(ParamTransfer, self).__init__()
        assert isinstance(mpmt, MultiProcessesMultiThreads)
        self.mpmt = mpmt  # type: MultiProcessesMultiThreads
        self._thread_local = threading.local()
        self._thread_local.cycle = {}
        self._thread_local.task = {}

    @property
    def self(self):
        """
        :rtype: MultiProcessesMultiThreads
        """
        return self.mpmt

    @property
    def cycle(self):
        """
        :rtype: dict
        """
        if not hasattr(self._thread_local, "cycle"):
            self._thread_local.cycle = {}
        return self._thread_local.cycle

    @property
    def task(self):
        """
        :rtype: Union[list, dict]
        """
        return self._thread_local.task

    @task.setter
    def task(self, value):
        self._thread_local.task = value

    def __setitem__(self, key, value):
        if key == "task":
            self._thread_local.task = value
            return

        if key == "self":
            self.mpmt = value
            return

        super(ParamTransfer, self).__setitem__(key, value)

    def __getitem__(self, key):
        if key == "task":
            return self._thread_local.task

        if key == "self":
            return self.mpmt

        return super(ParamTransfer, self).__getitem__(key)

    def __delitem__(self, key):
        if key in ("task", "self"):
            raise ValueError("You cannot delete the '{}' key".format(key))
        super(ParamTransfer, self).__delitem__(key)


class MultiProcessesMultiThreads:
    """
    provide an simple high-level multi-processes_count-multi-threads work environment
    """

    def _product_receiver(self):
        """
        接受子进程传入的结果,并把它发送到master_product_handler()中

        """

        while True:
            try:
                task, product = self.product_queue.get()
            except:
                self.product_queue.task_done()
                traceback.print_exc()
                continue

            if isinstance(product, _QueueEndSignal):
                self.product_queue.task_done()
                break

            try:
                self.meta.task = task

                # lifecycle 控制
                self.current_handler_lifecycle_count += 1
                if self.handler_setup is not None \
                        and self.current_handler_lifecycle_count % self.handler_lifecycle == 1:
                    self.handler_setup(self.meta)

                if isinstance(product, dict):
                    self.product_handler(self.meta, **product)
                elif isinstance(product, (tuple, list)):
                    self.product_handler(self.meta, *product)
                else:
                    self.product_handler(self.meta, product)
            except:
                traceback.print_exc()
            finally:
                self.meta.task = None

                # handler-teardown
                if self.handler_teardown is not None \
                        and self.current_handler_lifecycle_count % self.handler_lifecycle == 0:
                    try:
                        self.handler_teardown(self.meta)
                    except:
                        traceback.print_exc()

                self.product_queue.task_done()

        # final teardown
        if self.handler_teardown is not None:
            self.handler_teardown(self.meta)

    def put(self, task):
        """
        put task params into working queue, just like package queue's put
        :param task:
        """
        self.task_queue.put(task)

    def close(self):
        """
        Close task queue
        :return:
        """
        end_signal = _QueueEndSignal()
        # 在任务队列尾部加入结束信号来关闭任务队列
        for i in range(self.processes_count * self.threads_per_process):
            self.put(end_signal)
        self.task_queue.close()
        self.is_task_queue_closed = True

    def join(self, close=True):
        """
        Wait until the works and handlers terminates.

        """
        if close:  # 注意: 如果此处不close, 则一定需要在其他地方close, 否则无法结束
            # 先close
            if not self.is_task_queue_closed:
                self.close()
        # 等待所有工作进程结束
        for p in self.worker_processes_pool:
            p.join()
        self.product_queue.put((None, _QueueEndSignal()))  # 在结果队列中加入退出指示信号
        self.handler_thread.join()  # 等待处理线程结束

    def __del__(self):
        del self.product_queue
        del self.task_queue

    def __init__(
            self,
            worker_function,
            product_handler=None,
            handler_setup=None, handler_teardown=None, handler_lifecycle=1,
            processes=None, threads_per_process=2,
            task_queue_size=-1, product_queue_size=-1,
            meta=None
    ):
        """
        简易的多进程-多线程任务队列, 包含简单的 MapReduce


        --------- handler setup teardown lifecycle ------
        handler_setup 中可以用于初始化环境, 比如取得一个文件句柄, 数据库connect等
            然后传给handler使用, 在使用了几次(lifecycle的值)之后, 会由 teardown回收
            这样的好处: 1. 避免每次handler中都需要重复打开文件或者连接数据库这种操作
                       2. 避免由于handler崩溃导致的资源未能回收, 跟 with 的功能很像
            例: setup: 打开 fp=log.txt --> handler: 写5个值进 fp --> teardown: 关闭fp

        另一种可能更常用的使用方法是, setup中并不创建连接, 而是给handler一个结果缓冲池(比如一个list),
            handler每次把结果写入缓冲池中,
            执行数次(由lifecycle决定) handler 之后,
            由 teardown 将结果实际写入存储.
            例: setup:创建cache --> handler:写5次值入cache --> teardown:链接数据库, 写入cache中的值, 并关闭

        在setup和teardown中写入值时,
            如果是需要在实例全局生效的, 请以 meta[key] = obj 形式写入
            如果只需要在handler(当然也包括setup和teardown)中生效, 请以 meta.cycle[key] = obj 形式写入

        具体的例子可以看 demo1.py
        --------- handler setup teardown lifecycle ------

        :param worker_function: 工作函数
        :param product_handler: 结果处理函数
        :param handler_setup: handler的初始化函数
        :param handler_teardown: 回收handler中资源的函数
        :param handler_lifecycle: setup-->handler-->handler...-->teardown 这个循环中, handler会被执行几次
        :param processes: 进程数, 若不指定则为CPU核心数
        :type worker_function: Callable[[Any], Any]
        :type product_handler: Callable[[ParamTransfer, Any], None]
        :type handler_setup: Callable[[ParamTransfer], None]
        :type handler_teardown: Callable[[ParamTransfer], None]
        :type processes: int
        :type threads_per_process: int
        :type meta: dict
        """
        self.worker_function = worker_function
        self.processes_count = processes or os.cpu_count() or 1
        self.threads_per_process = threads_per_process
        self.product_handler = product_handler or _dummy_handler
        self.handler_setup = handler_setup
        self.handler_teardown = handler_teardown
        self.handler_lifecycle = handler_lifecycle
        self.current_handler_lifecycle_count = 0  # 用于lifecycle计数 
        self.worker_processes_pool = []  # process pool
        self.task_queue_size = task_queue_size
        self.product_queue_size = product_queue_size
        self.is_task_queue_closed = False

        if bool(self.handler_setup) ^ bool(self.handler_teardown):
            raise ValueError("handler_setup and handler_teardown should be set both or neither")

        self.meta = ParamTransfer(self)
        if meta is not None:
            self.meta.update(meta)

        # 初始化任务队列 进程级
        self.task_queue = multiprocessing.JoinableQueue(self.task_queue_size)
        # 初始化结果队列 进程级
        self.product_queue = multiprocessing.JoinableQueue(self.product_queue_size)

        # 初始化结果处理线程(在主进程中)
        self.handler_thread = threading.Thread(target=self._product_receiver)
        self.handler_thread.daemon = True
        self.handler_thread.start()

        for i in range(self.processes_count):
            p = multiprocessing.Process(
                target=_producer_multi_processes,
                args=(self.task_queue,
                      self.product_queue,
                      self.threads_per_process,
                      self.worker_function
                      )
            )
            p.start()
            self.worker_processes_pool.append(p)
