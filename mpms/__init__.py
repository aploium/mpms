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

            queue_product.put(result)
        except:
            traceback.print_exc()
        finally:
            queue_task.task_done()

    exit()


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

    pool = [threading.Thread(target=_producer_multi_threads, args=(queue_task, queue_product, worker_function),
                             daemon=True)
            for _ in range(threads_per_process)]
    for t in pool:
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


class MultiProcessesMultiThreads:
    """
    provide an simple high-level multi-processes_count-multi-threads work environment
    """

    def _product_receiver(self):
        """
        接受子进程传入的结果,并把它发送到master_product_handler()中

        """

        while True:
            product = self.product_queue.get()
            if isinstance(product, _QueueEndSignal):
                self.product_queue.task_done()
                break
            try:
                if isinstance(product, dict):
                    self.product_handler(self.meta, **product)
                elif isinstance(product, (tuple, list)):
                    self.product_handler(self.meta, *product)
                else:
                    self.product_handler(self.meta, product)
            except:
                traceback.print_exc()
            finally:
                self.product_queue.task_done()

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
        self.product_queue.put(_QueueEndSignal())  # 在结果队列中加入退出指示信号
        self.handler_thread.join()  # 等待处理线程结束

    def __del__(self):
        del self.product_queue
        del self.task_queue

    def __init__(self, worker_function, product_handler, processes=None, threads_per_process=2,
                 task_queue_size=-1, product_queue_size=-1, meta=None):
        """
        init

        :type worker_function: function
        :type product_handler: function
        :type processes: int #If not given,would use your cpu core(s) count
        :type threads_per_process: int
        """
        self.worker_function = worker_function
        self.processes_count = processes or os.cpu_count() or 1
        self.threads_per_process = threads_per_process
        self.product_handler = product_handler
        self.worker_processes_pool = []  # process pool
        self.task_queue_size = task_queue_size
        self.product_queue_size = product_queue_size
        self.is_task_queue_closed = False

        self.meta = meta or {}
        self.meta["self"] = self

        # 初始化任务队列 进程级
        self.task_queue = multiprocessing.JoinableQueue(self.task_queue_size)
        # 初始化结果队列 进程级
        self.product_queue = multiprocessing.JoinableQueue(self.product_queue_size)

        # 初始化结果处理线程(在主进程中)
        self.handler_thread = threading.Thread(target=self._product_receiver)
        self.handler_thread.start()

        for i in range(self.processes_count):
            p = multiprocessing.Process(target=_producer_multi_processes,
                                        args=(self.task_queue,
                                              self.product_queue,
                                              self.threads_per_process,
                                              self.worker_function
                                              )
                                        )
            p.start()
            self.worker_processes_pool.append(p)
