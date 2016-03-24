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

__ALL__ = ["MultiProcessesMultiThreads"]


def _producer_multi_threads(queue_task, queue_product, worker_function):
    """
    负责在本进程内分发多线程任务
    :type queue_task: multiprocessing.Queue
    :type queue_product: multiprocessing.Queue
    :type worker_function: function
    """
    is_started = False  # 是否已经接收到了至少一个任务
    while True:
        try:
            task = queue_task.get(timeout=1)
            if isinstance(task, _QueueEndSignal):  # 结束信号
                break
            elif not is_started:  # 等待第一个任务
                is_started = True
                # queue_task.task_done()
        except:
            if is_started:  # 在接收到第一个任务前超时不退出
                break
        else:  # 进行工作并将结果加入队列
            queue_product.put(worker_function(task))

    exit()


def _producer_multi_processes(queue_task,
                              queue_product,
                              threads_per_process,
                              worker_function):
    """
    接收与多进程任务并分发给子线程

    :type queue_task: multiprocessing.Queue
    :type queue_product: multiprocessing.Queue
    :type threads_per_process: int
    :type worker_function: function
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
    def __init__(self) -> None:
        pass


class MultiProcessesMultiThreads:
    """
    provide an simple high-level multi-processes-multi-threads work environment
    """

    def _product_receiver(self) -> None:
        """
        接受子进程传入的结果,并把它发送到master_product_handler()中

        """
        product = None
        while True:  # 不断等待第一个结果的出现
            try:
                product = self.product_queue.get(timeout=1)
            except:  # 超时,继续等待
                continue
            else:  # 收到结果,进行处理
                break

        received_flag = True  # 表示是否已收到数据
        while not isinstance(product, _QueueEndSignal):
            if received_flag:  # 在收到数据后才进行处理
                self.product_handler(product)  # 处理数据的任务交给handler
            try:  # 尝试接收数据
                product = self.product_queue.get(timeout=1)
            except:  # 超时,继续等待
                received_flag = False
            else:  # 收到数据(即使是_QueueEndSignal)
                received_flag = True

    def put(self, task):
        """
        put task params into working queue, just like package queue's put
        :param task:
        """
        self.task_queue.put(task)

    def close(self) -> None:
        """
        Close task queue
        :return:
        """
        end_signal = _QueueEndSignal()
        # 在任务队列尾部加入结束信号来关闭任务队列
        for i in range(self.processes * self.threads_per_process):
            self.put(end_signal)
        self.task_queue.close()
        self.is_task_queue_closed = True

    def join(self) -> None:
        """
        Wait until the works and handlers terminates.

        """
        # Close the task queue if didn't
        if not self.is_task_queue_closed:
            self.close()
        # 等待所有工作进程结束
        for p in self.pool_process:
            p.join()
        self.product_queue.put(_QueueEndSignal())  # 在结果队列中加入退出指示信号
        self.handler_thread.join()  # 等待处理线程结束

    def __init__(self, worker_function, product_handler, processes=None, threads_per_process=2,
                 task_queue_size=-1, product_queue_size=-1):
        """
        init

        :type worker_function: function
        :type product_handler: function
        :type processes: int #If not given,would use your cpu core(s) count
        :type threads_per_process: int
        """
        self.worker_function = worker_function
        self.processes = processes or os.cpu_count() or 1
        self.threads_per_process = threads_per_process
        self.product_handler = product_handler
        self.pool_process = []  # process pool
        self.task_queue_size = task_queue_size
        self.product_queue_size = product_queue_size
        self.is_task_queue_closed = False

        self.task_queue = multiprocessing.Queue(self.task_queue_size)  # 初始化任务队列 进程级
        self.product_queue = multiprocessing.Queue(self.product_queue_size)  # 初始化结果队列 进程级
        # 初始化结果处理线程(在主进程中)
        self.handler_thread = threading.Thread(target=self._product_receiver)
        self.handler_thread.start()
        for i in range(self.processes):
            p = multiprocessing.Process(target=_producer_multi_processes,
                                        args=(self.task_queue,
                                              self.product_queue,
                                              self.threads_per_process,
                                              self.worker_function
                                              )
                                        )
            p.start()
            self.pool_process.append(p)
