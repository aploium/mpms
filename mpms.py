#!/usr/bin/env python3
# coding=utf-8
from __future__ import absolute_import, division, unicode_literals

import multiprocessing
import os
import threading
import logging
import weakref

try:
    from typing import Callable, Any, Union
except:
    pass

__ALL__ = ["GMP", "Meta"]

VERSION = (2, 0, 0, 0)
VERSION_STR = "{}.{}.{}.{}".format(*VERSION)


def _worker_container(task_q, result_q, func):
    """
    Args:
        result_q (multiprocessing.Queue|None)
    """
    _th_name = threading.current_thread().name

    logging.debug('mpms worker %s starting', _th_name)

    while True:
        taskid, args, kwargs = task_q.get()
        # logging.debug("mpms worker %s got taskid:%s", _th_name, taskid)

        if taskid is StopIteration:
            logging.debug("mpms worker %s got stop signal", _th_name)
            break

        try:
            result = func(*args, **kwargs)
        except Exception as e:
            logging.error("Unhandled error %s in worker thread, taskid: %s", e, taskid, exc_info=True)
            if result_q is not None:
                result_q.put_nowait((taskid, e))
        else:
            # logging.debug("done %s", taskid)
            if result_q is not None:
                result_q.put_nowait((taskid, result))


def _slaver(task_q, result_q, threads, func):
    """
    接收与多进程任务并分发给子线程

    Args:
        task_q (multiprocessing.Queue)
        result_q (multiprocessing.Queue|None)
        threads (int)
        func (Callable)
    """
    _process_name = "{}(PID:{})".format(multiprocessing.current_process().name,
                                        multiprocessing.current_process().pid, )
    logging.debug("mpms subprocess %s start. threads:%s", _process_name, threads)

    pool = []
    for i in range(threads):
        th = threading.Thread(target=_worker_container,
                              args=(task_q, result_q, func),
                              name="{}#{}".format(_process_name, i + 1)
                              )
        th.daemon = True
        pool.append(th)
    for th in pool:
        th.start()

    for th in pool:
        th.join()

    logging.debug("mpms subprocess %s exiting", _process_name)


def get_cpu_count():
    try:
        if hasattr(os, "cpu_count"):
            return os.cpu_count()
        else:
            return multiprocessing.cpu_count()
    except:
        return 0


class Meta(dict):
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

    def __init__(self, mpms):
        super(Meta, self).__init__()
        self.mpms = weakref.proxy(mpms)  # type: MPMS
        self.args = ()
        self.kwargs = {}
        self.taskid = None

    @property
    def self(self):
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

        def main():
            m = MPMS(
                worker,
                collector, # optional
                processes=2, threads=2, # optional
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

        total_count (int): 总任务数
        finish_count (int): 已完成的任务数

    """

    def __init__(
            self,
            worker,
            collector=None,
            processes=None, threads=2,
            task_queue_maxsize=-1,
            meta=None
    ):

        self.worker = worker
        self.collector = collector

        self.processes_count = processes or get_cpu_count() or 1
        self.threads_count = threads

        self.total_count = 0  # 总任务数
        self.finish_count = 0  # 已完成的任务数

        self.processes_pool = []
        self.task_queue_maxsize = task_queue_maxsize
        self.task_queue_closed = False

        self.meta = Meta(self)
        if meta is not None:
            self.meta.update(meta)

        self.task_q = multiprocessing.Queue(maxsize=task_queue_maxsize)

        if self.collector:
            self.result_q = multiprocessing.Queue()
        else:
            self.result_q = None

        self.collector_thread = None

        self.worker_processes_pool = []

        self.running_tasks = {}

    def start(self):
        if self.worker_processes_pool:
            raise RuntimeError('You can only start ONCE!')
        logging.debug("mpms starting worker subprocess")

        for i in range(self.processes_count):
            p = multiprocessing.Process(
                target=_slaver,
                args=(self.task_q, self.result_q,
                      self.threads_count, self.worker),
                name="mpms-{}".format(i + 1)
            )
            p.daemon = True
            p.start()
            self.worker_processes_pool.append(p)

        if self.collector is not None:
            logging.debug("mpms starting collector thread")
            self.collector_thread = threading.Thread(target=self._collector_container, name='mpms-collector')
            self.collector_thread.daemon = True
            self.collector_thread.start()
        else:
            logging.debug("mpms no collector given, skip collector thread")

    def put(self, *args, **kwargs):
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

        self.task_q.put(task_tuple)
        self.total_count += 1

    def join(self, close=True):
        """
        Wait until the works and handlers terminates.

        """
        if close and not self.task_queue_closed:  # 注意: 如果此处不close, 则一定需要在其他地方close, 否则无法结束
            self.close()

        # 等待所有工作进程结束
        for p in self.worker_processes_pool:  # type: multiprocessing.Process
            p.join()
            logging.debug("mpms subprocess %s %s closed", p.name, p.pid)
        logging.debug("mpms all worker completed")

        if self.collector:
            self.result_q.put_nowait((StopIteration, None))  # 在结果队列中加入退出指示信号
            self.collector_thread.join()  # 等待处理线程结束

        logging.debug("mpms join completed")

    def _gen_taskid(self):
        return "mpms{}".format(self.total_count)

    def _collector_container(self):
        """
        接受子进程传入的结果,并把它发送到master_product_handler()中

        """
        logging.debug("mpms collector start")

        while True:
            taskid, result = self.result_q.get()

            if taskid is StopIteration:
                logging.debug("mpms collector got stop signal")
                break

            _, self.meta.args, self.meta.kwargs = self.running_tasks.pop(taskid)
            self.meta.taskid = taskid
            self.finish_count += 1

            try:
                self.collector(self.meta, result)
            except:
                # 为了继续运行, 不抛错
                logging.error("an error occurs in collector, task: %s", taskid, exc_info=True)

            # 移除meta中已经使用过的字段
            self.meta.taskid, self.meta.args, self.meta.kwargs = None, (), {}

    def close(self):
        """
        Close task queue
        """

        # 在任务队列尾部加入结束信号来关闭任务队列
        for i in range(self.processes_count * self.threads_count):
            self.task_q.put((StopIteration, (), {}))
        self.task_queue_closed = True
