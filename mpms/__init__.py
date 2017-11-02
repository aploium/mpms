#!/usr/bin/env python3
# coding=utf-8
from __future__ import absolute_import, division, unicode_literals

import multiprocessing
from multiprocessing.queues import Empty, Full
import os
import threading
import traceback
import logging
import collections

try:
    from typing import Callable, Any, Union
except:
    pass

__ALL__ = ["GMP", "Meta"]

VERSION = (2, 0, 0, 0)
VERSION_STR = "{}.{}.{}.{}".format(*VERSION)

logger = logging.getLogger(__name__)


def _put(q, v):
    while True:
        try:
            q.put(v, timeout=1)
        except Full:
            # gevent.sleep(0.1)
            continue
        else:
            return


def _get(q):
    while True:
        try:
            return q.get(timeout=1)
        except Empty:
            # gevent.sleep(0.1)
            continue


#
# def _worker_container(result_q, func, taskid, args, kwargs):
#     """
#     Args:
#         result_q (multiprocessing.Queue|None)
#     """
#     try:
#         result = func(*args, **kwargs)
#     except Exception as e:
#         logger.error("An error occurs in worker thread, taskid: %s", taskid, exc_info=True)
#         if result_q is not None:
#             result_q.put_nowait((taskid, e))
#         raise
#     else:
#         logger.debug("done %s", taskid)
#         if result_q is not None:
#             result_q.put_nowait((taskid, result))
#             # gevent.sleep(0.1)
#
#
# def _slaver(task_q, result_q,
#             threads, worker):
#     """
#     接收与多进程任务并分发给子线程
#
#     Args:
#         task_q (multiprocessing.Queue)
#         result_q (multiprocessing.Queue|None)
#         threads (int)
#         worker (Callable)
#     """
#     _cur_name = "{}(PID:{}) {}".format(multiprocessing.current_process().name,
#                                        multiprocessing.current_process().pid,
#                                        gevent.getcurrent())
#     logger.debug("mpms subprocess %s start. threads:%s", _cur_name, threads)
#     pool = gevent.pool.Pool(threads)
#
#     while True:
#         pool.wait_available()
#         taskid, args, kwargs = task_q.get()
#         # taskid, args, kwargs = _get(task_q)
#         logger.debug("mpms subprocess %s got %s %s %s", _cur_name, taskid, args, kwargs)
#         if taskid is StopIteration:
#             logger.debug("mpms subprocess %s got stop signal", _cur_name)
#             break
#         # gevent.sleep(0.1)
#         pool.spawn(_worker_container, result_q, worker, taskid, args, kwargs)
#
#         # gevent.sleep(0.1)
#
#     pool.join()
#     logger.debug("mpms subprocess %s exiting", _cur_name)
#     gevent.sleep(0.1)



def _worker_container2(task_q, result_q, func):
    """
    Args:
        result_q (multiprocessing.Queue|None)
    """
    _cur_name = "{}(PID:{}) {}".format(multiprocessing.current_process().name,
                                       multiprocessing.current_process().pid,
                                       gevent.getcurrent())

    while True:
        taskid, args, kwargs = task_q.get()
        # taskid, args, kwargs = _get(task_q)
        logger.debug("mpms worker %s got %s %s %s", _cur_name, taskid, args, kwargs)
        if taskid is StopIteration:
            logger.debug("mpms worker %s got stop signal", _cur_name)
            break

        try:
            result = func(*args, **kwargs)
        except Exception as e:
            logger.error("An error occurs in worker thread, taskid: %s", taskid, exc_info=True)
            result_q.put_nowait((taskid, e))
        else:
            logger.debug("done %s", taskid)
            result_q.put_nowait((taskid, result))


def _slaver2(task_q, result_q, threads, func):
    """
    接收与多进程任务并分发给子线程

    Args:
        task_q (multiprocessing.Queue)
        result_q (multiprocessing.Queue|None)
        threads (int)
        func (Callable)
    """
    _cur_name = "{}(PID:{})".format(multiprocessing.current_process().name,
                                    multiprocessing.current_process().pid, )
    logger.debug("mpms subprocess %s start. threads:%s", _cur_name, threads)
    for i in range(threads):
        th = threading.Thread

    gevent.joinall(workers, raise_error=True)
    logger.debug("mpms subprocess %s exiting", _cur_name)


def get_cpu_count():
    try:
        if hasattr(os, "cpu_count"):
            return os.cpu_count()
        else:
            return multiprocessing.cpu_count()
    except:
        return 0


class Meta(dict):
    def __init__(self, mpms):
        super(Meta, self).__init__()
        self.mpms = mpms  # type: GMP
        self.args = ()
        self.kwargs = ()
        self.taskid = None

    @property
    def self(self):
        """
        Returns:
            GMP
        """
        return self.mpms


class GMP:
    """
    简易的多进程-多线程任务队列 -- gevent

    Args:
        worker (Callable): 工作函数
        collector (Callable[[ParamTransfer, Any], None]): 结果处理函数
        processes (int): 进程数, 若不指定则为CPU核心数
        threads (int): 每个进程下多少个线程
        meta (Meta): meta信息

    Notes:
        --------- handler setup teardown lifecycle ------
        setup 中可以用于初始化环境, 比如取得一个文件句柄, 数据库connect等
            然后传给handler使用, 在使用了几次(lifecycle的值)之后, 会由 teardown回收
            这样的好处: 1. 避免每次handler中都需要重复打开文件或者连接数据库这种操作
                       2. 避免由于handler崩溃导致的资源未能回收, 跟 with 的功能很像
            例: setup: 打开 fp=log.txt --> handler: 写5个值进 fp --> teardown: 关闭fp

    具体的例子可以看 demo1_plus.py
    --------- handler setup teardown lifecycle ------

    """

    def __init__(
            self,
            worker,
            collector=None,
            processes=None, threads=2,
            task_queue_maxlen=-1,
            meta=None
    ):

        self.worker = worker
        self.collector = collector

        self.processes_count = processes or get_cpu_count() or 1
        self.threads_count = threads

        self.total_count = 0  # 总任务数
        self.finish_count = 0  # 已完成的任务数

        self.processes_pool = []  # process pool
        self.task_queue_maxlen = task_queue_maxlen
        self.task_queue_closed = False

        self.meta = Meta(self)
        if meta is not None:
            self.meta.update(meta)

        self.task_q = multiprocessing.Queue()

        if self.collector:
            self.result_q = multiprocessing.Queue()
        else:
            self.result_q = None

        self.collector_thread = None

        self.worker_processes_pool = []

        self.running_tasks = {}

    def start(self):

        logger.debug("mpms starting worker subprocess")
        for i in range(self.processes_count):
            p = multiprocessing.Process(
                # target=_slaver,
                target=_slaver2,
                args=(self.task_q, self.result_q,
                      self.threads_count, self.worker),
                # name="mpms-{}".format(i)
            )
            p.daemon = True
            p.start()
            # gevent.sleep(0.1)
            self.worker_processes_pool.append(p)

        if self.collector is not None:
            logger.debug("mpms starting collect subprocess")
            # self.collector_thread = gevent.spawn(self._collector_container)
            import threading
            self.collector_thread = threading.Thread(target=self._collector_container)
            self.collector_thread.daemon = True
            self.collector_thread.start()
            # self.collector_thread = multiprocessing.Process(target=self._collector_container)
            # self.collector_thread.daemon = True
            # self.collector_thread.start()

            # gevent.sleep(0.1)
        else:
            logger.debug("mpms no collector given, skip collector thread")

    def put(self, *args, **kwargs):
        """
        put task params into working queue

        """
        if self.task_queue_maxlen != -1:
            # gevent.sleep(0.1)
            while self.task_q.qsize() > self.task_queue_maxlen:
                gevent.sleep(0.1)
                logger.debug("q full %s", self.task_q.qsize())

        taskid = self._gen_taskid()
        task_tuple = (taskid, args, kwargs)
        if self.collector:
            self.running_tasks[taskid] = task_tuple
        # gevent.sleep(0.1)
        self.task_q.put_nowait(task_tuple)
        # gevent.sleep(0.1)
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
            # while True:
            # try:
            #     p.join(10)
            # except multiprocessing.TimeoutError:
            #     gevent.sleep(0.1)
            #     continue
            # else:
            #     break
            logger.debug("mpms subprocess %s %s closed", p, p.pid)
        logger.debug("mpms: all worker completed")

        if self.collector:
            self.result_q.put_nowait((StopIteration, None))  # 在结果队列中加入退出指示信号
            self.collector_thread.join()  # 等待处理线程结束

        logger.debug("mpms join completed")

    def waiting_size(self):
        return self.total_count - self.finish_count

    def _gen_taskid(self):
        return "mpms{}".format(self.total_count)

    def _collector_container(self):
        """
        接受子进程传入的结果,并把它发送到master_product_handler()中

        """
        logger.debug("mpms collector_container start")
        gevent.sleep(0.5)
        while True:
            # taskid, result = self.result_q.get()
            try:
                taskid, result = self.result_q.get(timeout=1)
            except Empty:
                #     gevent.sleep(0.1)
                continue

            if taskid is StopIteration:
                logger.debug("mpms collector_container got stop signal")
                break

            _, self.meta.args, self.meta.kwargs = self.running_tasks.pop(taskid)
            self.meta.taskid = taskid
            self.finish_count += 1

            try:
                self.collector(self.meta, result)
            except:
                # 为了继续运行, 不抛错
                logger.error("an error occurs in collector", exc_info=True)

            # 移除meta中已经使用过的字段
            self.meta.taskid, self.meta.args, self.meta.kwargs = None, (), {}

    def close(self):
        """
        Close task queue
        """

        # 在任务队列尾部加入结束信号来关闭任务队列
        for i in range(self.processes_count * self.threads_count):
            # for i in range(self.processes_count):
            self.task_q.put_nowait((StopIteration, (), {}))
        # self.task_q.close()
        self.task_queue_closed = True
