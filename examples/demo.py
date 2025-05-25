# coding=utf-8
"""
Do parallel python works easily in multithreads in multiprocesses
一个简单的多进程-多线程工作框架

Work model:
    A simple task-worker-handler model.
    Main threads will continuing adding tasks (task parameters) to task queue.
    Many outer workers(in many threads and many processes) would read tasks from queue one by one and work them out,
        then put the result(if we have) into the product queue.
    An handler thread in main process will read the products in the product queue(if we have),
        and then handle those products.

Why Multithreads in Multiprocesses?
    Many jobs are time-consuming but not very cpu-consuming (such as web fetching),
    due to python's GIL,we cannot use multi-cores in single processes,
    one process is able to handle 50-80 threads,
    but can never execute 1000 or 2000 threads,
    so a stupid but workable way is put those jobs in many threads in many processes

工作模型:
    主线程不断向队列中添加任务参数
    外部进程的大量线程(工作函数)不断从任务队列中读取参数,并行执行后将结果加入到结果队列
    主线程中新开一个处理线程,不断从结果队列读取并依此处理

Due to many threads, some time-consuming tasks would finish much faster than single threads
可以显著提升某些长时间等待的工作的效率,如网络访问

# Win10 x64, python3.5.1 32bit, Intel I7 with 4 cores 8 threads
Processes:20 Threads_per_process:50 Total_threads:1000 TotalTime: 0.7728791236877441
Processes:10 Threads_per_process:20 Total_threads:200 TotalTime: 2.1930654048919678
Processes:5 Threads_per_process:10 Total_threads:50 TotalTime: 8.134965896606445
Processes:3 Threads_per_process:3 Total_threads:9 TotalTime: 44.83632779121399
Processes:1 Threads_per_process:1 Total_threads:1 TotalTime: 401.3383722305298
"""
from __future__ import unicode_literals, print_function
from pprint import pprint
from time import time, sleep

from mpms import MPMS, Meta


def worker(index, t=None):
    """
    Worker function, accept task parameters and do actual work
    should be able to accept at least one arg
    ALWAYS works in external thread in external process

    工作函数,接受任务参数,并进行实际的工作
    总是工作在外部进程的线程中 (即不工作在主进程中)
    """
    sleep(0.2)  # delay 0.2 second
    print(index, t)

    # worker's return value will be added to product queue, waiting handler to handle
    # you can return any type here (Included the None , of course)
    # worker函数的返回值会被加入到队列中,供handler依次处理,返回值允许除了 StopIteration 以外的任何类型
    return index, "hello world"


# noinspection PyStatementEffect
def collector(meta, result):
    """
    Accept and handle worker's product
    It must have at least one arg, because any function in python will return value (maybe None)
    It is running in single thread in the main process,
        if you want to have multi-threads handler, you can simply pass it's arg(s) to another working queue

    接受并处理worker给出的product
    handler总是单线程的,运行时会在主进程中新开一个handler线程
    如果需要多线程handler,可以新建第二个多线程实例然后把它接收到的参数传入第二个实例的工作队列
    handler必须能接受worker给出的参数
    即使worker无显示返回值(即没有return)也应该写一个参数来接收None

    Args:
        meta (Meta): meta信息, 详见 Meta 的docstring
        result (Any|Exception):
            worker的返回值, 若worker出错, 则返回对应的 Exception
    """
    if isinstance(result, Exception):
        return
    index, t = result
    print("received", index, t, time())
    meta.taskid, meta.args, meta.kwargs  # 分别为此任务的 taskid 和 传入的 args kwargs
    meta['want']  # 在 main 中传入的meta字典中的参数
    meta.mpms  # meta.mpms 中保存的是当前的 MPMS 实例


def main():
    results = ""
    # we will run the benchmarks several times using the following params
    # 下面这些值用于多次运行,看时间
    test_params = (
        # (processes, threads_per_process)
        (20, 50),
        (10, 20),
        (5, 10),
        (3, 3),
        (1, 1)
    )
    for processes, threads_per_process in test_params:
        # Init the poll  # 初始化
        m = MPMS(
            worker,
            collector,
            processes=processes,  # optional, how many processes, default value is your cpu core number
            threads=threads_per_process,  # optional, how many threads per process, default is 2
            meta={"any": 1, "dict": "you", "want": {"pass": "to"}, "worker": 0.5},
        )
        m.start()  # start and fork subprocess
        start_time = time()  # when we started  # 记录开始时间

        # put task parameters into the task queue, 2000 total tasks
        # 把任务加入任务队列,一共2000次
        for i in range(2000):
            m.put(i, t=time())

        # optional, close the task queue. queue will be auto closed when join()
        # 关闭任务队列,可选. 在join()的时候会自动关闭
        # m.close()

        # close task queue and wait all workers and handler to finish
        # 等待全部任务及全部结果处理完成
        m.join()

        # write and print records
        # 下面只是记录和打印结果
        results += "Processes:" + str(processes) + " Threads_per_process:" + str(threads_per_process) \
                   + " Total_threads:" + str(processes * threads_per_process) \
                   + " TotalTime: " + str(time() - start_time) + "\n"
        print(results)

        print('sleeping 5s before next')
        sleep(5)


if __name__ == '__main__':
    main()
