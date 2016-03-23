"""
一个简单的多进程-多线程工作框架

工作模型:
主线程不断向队列中添加任务参数
外部进程的大量线程(工作函数)不断从任务队列中读取参数,并行执行后将结果加入到结果队列
主线程中新开一个处理线程,不断从结果队列读取并依此处理

可以显著提升效率(实测结果,I7 四核八线程)
Processes:20 Threads_per_process:50 Total_threads:1000 TotalTime: 0.7728791236877441
Processes:10 Threads_per_process:20 Total_threads:200 TotalTime: 2.1930654048919678
Processes:5 Threads_per_process:10 Total_threads:50 TotalTime: 8.134965896606445
Processes:3 Threads_per_process:3 Total_threads:9 TotalTime: 44.83632779121399
Processes:1 Threads_per_process:1 Total_threads:1 TotalTime: 401.3383722305298
"""
from time import time, sleep

from mpms import MultiProcessesMultiThreads


def worker(arg):
    """
    工作函数,接受任务参数,并进行实际的工作
    总是工作在外部进程的线程中 (即不工作在主进程中)
    """
    sleep(0.2)
    print(arg)

    # worker函数的返回值会被加入到队列中,供handler依次处理,返回值允许任何类型 (包含None)
    return arg, "return"


def handler(arg):
    """
    接受并处理worker给出的product
    handler总是单线程的,运行时会在主进程中新开一个handler线程

    handler必须能接受worker给出的参数
    即使worker无显示返回值(即没有return)也应该写一个参数来接收None

    """
    print("received", arg, time())


# 在__main__外的代码会在[每一个子进程]中被执行一遍
SOME_GLOBAL_CODES = None


# 只有主进程运行的代码[必须]包含在下面这样的main()里
# 否则会每个子进程都会执行一遍(意思就是会执行好多遍)
def main():
    results = ""
    # 下面这些值用于多次运行,看时间
    test_params = (
        (20, 50),
        (10, 20),
        (5, 10),
        (3, 3),
        (1, 1)
    )
    for processes, threads_per_process in test_params:
        # 初始化
        m = MultiProcessesMultiThreads(worker, handler, processes=processes, threads_per_process=threads_per_process)
        start_time = time()  # 记录开始时间

        # 把任务加入任务队列,一共2000次
        for i in range(2000):
            m.put(str(i))

        m.close()  # 关闭任务队列,可选. 在join()的时候会自动关闭
        m.join()  # 等待全部任务及全部结果处理完成

        # 下面只是记录和打印结果
        results += "Processes:" + str(processes) + " Threads_per_process:" + str(threads_per_process) \
                   + " Total_threads:" + str(processes * threads_per_process) \
                   + " TotalTime: " + str(time() - start_time) + "\n"
        print(results)
        input("按回车进入下一组")


if __name__ == '__main__':
    main()
