# mpms
Simple python Multiprocesses-Multithreads queue  
简易Python多进程-多线程任务队列 (自用)  
  
在多个进程的多个线程的worker中完成耗时的任务, 并在主进程的handler中处理结果  
  
支持python 2.7/3.4+ 

## 开始使用

我们先来看一个最简单的使用情景：已经确定要进行的任务(而不是在执行过程中动态添加)，并且任务线程不需要争抢资源(如将日志写到同一个文件)

### 1. 下载本项目，并将mpms文件夹放到你的项目中

下载地址:

https://codeload.github.com/aploium/mpms/zip/master

完成后目录结构是这样的：

```
your_project
  |-mpms
  |  |-__init__.py
  |-run.py # 你的代码, 文件名随意
  |-... # 你的项目的其他文件
```

### 2. 准备一个worker函数

```
worker: 并发执行的任务函数，可以接受参数(后文将提到用put传入参数)
```

### 3.在main中调用 -- 主线程创建对象后循环put,然后等待结束:

```python
from mpms import MultiProcessesMultiThreads
if __name__ == '__main__': #这一句很重要
    m = MultiProcessesMultiThreads(
        worker,
        processes=5, # 进程数
        threads_per_process=10, # 线程数
    )
    for i in range(1000): # 你可以自行控制循环条件
        m.put([第一个参数,第二个参数...]) #这里的参数列表就是worker接受的参数
    m.join()
```

举个例子：对localhost请求1000次：[demo0.py](demo0.py)

```python
from mpms import MultiProcessesMultiThreads
import requests
def worker(i):
    print(i)
    requests.get("http://localhost/?i={i}".format(i=i))

if __name__ == '__main__':
    m = MultiProcessesMultiThreads(worker,processes=5, threads_per_process=10)
    for i in range(1000): # 循环1000次, 你可以自行控制循环条件
        m.put([i]) #注意要传入list
    m.join()
```

## 开始使用 -- 我要MapReduce

本项目支持简单的MapReduce, 除了worker函数外你还需要准备handler函数：

```
handler: 全局只会运行一份的处理函数，传入参数为 meta 和 worker所有的返回值
         一般用于写日志等非瓶颈操作 或 动态添加任务(详见下文高级部分)
```

### 看个例子 [demo1.py](demo1.py)

下面这个例子演示了请求1000次`http://example.com`并将结果写入文件  
worker使用5个进程, 每个进程10个线程, 共50线程  

```python
#!/usr/bin/env python3
# coding=utf-8
import requests
from mpms import MPMS #更友好的类名称
from time import sleep

def worker(no, url):
    print("requesting", no, url)
    r = requests.get(url)
    with open("{}.html".format(no), "wb") as fw:  # 将内容写入文件, 这也是比较耗时的IO操作
        fw.write(r.content)
    return no, r.url, r.status_code, len(r.content), r.elapsed.total_seconds()
    # 这里返回了序号, url, 请求得到的http code, 请求得到的网页长度, 请求耗时

def handler(meta, no, url, status_code, length, time):
    # 其中第一个参数 meta 不是由 worker() 传入的, 其他几个跟 worker() 的返回值一一对应
    #    正如字面意思, 它包含了一些在初始化时传入的参数
    #    在本例中, 由主线程创建对象时meta传入了一个文件对象, 用于写日志
    #    详情请看下面的 高级 部分
    print("Done:", no, url, status_code, length, time)
    meta["log_file"].write("{} {} {} {} {}\n".format(no, url, status_code, length, time))
    meta["total_req_time"] += time # 统计总共的耗时

def main():
    fw = open("log.txt", "w", encoding="utf-8")
    
    m = MPMS(
        worker,
        handler,
        processes=5,
        threads_per_process=10,
        meta={ # 传入meta的初始值, 可以传入文件对象, 你可以不使用它
            "log_file":fw,
            "total_req_time":0.0
        },
    )
    
    for i in range(1000):  # 请求1000次
        m.put([i, "http://example.com/?q=" + str(i)])
    
    while len(m)>10:
        print("Remaining: {size}".format(size=len(m)))  # 还有多少任务需要执行的周期性提示
        sleep(1)
    # 在剩余任务少于10个时退出循环，等待任务全部完成
    m.join()
    fw.close()
    
    return m.meta["total_req_time"]

if __name__ == '__main__':
    from time import time
    start = time()
    total_req_time = main()
    spent_time = time() - start
    print("------- Done --------")
    print("Total request time:", total_req_time)
    print("Real time spent:", spent_time)
    print("You are faster about: {}x".format(round(total_req_time / spent_time, 3)))

```

点击下面的图片看以上脚本的演示视频  
[![mpms demo](https://asciinema.org/a/85802.png)](https://asciinema.org/a/85802)  

## 高级

### meta变量

你应该注意到了 handler() 的第一个参数永远是 `meta`  
其中包含了三样东西:  
1. `meta.self` 或者 `meta.mpmt` 中保存了当前的 MultiProcessesMultiThreads 实例  
 以上面demo的例子, 就是 main 函数中的 m 变量.  
 `meta.self`的作用主要是为了在handler中往队列put东西  
 限制: 在目前版本, 如果需要在handler中继续put东西, 在main中调用 `.join()` 时, 应该指定 `close=False`, 即 `m.join(close=False)`  
 否则会因为传入队列被关闭导致无法在handler中继续put  
　　歪楼的ps: 当然允许启动多个这玩意的实例, 互不干扰  
2. `meta.task` 中保存了当前的task参数, 即worker接收到的任务参数.  
　　这个非常有用, 在handler中记录日志或者控制重试, 都可能需要读它.  
3. `meta["某些东西"]` 就是在初始化时 meta 参数中指定的字典.  按上例子, 即为`{"log_file":fw,"total_req_time":0.0}`  
　　由于dict的mutable特性, 在handler中修改它, 同时也会对main中的那个meta生效,  
　　在上例中, 就用它来写文件和统计全局理论总耗时  

### handler_teardown -- 周期性写日志

传入额外的handler_lifecycle, handler_setup, handler_teardown参数

可以实现每请求特定次数后就写日志，保证意外退出也能写好日志

请看[demo1_plus.py](demo1_plus.py)

### len方法

将MPMS的对象传入len，例如上述例子中的

```
print("Remaining: {size}".format(size=len(m)))  # 还有多少任务需要执行的周期性提示
```

其实现是返回了`task_queue`(`Queue`类的对象)的`qsize()` [官方文档](https://docs.python.org/3/library/queue.html)

注意这个函数返回的是对还需要处理的任务数量的**不准确**估计值