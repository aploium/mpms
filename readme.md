# mpms
Simple python Multiprocesses-Multithreads queue  
简易Python多进程-多线程任务队列 (自用)  
  
在多个进程的多个线程的worker中完成耗时的任务, 并在主进程的handler中处理结果  
  
支持python 2.7 3.4/3.5/3.6+ 

## demo
下面这个例子演示了请求1000次`http://example.com`并将结果写入文件  
worker使用5个进程, 每个进程10个线程, 共50线程  
```python
import requests
from mpms import MultiProcessesMultiThreads

def worker(no, url):
    print("requesting", no, url)
    r = requests.get(url)
    with open("{}.html".format(no),"wb") as fw: # 将内容写入文件, 这也是比较耗时的IO操作
        fw.write(r.content)
    return no, r.url, r.status_code, len(r.content), r.elapsed.total_seconds()

def handler(meta, no, url, status_code, length, time):
    # 其中第一个参数 meta 不是由 worker() 传入的, 其他几个跟 worker() 的返回值一一对应
    #    正如字面意思, 它包含了一些在初始化时传入的参数
    #    在本例中, 为了演示, 传入了一个文件对象, 用于写日志
    #    详情请看下面的 高级 部分
    print("Done:", no, url, status_code, length, time)
    
    fw = meta["log_file"]
    fw.write("{} {} {} {} {}\n".format(no,url,status_code,length,time))
    
    meta["total_req_time"] += time # 统计总共的耗时

def main():
    fw = open("log.txt","w",encoding="utf-8")
    
    m = MultiProcessesMultiThreads(
        worker,
        handler,
        processes=5,
        threads_per_process=10,
        meta={"log_file":fw,"total_req_time":0.0},
    )
    
    for i in range(1000): # 请求1000次
        m.put([i,"http://example.com/?q=" + str(i)])
    
    m.join()
    fw.close()
    
    return m.meta["total_req_time"]

if __name__ == '__main__':
    from time import time
    start = time()
    total_req_time = main()
    spent_time = time()-start
    print("------- Done --------")
    print("Total request time:", total_req_time)
    print("Real time spent:", spent_time)
    print("You are faster about: {}x".format(round(total_req_time/spent_time,3)))
```

点击下面的图片看以上脚本的演示视频  
[![mpms demo](https://asciinema.org/a/85802.png)](https://asciinema.org/a/85802)  

## 高级
### meta变量
你应该注意到了 handler() 的第一个参数永远是 `meta`  
其中包含了三样东西:  
1. `meta.self` 或者 `meta.mpmt` 中保存了当前的 MultiProcessesMultiThreads 实例  
 以上面demo的例子, 就是 main 函数中的 q 变量.  
 `meta.self`的作用主要是为了在handler中往队列put东西  
 限制: 在目前版本, 如果需要在handler中继续put东西, 在main中调用 `.join()` 时, 应该指定 `close=False`, 即 `q.join(close=False)`  
 否则会因为传入队列被关闭导致无法在handler中继续put  
　　歪楼的ps: 当然允许启动多个这玩意的实例, 互不干扰  
2. `meta.task` 中保存了当前的task参数, 即worker接收到的任务参数.  
　　这个非常有用, 在handler中记录日志或者控制重试, 都可能需要读它.  
3. `meta["某些东西"]` 就是在初始化时 meta 参数中指定的字典.  按上例子, 即为`{"log_file":fw,"total_req_time":0.0}`  
　　由于dict的mutable特性, 在handler中修改它, 同时也会对main中的那个meta生效,  
　　在上例中, 就用它来写文件和统计全局理论总耗时  

