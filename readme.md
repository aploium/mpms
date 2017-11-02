# mpms
Simple python Multiprocesses-Multithreads queue  
简易Python多进程-多线程任务队列  
(自用, ap不为生产环境下造成的任何损失和灵异现象负责)
  
在多个进程的多个线程的 worker 中完成耗时的任务, 并在主进程的 collector 中处理结果
  
支持python 2.7/3.4+

### install

```shell
pip install mpms
```

### run

```python
import requests
from mpms import MPMS

def worker(i, j=None):
    r = requests.get('http://example.com', params={"q": i})
    return r.elapsed

def collector(meta, result):
    print(meta.args[0], result)

def main():
    m = MPMS(
        worker,
        collector,  # optional
        processes=2,
        threads=10,  # 每进程的线程数
    )
    m.start()
    for i in range(100):  # 你可以自行控制循环条件
        m.put(i, j=i + 1)  # 这里的参数列表就是worker接受的参数
    m.join()

if __name__ == '__main__':
    main()
```

更多请看 `demo.py`
