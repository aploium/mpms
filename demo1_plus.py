#!/usr/bin/env python3
# coding=utf-8
"""
与 demo1.py 的区别是本文件使用 handler_setup 和 handler_teardown 来打开和关闭log文件
而demo1.py在main()中打开和关闭
plus版本的好处是可以避免程序在中途意外退出后log文件内容的丢失
"""
import requests
import mpms


def worker(no, url):
    print("requesting", no, url)
    r = requests.get(url)
    with open("{}.html".format(no), "wb") as fw:  # 将内容写入文件, 这也是比较耗时的IO操作
        fw.write(r.content)
    return no, r.url, r.status_code, len(r.content), r.elapsed.total_seconds()


def handler_setup(meta):  # 注意这个函数
    print("setup!")
    meta.cycle["log_file"] = open("log.txt", "a", encoding="utf-8")


def handler_teardown(meta):  # 注意这个函数
    print("teardown!")
    meta.cycle["log_file"].close()


def handler(meta, no, url, status_code, length, time):
    print("Done:", no, url, status_code, length, time)

    fw = meta.cycle["log_file"]  # !!!!!!!!!!!!!!注意这行与 demo.py 的不同!!!!!!!!!
    fw.write("{} {} {} {} {}\n".format(no, url, status_code, length, time))

    meta["total_req_time"] += time  # 统计总共的耗时


def main():
    m = mpms.MultiProcessesMultiThreads(
        worker,
        handler,
        handler_setup=handler_setup,
        handler_teardown=handler_teardown,
        handler_lifecycle=5,
        processes=5,
        threads_per_process=10,
        meta={"total_req_time": 0.0},
    )

    for i in range(100):  # 请求1000次
        m.put([i, "http://example.com/?q=" + str(i)])

    m.join()

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
