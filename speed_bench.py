#!/usr/bin/env python3
# coding=utf-8
from mpms import MPMS, Meta
import requests
import random
import time
import err_hunter

err_hunter.colorConfig("DEBUG")

def worker(url):
    r = requests.get(url, timeout=10)
    # time.sleep(5)
    # print("worker req", url, )
    # gevent.sleep(0.1)

    return url


def collector(meta, r):
    # print("collector:", meta, r)
    if isinstance(r, Exception):
        print("got error", r)
        return

        # print("succ", r)


def main():
    start = time.time()
    print("begin")
    mp = MPMS(
        worker, collector,
        processes=8, threads=40,
        task_queue_maxsize=320,
        meta={"cat":1}
    )
    mp.start()

    for i in range(10000):
        url = "https://example.com/?q={}".format(i)
        mp.put(url)
    print("put complete")
    mp.close()
    print("closed")
    # gevent.sleep(20)
    # x=gevent.spawn(g.join)
    # x.join()
    mp.join()
    # gevent.sleep(100)
    print("all done", time.time() - start)


if __name__ == '__main__':

    main()
