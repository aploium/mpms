import time

import requests
import logging
from mpms import MPMS
import err_hunter



def worker(i, j=None):
    logger = err_hunter.getLogger()
    r = requests.head('http://example.com', params={"q": i})
    logger.debug("worker %s, %s", i, j)
    time.sleep(0.1)
    return r.elapsed


def collector(meta, result):
    logger = err_hunter.getLogger()
    logger.info("collect %s %s", meta.args[0], result)


def main():
    m = MPMS(
        worker,
        collector,  # optional
        processes=2,
        threads=1,  # 每进程的线程数
        lifecycle=100,
        subproc_check_interval=3,
    )
    m.start()
    for i in range(10000):  # 你可以自行控制循环条件
        m.put(i, j=i + 1)  # 这里的参数列表就是worker接受的参数
    m.join()


if __name__ == '__main__':
    import err_hunter

    err_hunter.basicConfig('DEBUG', file_level="DEBUG",
                           file_ensure_single_line=False,
                           logfile='/dev/shm/_test.log', multi_process=True)
    main()
