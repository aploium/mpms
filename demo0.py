from mpms import MultiProcessesMultiThreads
import requests
def worker(i):
    print(i)
    requests.get("http://localhost/?i={i}".format(i=i))
if __name__ == '__main__':
    m = MultiProcessesMultiThreads(
        worker,
        processes=5, 
        threads_per_process=10, 
    )
    for i in range(1000): # 循环1000次, 你可以自行控制循环条件
        m.put([i]) #注意要传入list
    m.join()