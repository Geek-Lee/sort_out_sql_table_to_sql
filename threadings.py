import threading
from time import ctime, sleep
import multiprocessing
import time
# #1.1
# def worker(interval):
#     n = 5
#     while n > 0:
#         print("This time is {0}".format(ctime()))
#         time.sleep(interval)
#         n -= 1
#
# if __name__ == '__main__':
#     p = multiprocessing.Process(target=worker, args=(3,))
#     p.start()
#     print("p.pid:", p.pid)
#     print("p.name:", p.name)
#     print("p.is_alive:", p.is_alive())
#
# #1.2
# def worker_1(interval):
#     print("worker_1")
#     time.sleep(interval)
#     print("end worker_1")
# def worker_2(interval):
#     print("worker_2")
#     time.sleep(interval)
#     print("end worker_2")
# def worker_3(interval):
#     print("worker_3")
#     time.sleep(interval)
#     print("end worker_3")
# if __name__ == "__main__":
#     p1 = multiprocessing.Process(target=worker_1, args=(3,))
#     p2 = multiprocessing.Process(target=worker_2, args=(4,))
#     p3 = multiprocessing.Process(target=worker_3, args=(5,))
#
#     p1.start()
#     p2.start()
#     p3.start()
#
#     print("The nu")

# def music(func):
#     for i in range(2):
#         print("I was listening to %s. %s" % (func, ctime()))
#         sleep(1)
#
# def move(func):
#     for i in range(2):
#         print("I was at the %s! %s" % (func, ctime()))
#         sleep(5)
#
# threads = []
# t1 = threading.Thread(target=music, args=(u'爱情买卖',))
# threads.append(t1)
# t2 = threading.Thread(target=move, args=(u'阿凡达',))
# threads.append(t2)
#
# if __name__ == '__main__':
#     for t in threads:
#         t.setDaemon(True)
#         t.start()
#
#     print("all over %s" % ctime())

# #coding=utf-8
# import threading
# from time import ctime,sleep
#
#
# def music(func):
#     for i in range(2):
#         print("I was listening to %s. %s" %(func,ctime()))
#         sleep(1)
#
# def move(func):
#     for i in range(2):
#         print("I was at the %s! %s" %(func,ctime()))
#         sleep(5)
#
# threads = []
# t1 = threading.Thread(target=music,args=(u'爱情买卖',))
# threads.append(t1)
# t2 = threading.Thread(target=move,args=(u'阿凡达',))
# threads.append(t2)
#
# if __name__ == '__main__':
#     for t in threads:
#         t.setDaemon(True)
#         t.start()
#
#     print("all over %s" %ctime())

def func(msg):
    print("msg:", msg)
    time.sleep(3)
    print("end")

if __name__ == "__main__":
    pool = multiprocessing.Pool(processes=10)
    for i in range(4):
        msg = "hello %d" %(i)
        pool.apply_async(func, (msg,))

    print("mark~~~")
    pool.close()
    pool.join()
    print("done")