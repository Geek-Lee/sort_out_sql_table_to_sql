import threading
from time import ctime, sleep

weekly_i = 10

def one(i):
    print("1.to sql开始! %s %s" % (ctime(), i*1000))
    sleep(5)
    print("1.to sql结束! %s %s" % (ctime(), (i+1)*1000))
    #print("1.to sql结束! %s %s" % (ctime(), (i * 1) * 1000))

def two(i):
    print("2.to sql开始! %s %s" % (ctime(), (i+2)*1000))
    sleep(5)
    print("2.to sql结束! %s %s" % (ctime(), (i+3)*1000))



for i in range(weekly_i):
    threads = []
    t1 = threading.Thread(target=one, args=(i,))
    threads.append(t1)
    t2 = threading.Thread(target=two, args=(i,))
    threads.append(t2)
    for t in threads:
        t.setDaemon(True)
        t.start()
    t.join()
    print("all over %s" % ctime())

# if __name__ == '__main__':
#     for t in threads:
#         t.setDaemon(True)
#         t.start()
#     t.join()
#     print("all over %s" % ctime())

