import multiprocessing
import time

weekly_i = 200
def worker(k, l):
    print("work start:{0}".format(time.ctime()))
    time.sleep(5)
    print("{}:{}".format(k, l))
    print("work end:{0}".format(time.ctime()))
# def worker(i):
#     print("work start:{0}".format(time.ctime()))
#     time.sleep(5)
#     print("{}:{}".format(10*i* 1000, (10*i + 1)*1000))
#     print("work end:{0}".format(time.ctime()))
# def worker2(i):
#     print("2work start:{0}".format(time.ctime()))
#     time.sleep(5)
#     print("{}:{}".format((10*i+1)*1000,(10*i+2)*1000))
#     print("2work end:{0}".format(time.ctime()))
# def worker3(i):
#     print("3work start:{0}".format(time.ctime()))
#     time.sleep(5)
#     print("{}:{}".format((10*i+2)*1000,(10*i+3)*1000))
#     print("3work end:{0}".format(time.ctime()))
# def worker4(i):
#     print("4work start:{0}".format(time.ctime()))
#     time.sleep(5)
#     print("{}:{}".format((10*i+2)*1000,(10*i+4)*1000))
#     print("4work end:{0}".format(time.ctime()))
# def worker5(i):
#     print("5work start:{0}".format(time.ctime()))
#     time.sleep(5)
#     print("{}:{}".format((10*i+4)*1000,(10*i+5)*1000))
#     print("5work end:{0}".format(time.ctime()))
# def worker6(i):
#     print("6work start:{0}".format(time.ctime()))
#     time.sleep(5)
#     print("{}:{}".format((10*i+5)*1000,(10*i+6)*1000))
#     print("6work end:{0}".format(time.ctime()))
# def worker7(i):
#     print("7work start:{0}".format(time.ctime()))
#     time.sleep(5)
#     print("{}:{}".format((10*i+6)*1000,(10*i+7)*1000))
#     print("7work end:{0}".format(time.ctime()))
# def worker8(i):
#     print("8work start:{0}".format(time.ctime()))
#     time.sleep(5)
#     print("{}:{}".format((10*i+7)*1000,(10*i+8)*1000))
#     print("8work end:{0}".format(time.ctime()))
# def worker9(i):
#     print("9work start:{0}".format(time.ctime()))
#     time.sleep(5)
#     print("{}:{}".format((10*i+8)*1000,(10*i+9)*1000))
#     print("9work end:{0}".format(time.ctime()))
# def worker10(i):
#     print("10work start:{0}".format(time.ctime()))
#     time.sleep(5)
#     print("{}:{}".format((10*i+9)*1000,(10*i+10)*1000))
#     print("10work end:{0}".format(time.ctime()))


if __name__ == "__main__":
    for i in range(weekly_i):
        p = multiprocessing.Process(target=worker, args=(10*i*1000, (10*i+1)*1000,))
        p2 = multiprocessing.Process(target=worker, args=((10*i+1)*1000, (10*i+2)*1000,))
        p3 = multiprocessing.Process(target=worker, args=((10*i+2)*1000, (10*i+3)*1000,))
        p4 = multiprocessing.Process(target=worker, args=((10*i+3)*1000, (10*i+4)*1000,))
        p5 = multiprocessing.Process(target=worker, args=((10*i+4)*1000, (10*i+5)*1000,))
        p6 = multiprocessing.Process(target=worker, args=((10*i+5)*1000, (10*i+6)*1000,))
        p7 = multiprocessing.Process(target=worker, args=((10*i+6)*1000, (10*i+7)*1000,))
        p8 = multiprocessing.Process(target=worker, args=((10*i+7)*1000, (10*i+8)*1000,))
        p9 = multiprocessing.Process(target=worker, args=((10*i+8)*1000, (10*i+9)*1000,))
        p10 = multiprocessing.Process(target=worker, args=((10*i+9)*1000, (10*i+10)*1000,))
        # p = multiprocessing.Process(target=worker, args=(i,))
        # p2 = multiprocessing.Process(target=worker2, args=(i,))
        # p3 = multiprocessing.Process(target=worker3, args=(i,))
        # p4 = multiprocessing.Process(target=worker4, args=(i,))
        # p5 = multiprocessing.Process(target=worker5, args=(i,))
        # p6 = multiprocessing.Process(target=worker6, args=(i,))
        # p7 = multiprocessing.Process(target=worker7, args=(i,))
        # p8 = multiprocessing.Process(target=worker8, args=(i,))
        # p9 = multiprocessing.Process(target=worker9, args=(i,))
        # p10 = multiprocessing.Process(target=worker10, args=(i,))
        p.daemon = True
        p2.daemon = True
        p3.daemon = True
        p4.daemon = True
        p5.daemon = True
        p6.daemon = True
        p7.daemon = True
        p8.daemon = True
        p9.daemon = True
        p10.daemon = True
        p.start()
        p2.start()
        p3.start()
        p4.start()
        p5.start()
        p6.start()
        p7.start()
        p8.start()
        p9.start()
        p10.start()
        p.join()
        p2.join()
        p3.join()
        p4.join()
        p5.join()
        p6.join()
        p7.join()
        p8.join()
        p9.join()
        p10.join()
        print("end!")