from multiprocessing import Process, Manager
import time

def quicksort(s):
    if len(s) == 0:
        return
    pivot = s[len(s) - 1]
    s.pop()
    manager = Manager()
    left = manager.list()
    right = manager.list()
    def compare(a):
        time.sleep(2)
        if a < pivot:
            left.append(a)
        else:
            right.append(a)
    for i in s:
        p = Process(target = compare, args = (i,))
        p.start()
        p.join()

    p = Process(target = quicksort, args = (left,))
    p.start()
    p.join()

    p = Process(target = quicksort, args = (right,))
    p.start()
    p.join()

    #this part is really ugly
    #what I want to do is change the input parameter of the function (like reference in C++)
    #but I found that python does not support this
    #like s = left + right, the outside s value will not be changed
    #this must be optimized
    del s[:]
    for i in left:
        s.append(i)
    s.append(pivot)
    for j in right:
        s.append(j)

    return

if __name__ == '__main__':
    s = [3,1,2,9,5,1,2,3,5,7]
    quicksort(s)
    print s
