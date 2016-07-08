# from multiprocessing import Process, Manager
import crowdcontext
import random

def quicksort(data, cc):
    if len(data) <= 1:
        return
    pivot_index = int(cc.once(lambda d: random.randrange(0, len(d)), data))
    pivot = data.pop(pivot_index)
    object_list = []
    for i in data:
        object_list.append([i, pivot])
    #manager = Manager()
    # left = manager.list()
    # right = manager.list()
    left = []
    right = []
    print object_list
    crowddata = cc.crowddata(object_list, cache_table = "quicksort") \
                            .map_to_presenter("quicksort", map_func = lambda obj: {'pic1': obj[0], 'pic2':obj[1]}) \
                             .publish_task().get_result()
    # print "123"
    #since all results are stored in cd['result']
    #parallel is not used here

    for i, d in enumerate(crowddata.table['result']):
        if d['info'] == 'Down':
            left.append((crowddata.table['raw_object'][i])[0])
        else:
            right.append((crowddata.table['raw_object'][i])[0])
    # print left
    # print right
    quicksort(left, cc)
    quicksort(right, cc)
    # p = Process(target = quicksort, args = (left, cc, n + len(o)))
    # p.start()
    # p.join()
    #
    # p = Process(target = quicksort, args = (right, cc,))
    # p.start()
    # p.join()

    del data[:]
    for i in left:
        data.append(i)
    data.append(pivot)
    for j in right:
        data.append(j)

    return

if __name__ == "__main__":
    cc = crowdcontext.CrowdContext('http://localhost:7000/', '8df67fd6-9c9b-4d32-a6ab-b0b5175aba30')
    data = ["http://www.kidsmathgamesonline.com/images/pictures/numbers600/number3.jpg",
            "http://www.kidsmathgamesonline.com/images/pictures/numbers600/number4.jpg",
            "http://www.kidsmathgamesonline.com/images/pictures/numbers600/number2.jpg",
            "http://www.kidsmathgamesonline.com/images/pictures/numbers600/number1.jpg",
            "http://www.kidsmathgamesonline.com/images/pictures/numbers600/number7.jpg",
            "http://www.kidsmathgamesonline.com/images/pictures/numbers600/number5.jpg",]
    quicksort(data, cc)
    print data
