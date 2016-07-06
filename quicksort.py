from multiprocessing import Process, Manager
import crowddata
import random

def map_presenter(row):
    return {'pic1':row.data[0], 'pic2':row.data[1]}

def quicksort(data):
    if len(data) == 0:
        return
    pivot_index = random.randrange(0, len(data))
    pivot = data.pop(pivot_index)
    d = []
    for i in data:
        d.append({i, pivot})
    manager = Manager()
    left = manager.list()
    right = manager.list()

    cd = CrowdData('http://localhost:7000/', '8df67fd6-9c9b-4d32-a6ab-b0b5175aba30', d, "test24", "test24", "test24")
    cd.map(map_presenter, "presenter_data").
        createTask("presenter_data", "task", "compare", n_answers = 1).
        getTaskResult("task", "result", stop_condition = lambda result, n: len(result) >= n)

    #since all results are stored in cd['result']
    #parallel is not used here

    for i, d in enumerate(cd.cd['result']):
        if d['info'] == 'left':
            left.append(cd.cd['data'][i])
        else:
            right.append(cd.cd['data'][id])

    p = Process(target = quicksort, args = (left,))
    p.start()
    p.join()

    p = Process(target = quicksort, args = (right,))
    p.start()
    p.join()

    del data[:]
    for i in left:
        data.append(i)
    data.append(pivot)
    for j in right:
        data.append(j)

    return
