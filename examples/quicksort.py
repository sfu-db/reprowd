import random
from crowdbase.crowdcontext import *
from crowdbase.presenter.image import ImageCmp

def _quicksort(object_list, cache_table, batch_n, cc):
    if len(object_list) <= 1:
        return batch_n

    pivot_index = random.randint(0, len(object_list)-1)
    pivot = object_list.pop(pivot_index)

    object_pair_list = [(o, pivot) for o in object_list]
    crowddata = cc.CrowdData(object_pair_list, cache_table = "%s_batch_%d" %(cache_table, batch_n)) \
                            .map_to_presenter(ImageCmp(), map_func = lambda obj: {'pic1': obj[0], 'pic2':obj[1]}) \
                             .publish_task().get_result()
    batch_n += 1

    left = []
    right = []
    for raw_object, result in zip(crowddata.table['raw_object'], crowddata.table['result']):
        if result['info'] == 'Down':
            left.append(raw_object[0])
        else:
            right.append(raw_object[0])

    batch_n = _quicksort(left, cache_table, batch_n, cc)
    batch_n = _quicksort(right, cache_table, batch_n, cc)
    object_list[:] = left+[pivot]+right

    return batch_n

def quicksort(object_list, cache_table, seed, cc):
    random.seed(seed)  # This is to gaurantee that quicksort is determinintic.
    batch_n = 1
    _quicksort(object_list, cache_table, 1, cc)

if __name__ == "__main__":
    cc = CrowdContext('http://localhost:7000/', '1588f716-d496-4bb2-b107-9f6b200cbfc9')

    object_list = ["http://www.kidsmathgamesonline.com/images/pictures/numbers600/number3.jpg",
            "http://www.kidsmathgamesonline.com/images/pictures/numbers600/number4.jpg",
            "http://www.kidsmathgamesonline.com/images/pictures/numbers600/number2.jpg",
            "http://www.kidsmathgamesonline.com/images/pictures/numbers600/number1.jpg",
            "http://www.kidsmathgamesonline.com/images/pictures/numbers600/number7.jpg",
            "http://www.kidsmathgamesonline.com/images/pictures/numbers600/number5.jpg"]



    print "Unsorted Data:"
    print "\n".join(object_list)

    quicksort(object_list, "quicksort", 0, cc)

    print "Sorted Data:"
    print "\n".join(object_list)










