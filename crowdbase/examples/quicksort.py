import random
from crowdbase.crowdcontext import *
from crowdbase.presenter.image import ImageCmp

def _quicksort(crowddata, object_list):
    if len(object_list) <= 1:
        return crowddata

    pivot_index = random.randint(0, len(object_list)-1)
    pivot = object_list.pop(pivot_index)

    object_pair_list = [(o, pivot) for o in object_list]
    crowddata = crowddata.clear().append(object_pair_list).publish_task().get_result()

    left = []
    right = []
    for obj, result in zip(crowddata.table['object'], crowddata.table['result']):
        if result['info'] == 'Down':
            left.append(obj[0])
        else:
            right.append(obj[0])

    crowddata = _quicksort(crowddata, left)
    crowddata = _quicksort(crowddata, right)
    object_list[:] = left+[pivot]+right

    return crowddata

def quicksort(object_list, cache_table, seed, cc):
    random.seed(seed)  # This is to gaurantee that quicksort is determinintic.
    crowddata = cc.CrowdData([], table_name = cache_table) \
                            .map_to_presenter(ImageCmp(), map_func = lambda obj: {'pic1': obj[0], 'pic2':obj[1]})
    _quicksort(crowddata, object_list)

if __name__ == "__main__":
    cc = CrowdContext('http://localhost:7000/', '1588f716-d496-4bb2-b107-9f6b200cbfc9')

    object_list = ["http://www.kidsmathgamesonline.com/images/pictures/numbers600/number3.jpg",
            "http://www.kidsmathgamesonline.com/images/pictures/numbers600/number4.jpg",
            "http://www.kidsmathgamesonline.com/images/pictures/numbers600/number2.jpg",
            "http://www.kidsmathgamesonline.com/images/pictures/numbers600/number1.jpg",
            "http://www.kidsmathgamesonline.com/images/pictures/numbers600/number7.jpg",
            "http://www.kidsmathgamesonline.com/images/pictures/numbers600/number6.jpg",
            "http://www.kidsmathgamesonline.com/images/pictures/numbers600/number5.jpg"]

    cc.delete_table("quicksort")
    print "Unsorted Data:"
    print "\n".join(object_list)

    quicksort(object_list, "quicksort", 0, cc)

    print "Sorted Data:"
    print "\n".join(object_list)










