from crowdbase.crowdcontext import *
from crowdbase.presenter.text import TextCmp

if __name__ == "__main__":

    object_list = ['iphone5', 'the fifth iphone', 'ipad2']
    cc = CrowdContext('http://localhost:7000/', '1588f716-d496-4bb2-b107-9f6b200cbfc9')
    crowddata = cc.CrowdSelfJoin(object_list, cache_table = "fullpairjoin") \
                            .set_presenter(TextCmp(), map_func = lambda obj_pair: {'obj1':obj_pair[0], 'obj2':obj_pair[1]}) \
                             .join()

    #print crowddata.table["raw_object"]
    #print crowddata.table["presenter_object"]
   # print crowddata.table["task"]
    print crowddata
