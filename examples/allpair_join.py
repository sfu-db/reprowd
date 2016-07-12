from crowdbase.crowdcontext import *

if __name__ == "__main__":

    object_list = [['iphone5', 'the fifth iphone'], ['ipad2', 'the second ipad']]
    cc = CrowdContext('http://localhost:7000/', '1588f716-d496-4bb2-b107-9f6b200cbfc9')
    crowddata = cc.CrowdData(object_list, cache_table = "fullpairjoin") \
                            .map_to_presenter("fullpairjoin", map_func = lambda obj: {'obj1':obj[0], 'obj2':obj[1]}) \
                             .publish_task().get_result()

    #print crowddata.table["raw_object"]
    #print crowddata.table["presenter_object"]
   # print crowddata.table["task"]
    print crowddata.table["result"]

