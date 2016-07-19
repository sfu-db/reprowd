from crowdbase.crowdcontext import *
from crowdbase.presenter.text import TextCmp
from crowdbase.utils.simjoin import wordset, gramset, jaccard, editsim

if __name__ == "__main__":

    object_list = ['iPad Two 16GB WiFi White', \
                         'iPad 2nd generation 16GB WiFi White', \
                         'iPhone 4th generation White 16GB', \
                         'Apple iPhone 4 16GB White', \
                         'Apple iPhone 3rd generation Black 16GB', \
                         'iPhone 4 32GB White', \
                         'Apple iPad2 16GB WiFi White', \
                         'Apple iPod shuffle 2GB Blue', \
                         'Apple iPod shuffle USB Cable']

    cc = CrowdContext('http://localhost:7000/', '1588f716-d496-4bb2-b107-9f6b200cbfc9')

    print "=========== CrowdJoin (all pairs&transitivity) ==========="
    def map_func(obj_pair):
        return {'obj1':obj_pair[0], 'obj2':obj_pair[1]}

    def score_func(obj_pair):
        return jaccard(wordset(obj_pair[0]), wordset(obj_pair[1]))

    matches = cc.CrowdJoin(object_list, cache_table = "dedup_tr_pairjoin") \
                            .set_presenter(TextCmp(), map_func) \
                            .set_transitivity(score_func) \
                             .join()

    print matches


    print "\n=========== CrowdJoin (simjoin&transitivity) ==========="
    matches = cc.CrowdJoin(object_list, cache_table = "dedup_tr_pairjoin_simjoin") \
                            .set_presenter(TextCmp(), map_func) \
                            .set_simjoin(lambda x: wordset(x), 0.2) \
                            .set_transitivity(score_func) \
                            .join()


    print matches


    #print crowddata.table["raw_object"]
    #print crowddata.table["presenter_object"]
   # print crowddata.table["task"]

