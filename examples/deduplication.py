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
    matches = cc.CrowdJoin(object_list, cache_table = "fullpairjoin") \
                            .set_presenter(TextCmp(), map_func = lambda obj_pair: {'obj1':obj_pair[0], 'obj2':obj_pair[1]}) \
                             .selfjoin()

    print "CrowdJoin (all pairs)"
    print matches


    matches = cc.CrowdJoin(object_list, cache_table = "pairjoin_with_simjoin") \
                            .set_presenter(TextCmp(), map_func = lambda obj_pair: {'obj1':obj_pair[0], 'obj2':obj_pair[1]}) \
                            .set_simjoin(lambda x: wordset(x), 0.4) \
                            .selfjoin()


    print "CrowdJoin (simjoin)"
    print matches


    def matcher(o1, o2):
        return jaccard(wordset(o1), wordset(o2)) >= 0.8 or editsim(o1, o2) >= 0.8

    matches = cc.CrowdJoin(object_list, cache_table = "pairjoin_with_simjoin_matcher") \
                            .set_presenter(TextCmp(), map_func = lambda obj_pair: {'obj1':obj_pair[0], 'obj2':obj_pair[1]}) \
                            .set_simjoin(lambda x: wordset(x), 0.4) \
                            .set_matcher(matcher) \
                            .selfjoin()

    print "CrowdJoin (simjoin & matcher)"
    print matches

    #print crowddata.table["raw_object"]
    #print crowddata.table["presenter_object"]
   # print crowddata.table["task"]

