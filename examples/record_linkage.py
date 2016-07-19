from crowdbase.crowdcontext import *
from crowdbase.presenter.text import TextCmp
from crowdbase.utils.simjoin import wordset, gramset, jaccard, editsim

if __name__ == "__main__":

    object_list1 = [{'name': 'iPad Two 16GB WiFi White', 'price': 490}, \
                         {'name': 'Apple iPhone 4 16GB White', 'price': 545}, \
                         {'name': 'iPhone 4 32GB White', 'price': 599}, \
                         {'name': 'Apple iPod shuffle 2GB Blue', 'price': 49}]

    object_list2 = [{'name': 'iPad 2nd generation 16GB WiFi White', 'price': 469}, \
                         {'name': 'iPhone 4th generation White 16GB', 'price': 520}, \
                         {'name': 'Apple iPhone 3rd generation Black 16GB', 'price': 375}, \
                         {'name': 'Apple iPad2 16GB WiFi White', 'price': 499}, \
                         {'name': 'Apple iPod shuffle USB Cable', 'price': 19}]

    cc = CrowdContext('http://localhost:7000/', '1588f716-d496-4bb2-b107-9f6b200cbfc9')

    print "=========== CrowdJoin (all pairs) ==========="

    def map_func(obj_pair):
        o1, o2 = obj_pair
        return {'obj1': o1['name']+" | "+str(o1['price']), 'obj2': o2['name']+" | "+str(o2['price'])}

    matches = cc.CrowdJoin(object_list1, cache_table = "rl_pairjoin") \
                            .set_presenter(TextCmp(), map_func) \
                            .join(object_list2)

    print matches

    print "\n===== CrowdJoin (simjoin) =================="
    matches = cc.CrowdJoin(object_list1, cache_table = "rl_pairjoin_with_simjoin") \
                            .set_presenter(TextCmp(), map_func) \
                            .set_simjoin(lambda x: wordset(x['name']), 0.4) \
                            .join(object_list2)

    print matches


    print "\n=== CrowdJoin (simjoin&nonmatcher) ==========="
    # remove the product pairs where one product is twice more expensive than the other
    def nonmatcher(o1, o2):
        return (o1['price']*2 < o2['price']) or (o2['price']*2 < o1['price'])

    matches = cc.CrowdJoin(object_list1, cache_table = "rl_pairjoin_with_simjoin_unmatcher") \
                            .set_presenter(TextCmp(), map_func) \
                            .set_simjoin(lambda x: wordset(x['name']), 0.4) \
                            .set_nonmatcher(nonmatcher) \
                            .join(object_list2)
    print matches

    print "\n=== CrowdJoin (simjoin&unmatcher&matcher) ==="
    def matcher(o1, o2):
        return jaccard(wordset(o1['name']), wordset(o2['name'])) >= 0.8 or editsim(o1['name'], o2['name']) >= 0.8

    matches = cc.CrowdJoin(object_list1, cache_table = "rl_pairjoin_with_simjoin_unmatcher_matcher") \
                            .set_presenter(TextCmp(), map_func) \
                            .set_simjoin(lambda x: wordset(x['name']), 0.4) \
                            .set_nonmatcher(nonmatcher) \
                            .set_matcher(matcher) \
                            .join(object_list2)
    print matches

    #print crowddata.table["raw_object"]
    #print crowddata.table["presenter_object"]
   # print crowddata.table["task"]

