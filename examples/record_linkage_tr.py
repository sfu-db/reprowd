from crowdbase.crowdcontext import *
from crowdbase.presenter.text import TextCmp
from crowdbase.utils.simjoin import wordset, gramset, jaccard, editsim

if __name__ == "__main__":

    object_list1 = [{'name': 'iPad Two 16GB WiFi White', 'price': 490}, \
                         {'name': 'Apple iPhone 4 16GB White', 'price': 545}, \
                         {'name': 'Apple iPad 2 16GB White WiFi', 'price': 495}, \
                         {'name': 'Apple iPod shuffle 2GB Blue', 'price': 49}]

    object_list2 = [{'name': 'iPad 2nd generation 16GB WiFi White', 'price': 469}, \
                         {'name': 'iPhone 4th generation White 16GB', 'price': 520}, \
                         {'name': 'Apple iPhone 3rd generation Black 16GB', 'price': 375}, \
                         {'name': 'Apple iPad2 16GB WiFi White', 'price': 499}, \
                         {'name': 'Apple iPod shuffle USB Cable', 'price': 19}]

    cc = CrowdContext('http://localhost:7000/', '1588f716-d496-4bb2-b107-9f6b200cbfc9')
    cc.delete_cache_tables("rl_tr_pairjoin_1", "rl_tr_pairjoin_2", "rl_tr_pairjoin_3", "rl_tr_pairjoin_4", "rl_tr_pairjoin_with_simjoin_1", "rl_tr_pairjoin_with_simjoin_2", "rl_tr_pairjoin_with_simjoin_3")
    print "=========== CrowdJoin (all pairs&transitivity) ==========="

    def map_func(obj_pair):
        o1, o2 = obj_pair
        return {'obj1': o1['name']+" | "+str(o1['price']), 'obj2': o2['name']+" | "+str(o2['price'])}

    def score_func(obj_pair):
        o1, o2 = obj_pair
        return jaccard(wordset(o1['name']), wordset(o2['name']))

    matches = cc.CrowdJoin(object_list1, cache_table = "rl_tr_pairjoin") \
                            .set_presenter(TextCmp(), map_func) \
                            .set_transitivity(score_func) \
                            .join(object_list2)

    print matches

    print "\n===== CrowdJoin (simjoin&transitivity) =================="
    matches = cc.CrowdJoin(object_list1, cache_table = "rl_tr_pairjoin_with_simjoin") \
                            .set_presenter(TextCmp(), map_func) \
                            .set_simjoin(lambda x: wordset(x['name']), 0.2) \
                            .set_transitivity(score_func) \
                            .join(object_list2)

    print matches



