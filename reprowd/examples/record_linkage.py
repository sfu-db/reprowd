from reprowd.crowdcontext import *
from reprowd.presenter.text import TextCmp
from reprowd.utils.simjoin import wordset, gramset, jaccard, editsim
import pprint

if __name__ == "__main__":

    object_list1 = [{'name': 'iPad Two 16GB WiFi White', 'price': 490}, \
                         {'name': 'Apple iPhone 4 16GB White', 'price': 545}, \
                         {'name': 'iPhone 4 16GB White', 'price': 550}, \
                         {'name': 'Apple iPod shuffle 2GB Blue', 'price': 49}]

    object_list2 = [{'name': 'iPad 2nd generation 16GB WiFi White', 'price': 469}, \
                         {'name': 'iPhone 4th generation White 16GB', 'price': 520}, \
                         {'name': 'Apple iPhone 3rd generation Black 16GB', 'price': 375}, \
                         {'name': 'Apple iPad2 16GB WiFi White', 'price': 499}, \
                         {'name': 'Apple iPod shuffle USB Cable', 'price': 19}]

    cc = CrowdContext()
    cc.print_tables()

    print "=========== CrowdJoin (all pairs) ==========="

    def map_func(obj_pair):
        o1, o2 = obj_pair
        return {'obj1': o1['name']+" | "+str(o1['price']), 'obj2': o2['name']+" | "+str(o2['price'])}

    matches = cc.CrowdJoin(object_list1, table_name = "reclink_allpair") \
                            .set_presenter(TextCmp(), map_func) \
                            .join(object_list2)
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(matches)

    print "\n===== CrowdJoin (simjoin) =================="
    matches = cc.CrowdJoin(object_list1, table_name = "reclink_simjoin") \
                            .set_presenter(TextCmp(), map_func) \
                            .set_simjoin(lambda x: wordset(x['name']), 0.4) \
                            .join(object_list2)

    pp.pprint(matches)


    print "\n=== CrowdJoin (simjoin&nonmatcher) ==========="
    # remove the product pairs where one product is twice more expensive than the other
    def nonmatcher(obj_pair):
        o1, o2 = obj_pair
        return (o1['price']*2 < o2['price']) or (o2['price']*2 < o1['price'])

    matches = cc.CrowdJoin(object_list1, table_name = "reclink_simjoin_nonmatcher") \
                            .set_presenter(TextCmp(), map_func) \
                            .set_simjoin(lambda x: wordset(x['name']), 0.4) \
                            .set_nonmatcher(nonmatcher) \
                            .join(object_list2)
    pp.pprint(matches)

    print "\n=== CrowdJoin (simjoin&Transitivity) ==="
    def score_func(obj_pair):
        o1, o2 = obj_pair
        return jaccard(wordset(o1['name']), wordset(o2['name']))
    matches = cc.CrowdJoin(object_list1, table_name = "reclink_simjoin_trans") \
                            .set_presenter(TextCmp(), map_func) \
                            .set_simjoin(lambda x: wordset(x['name']), 0.2) \
                            .set_transitivity(score_func) \
                            .join(object_list2)
    pp.pprint(matches)

    #print crowddata.table["raw_object"]
    #print crowddata.table["presenter_object"]
   # print crowddata.table["task"]
