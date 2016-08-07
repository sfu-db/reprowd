from reprowd.crowdcontext import *
from reprowd.presenter.text import TextCmp
from reprowd.utils.simjoin import wordset, gramset, jaccard, editsim
import pprint

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

    cc = CrowdContext()

    print "=========== CrowdJoin (all pairs) ==========="
    matches = cc.CrowdJoin(object_list, table_name = "dedup_allpair") \
                        .set_presenter(TextCmp()) \
                        .join()
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(matches)


    print "\n=========== CrowdJoin (simjoin) ==========="
    matches = cc.CrowdJoin(object_list, table_name = "dedup_simjoin") \
                        .set_presenter(TextCmp()) \
                        .set_simjoin(lambda x: wordset(x), 0.4) \
                        .join()


    pp.pprint(matches)


    print "\n======= CrowdJoin (simjoin & matcher) ======"
    def matcher(obj_pair):
        o1, o2 = obj_pair
        return jaccard(wordset(o1), wordset(o2)) >= 0.8 or editsim(o1, o2) >= 0.8

    matches = cc.CrowdJoin(object_list, table_name = "dedup_simjoin_matcher") \
                        .set_presenter(TextCmp()) \
                        .set_simjoin(lambda x: wordset(x), 0.4) \
                        .set_matcher(matcher) \
                        .join()
    pp.pprint(matches)


    print "\n=========== CrowdJoin (simjoin&transitivity) ==========="
    def score_func(obj_pair):
        o1, o2 = obj_pair
        return jaccard(wordset(o1), wordset(o2))
    matches = cc.CrowdJoin(object_list, table_name = "dedup_simjoin_trans") \
                            .set_presenter(TextCmp()) \
                            .set_simjoin(lambda x: wordset(x), 0.2) \
                            .set_transitivity(score_func) \
                            .join()

    pp.pprint(matches)
