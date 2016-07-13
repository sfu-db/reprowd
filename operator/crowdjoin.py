from crowdbase.utils.simjoin import SimJoin, jaccard, editsim
import pbclient
import sqlite3
import time
import itertools



class CrowdJoin:

    def __init__(self, object_list, cache_table, crowd_context):
        self.cc = crowd_context
        self.o_list =object_list
        self.cache_table = cache_table

        self.presenter_flag = False
        self.matcher_flag = False
        self.unmatcher_flag = False
        self.simjoin_flag = False
        self.transitivity_flag = False
        self.assignment = 1


    def set_presenter(self, presenter, map_func):
        self.presenter = presenter
        self.map_func = map_func
        self.presenter_flag = True
        return self


    def set_simjoin(self, joinkey_func, threshold, weight_on = False):
        self.joinkey_func = joinkey_func
        self.threshold = threshold
        self.weight_on = weight_on
        self.simjoin_flag = True
        return self


    def set_matcher(self, matcher_func):
        self.matcher_func = matcher_func
        self.matcher_flag = True
        return self

    def set_unmatcher(self, unmatcher_func):
        self.unmatcher_func = unmatcher_func
        self.unmatcher_flag = True
        return self


    def set_transitivity(self):
        self.transitivity_flag = True
        return self

    def set_assignment(n):
        self.assignment = n
        return self


    def join(self, other_object_list = None):
        if not self.presenter_flag:
            raise Exception("""There is no presenter specified. Please call the 'set_presenter' func to specifiy a presenter.""")

        pairs = None
        # Apply fast simjoin algorithms to remove obviously non-matching pairs
        if other_object_list == None: # self-join
            if self.simjoin_flag:
                k_o_list = [(self.joinkey_func(o), o) for o in self.o_list]
                joined = SimJoin(k_o_list).selfjoin(self.threshold, self.weight_on)
                pairs = [ (x[0][1], x[1][1]) for x in joined]
            else:
                pairs= list(itertools.combinations(self.o_list, 2))
        else: # join between two lists
            if self.simjoin_flag:
                k_o_list1 = [(self.joinkey_func(o), o) for o in self.o_list]
                k_o_list2 = [(self.joinkey_func(o), o) for o in other_object_list]
                joined = SimJoin(k_o_list1).join(k_o_list2, self.threshold, self.weight_on)
                pairs = [ (x[0][1], x[1][1]) for x in joined]
            else:
                pairs= list(itertools.product(self.o_list,other_object_list))

        # remove unmatching pairs
        if self.unmatcher_flag:
            pairs = [p for p in pairs if not self.unmatcher_func(p[0], p[1])]

        # Apply the user-specified matcher to identify obviously matching pairs
        unknown_pairs = pairs
        matching_pairs= []
        if self.matcher_flag:
            unknown_pairs = []
            for pair in pairs:
                if self.matcher_func(pair[0], pair[1]):
                    matching_pairs.append(pair)
                else:
                    unknown_pairs.append(pair)

        # Ask the crowd to label the remaining pairs
        crowddata = self.cc.CrowdData(unknown_pairs, self.cache_table) \
                  .map_to_presenter(self.presenter, self.map_func) \
                  .publish_task(self.assignment).get_result()

        # Return the matching pairs identified by the crowd and the matcher
        crowdsourced_pairs = []
        for object_pair, result in zip(crowddata.table["raw_object"], crowddata.table['result']):
            if result['info'] == 'Yes':
                crowdsourced_pairs.append(object_pair)

        return {"crowd":crowdsourced_pairs, "machine": matching_pairs}








