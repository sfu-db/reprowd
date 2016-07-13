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
        self.simjoin_flag = False
        self.transitivity_flag = False


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


    def set_transitivity(self):
        self.transitivity_flag = True
        return self


    def join(self, other_object_list, n_answers = 1):
        if not self.presenter_flag:
            raise Exception("""There is no presenter specified. Please call the 'set_presenter' func to specifiy a presenter.""")
        pass

    def selfjoin(self, n_answers = 1):

        if not self.presenter_flag:
            raise Exception("""There is no presenter specified. Please call the 'set_presenter' func to specifiy a presenter.""")

        pairs = None
        if self.simjoin_flag:
            k_o_list = [(self.joinkey_func(o), o) for o in self.o_list]
            joined = SimJoin(k_o_list).selfjoin(self.threshold, self.weight_on)
            pairs = [ (x[0][1], x[1][1]) for x in joined]
        else:
            pairs= list(itertools.combinations(self.o_list, 2))

        unknown_pairs = pairs
        matching_pairs= []
        if self.matcher_flag:
            unknown_pairs = []
            for pair in pairs:
                if self.matcher_func(pair[0], pair[1]):
                    matching_pairs.append(pair)
                else:
                    unknown_pairs.append(pair)

        #print unknown_pairs
        crowddata = self.cc.CrowdData(unknown_pairs, self.cache_table) \
                  .map_to_presenter(self.presenter, self.map_func) \
                  .publish_task(n_answers).get_result()

        crowdsourced_pairs = []
        for object_pair, result in zip(crowddata.table["raw_object"], crowddata.table['result']):
            if result['info'] == 'Yes':
                crowdsourced_pairs.append(object_pair)

        return {"crowd":crowdsourced_pairs, "machine": matching_pairs}








