import pbclient
import sqlite3
import time
import itertools


class CrowdSelfJoin:

    def __init__(self, object_list, cache_table, crowd_context):
        self.cc = crowd_context
        self.object_list =object_list
        self.cache_table = cache_table
        self.presenter = None
        self.lower_threshold = None

    def set_presenter(self, presenter, map_func):
        self.presenter = presenter
        self.map_func = map_func
        return self


    def set_filter(self, lower_threshold, upper_threshold = 1):
        self.lower_threshold = lower_threshold
        self.upper_threshold = upper_threshold
        return self

    def join(self, n_answers = 1, transitivity = False):

        if self.presenter  == None:
            raise Exception("""There is no presenter specified. Please call the 'set_presenter' func to specifiy a presenter.""")

        object_pair_list = list(itertools.combinations(self.object_list, 2))

        crowddata = self.cc.CrowdData(object_pair_list, self.cache_table) \
                  .map_to_presenter(self.presenter, self.map_func) \
                  .publish_task() \

        print crowddata.table

        crowddata = crowddata.get_result()



        matched_pairs = []
        for object_pair, result in zip(crowddata.table["raw_object"], crowddata.table['result']):
            if result.info == 'YES':
                matched_pairs.append(object_pair)
        return matched_pairs







