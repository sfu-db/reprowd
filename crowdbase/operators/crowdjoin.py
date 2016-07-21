from crowdbase.utils.simjoin import SimJoin, jaccard, editsim
from crowdbase.utils.union_find import UnionFind
from crowdbase.operators.crowddata import CrowdData
from sets import ImmutableSet
import pbclient
import sqlite3
import time
import itertools



class CrowdJoin:

    MATCHING = 1
    NONMATCHING = 0
    UNKNOWN = -1

    def __init__(self, object_list, cache_table, crowd_context):
        self.cc = crowd_context
        self.o_list =object_list
        self.cache_table = cache_table

        self.presenter_flag = False
        self.matcher_flag = False
        self.nonmatcher_flag = False
        self.simjoin_flag = False
        self.transitivity_flag = False
        self.assignment = 1

        self.crowddata = CrowdData(object_list, cache_table, self.cc)


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

    def set_nonmatcher(self, nonmatcher_func):
        self.nonmatcher_func = nonmatcher_func
        self.nonmatcher_flag = True
        return self


    def set_transitivity(self, score_func = None):
        self.transitivity_flag = True
        self.score_func = score_func
        return self

    def set_assignment(n):
        self.assignment = n
        return self


    def join(self, other_object_list = None):
        if not self.presenter_flag:
            raise Exception("""Presenter has not been specified. Please use set_presenter() func to specifiy a presenter.""")

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

        # remove nonmatching pairs
        if self.nonmatcher_flag:
            pairs = [p for p in pairs if not self.nonmatcher_func(p[0], p[1])]

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

        if not self.transitivity_flag:
            # Ask the crowd to label the remaining pairs
            self.crowddata.append(unknown_pairs)\
                      .map_to_presenter(self.presenter, self.map_func) \
                      .publish_task(self.assignment).get_result()

            # Return the matching pairs identified by the crowd and the matcher
            crowdsourced_pairs = []
            for object_pair, result in zip(crowddata.table["object"], crowddata.table['result']):
                if result['info'] == 'Yes':
                    crowdsourced_pairs.append(object_pair)
            return {"all": crowdsourced_pairs+matching_pairs, "crowd":crowdsourced_pairs, "machine": matching_pairs}
        else:
            crowdsourced_pairs, transitivity_pairs = self._label_pairs_with_transitivity(unknown_pairs, self.score_func)
            return {"all": crowdsourced_pairs+matching_pairs+transitivity_pairs, "crowd":crowdsourced_pairs, "machine": matching_pairs, "transitivity": transitivity_pairs}



    def _label_pairs_with_transitivity(self, unknown_pairs, score_func):

        """
        References:
            Jiannan Wang, Guoliang Li, Tim Kraska, Michael J. Franklin, Jianhua Feng.
            Leveraging Transitive Relations for Crowdsourced Joins.
            SIGMOD 2013
        """


        sorted_pairs = unknown_pairs
        if score_func != None:
            sorted_pairs = sorted(unknown_pairs, key = lambda x: score_func(x), reverse = True)

        crowdsourced_pairs = []
        pair_crowdlabel = [] #  a list of published pairs and crowdsourced labels

        crowddata = self.crowddata.map_to_presenter(self.presenter, self.map_func)
        while True:
            must_crowdsourced_pairs = self._must_crowdsourced_pairs(sorted_pairs, pair_crowdlabel)

            # Termination condition: No pair can be published
            if len(must_crowdsourced_pairs) == 0:
                break

            # publish tasks and wait for results
            crowddata = crowddata.clear().append(must_crowdsourced_pairs).publish_task(self.assignment).get_result()

            # Once all the results are collected, add the newly labled pairs into pair_crowdlabel
            for object_pair, result in zip(crowddata.table["object"], crowddata.table['result']):
                if result['info'] == 'Yes':
                    crowdsourced_pairs.append(object_pair)
                    pair_crowdlabel.append((object_pair, CrowdJoin.MATCHING))
                else:
                    pair_crowdlabel.append((object_pair, CrowdJoin.NONMATCHING))

        # Get the matching pairs whose labels are deduced based on transitivity
        pairid_to_label = self._deduce_labels(sorted_pairs, pair_crowdlabel)
        matching_pairs = [pair for pair in unknown_pairs if pairid_to_label[self._id(pair)] == CrowdJoin.MATCHING]
        transitivity_pairs = [p for p in matching_pairs if p not in crowdsourced_pairs]

        return crowdsourced_pairs, transitivity_pairs


    def _id(self, pair):
        return (id(pair[0]), id(pair[1]))


    def _deduce_labels(self, unlabeled_pairs, pair_crowdlabel):
        """
        Deduce the labels of "unlabeled_pairs" based on labeled pairs
        """

        # Init cluster graph
        cluster_graph = self.ClusterGraph()
        for pair, label in pair_crowdlabel:
            if label == CrowdJoin.MATCHING:
                cluster_graph.add_matching_edge(self._id(pair))
            elif label == CrowdJoin.NONMATCHING:
                cluster_graph.add_nonmatching_edge(self._id(pair))

        # Use the cluster graph to deduce the labels of unlabeled pairs
        pairid_to_label = {}
        for pair in unlabeled_pairs:
            deduced_label = cluster_graph.deduce_label(self._id(pair))
            pairid_to_label[self._id(pair)] = deduced_label

        return pairid_to_label


    def _must_crowdsourced_pairs(self, sorted_pairs, pair_crowdlabel):
        """
        Return the pairs that must need to be crowdsourced, i.e. those cannot be deduced based on transitivity
        """

        # Deduce the labels of "sorted_pairs" based on labeled pairs
        pairid_to_label = self._deduce_labels(sorted_pairs, pair_crowdlabel)

        # Identify the pairs in "sorted_pairs" that must require to be crowdsourced
        must_crowdsourced_pairs = []
        cluster_graph = self.ClusterGraph()
        for pair in sorted_pairs:
            deduced_label = pairid_to_label.get(self._id(pair), CrowdJoin.UNKNOWN)
            if deduced_label == CrowdJoin.MATCHING:
                cluster_graph.add_matching_edge(self._id(pair))
            elif deduced_label == CrowdJoin.NONMATCHING:
                cluster_graph.add_nonmatching_edge(self._id(pair))
            else:
                if cluster_graph.deduce_label(self._id(pair)) == CrowdJoin.UNKNOWN:
                    must_crowdsourced_pairs.append(pair)
                cluster_graph.add_matching_edge(self._id(pair))
        return must_crowdsourced_pairs


    class ClusterGraph:
        """
        This class maintains a cluster graph for a labeled graph, where the label
        is either matching or non-matching. The cluster graph merges the matching
        vertices into the same cluster, and then, for each pair of non-matching
        vertices, it adds an edge between their corresponding clusters.
        """
        def __init__(self):
            self.cluster_graph = {}

            # The Union-Find data structure keeps track of a set of vertices
            # partitioned into a number of disjoint clusters.
            self.uf = UnionFind()

        def add_matching_edge(self, edge):
            v1, v2 = edge
            cluster_v1 = self.uf[v1]
            cluster_v2 = self.uf[v2]

            if cluster_v1 == cluster_v2:
                return

            # Add cluster_v1 and cluster_v2 into the cluster graph if they do not exist
            self.cluster_graph.setdefault(cluster_v1, {})
            self.cluster_graph.setdefault(cluster_v2, {})

            # Merge the adjacent vertices of cluster_v1 and cluster_v2
            adjacent_cluster_vset = dict(self.cluster_graph[cluster_v1].items()
                                        +self.cluster_graph[cluster_v2].items())

            # Delete cluster_v1 and cluster_v2 if they are in "adjacent_cluster_vset"
            adjacent_cluster_vset.pop(cluster_v1, None)
            adjacent_cluster_vset.pop(cluster_v2, None)

            # Remove cluster_v1 and cluster_v2 from the cluster graph
            del self.cluster_graph[cluster_v1]
            del self.cluster_graph[cluster_v2]

            # Remove the edges (cluster_v1, adjacent_cluster_v) and
            # (cluster_v2, adjacent_cluster_v) from the cluster graph
            for adjacent_cluster_v in adjacent_cluster_vset.keys():
                self.cluster_graph[adjacent_cluster_v].pop(cluster_v1, None)
                self.cluster_graph[adjacent_cluster_v].pop(cluster_v2, None)

            # Merge cluster_v1 and cluster_v2 as merged_cluster_v
            merged_cluster_v = self.uf.union(cluster_v1, cluster_v2)
            assert merged_cluster_v not in adjacent_cluster_vset
            assert merged_cluster_v == cluster_v1 or merged_cluster_v == cluster_v2

            # Add edges (merged_cluster_v, adjacent_cluster_v)
            self.cluster_graph[merged_cluster_v] = adjacent_cluster_vset
            for adjacent_cluster_v in adjacent_cluster_vset.keys():
                self.cluster_graph[adjacent_cluster_v][merged_cluster_v] = True

        def add_nonmatching_edge(self, edge):
            v1, v2 = edge
            cluster_v1 = self.uf[v1]
            cluster_v2 = self.uf[v2]

            if cluster_v1 == cluster_v2:
                return

            self.cluster_graph.setdefault(cluster_v1, {})[cluster_v2] = True
            self.cluster_graph.setdefault(cluster_v2, {})[cluster_v1] = True

        def deduce_label(self, edge):
            """
            Decide the label of the input edge based on the cluster graph
            """
            v1, v2 = edge
            cluster_v1 = self.uf[v1]
            cluster_v2 = self.uf[v2]

            if cluster_v1 == cluster_v2:
                return CrowdJoin.MATCHING
            elif cluster_v1 in self.cluster_graph.get(cluster_v2, []):
                return CrowdJoin.NONMATCHING
            else:
                return CrowdJoin.UNKNOWN










