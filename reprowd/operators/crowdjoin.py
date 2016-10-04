# -*- coding: utf-8 -*-

from reprowd.utils.simjoin import SimJoin, jaccard, editsim
from reprowd.utils.union_find import UnionFind
from reprowd.operators.crowddata import CrowdData
from sets import ImmutableSet
import pbclient
import sqlite3
import time
import itertools




class CrowdJoin:

    """
        Given a list of objects (or two lists of objects), the CrowdJoin operator
        finds matching objects in the list (or between the two lists).
    """

    MATCHING = 1
    NONMATCHING = 0
    UNKNOWN = -1

    def __init__(self, object_list, table_name, crowdcontext):
        """
        Initialize a CrowdJoin object.

        Note: It is not recommended to call the constructor directly.
        Please call it through :func:`reprowd.crowdcontext.CrowdContext.CrowdJoin`.

        >>> object_list = ["iPad 2", "iPad Two", "iPhone 2", "iPad2"]
        >>> cc.CrowdJoin(object_list, table_name = "jointest")  #doctest: +SKIP
        <reprowd.operators.crowdjoin.CrowdJoin instance at 0x...>
        """
        self.cc = crowdcontext
        self.object_list =object_list
        self.table_name = table_name

        self.presenter = None
        self.map_func = lambda op: {'obj1':op[0], 'obj2':op[1]}
        self.matcher_func = None
        self.nonmatcher_func = None

        self.simjoin_on = False
        self.transitivity_on = False
        self.n_assignments = 1
        self.priority = 0

        self.crowddata =  self.cc.CrowdData([], table_name)


    def set_presenter(self, presenter, map_func = lambda op: {'obj1':op[0], 'obj2':op[1]}):
        """
        Specify a presenter

        :param presenter: A Presenter object (e.g., :class:`reprowd.presenter.test.TextCmp`).
        :param map_func:  map_func() maps a pair of objects into the data format the presenter requires.
                                       If ``map_func`` is not specified, it will use the default ``map_func = lambda op: {'obj1':op[0], 'obj2':op[1]}``
        :return: The updated CrowdJoin object

        >>> from reprowd.presenter.text import TextCmp
        >>> object_list = ["iPad 2", "iPad Two", "iPhone 2", "iPad2"]
        >>> def map_func(obj_pair):
        ...     o1, o2 = obj_pair
        ...     return {'obj1':o1, 'obj2':o2}
        >>> cc.CrowdJoin(object_list, table_name = "jointest") \\ #doctest: +SKIP
        ...   .set_presenter(TextCmp(), map_func) #doctest: +SKIP
        <reprowd.operators.crowdjoin.CrowdJoin instance at 0x...>
        """
        self.presenter = presenter
        self.map_func = map_func
        return self


    def set_simjoin(self, joinkey_func, threshold = 0.4, weight_on = False):
        """
        Set a simjoin operator

        :param joinkey_func: A function that takes an object as input and outputs a join key on which simjoin will perform.
                                        The join key has to be a set of elements (e.g., bag of words, n-grams)
        :param threshold:  A float number in [0, 1]. The higher the value, the more the number of object pairs that are removed.
        :param weight_on: A boolean value that indicates which similarity function, non-weighted Jaccard (``weight_on = False``)
                                     or weighted Jaccard  (``weight_on = True``), will be used to compute similarity.
        :return: The updated CrowdJoin object

        Why do we need this? Consider a list of n objects. A naive implementation of CrowdJoin is to
        ask workers to label all n^2 object pairs. In fact, among these pairs, most of them look quite dissimilar
        and can be easily identified as non-matching pairs. Setting a simjoin operator will help us to remove
        these obviously non-matching pairs. Specifically, when it is set, all the object pairs whose Jaccard
        similarity values are below the threshold will be removed.

        >>> from reprowd.presenter.text import TextCmp
        >>> from reprowd.utils.simjoin import gramset
        >>> object_list = ["iPad 2", "iPad Two", "iPhone 2", "iPad2"]
        >>> def joinkey_func(obj):
        ...     # Use a 2-gram set as a joinkey
        ...     return gramset(obj, 2)
        >>> crowdjoin = cc.CrowdJoin(object_list, table_name = "jointest") \\ #doctest: +SKIP
        ...               .set_presenter(TextCmp()) \\ #doctest: +SKIP
        ...               .set_simjoin(joinkey_func, 0.2) #doctest: +SKIP
        """
        self.joinkey_func = joinkey_func
        self.threshold = threshold
        self.weight_on = weight_on
        self.simjoin_on = True
        return self


    def set_matcher(self, matcher_func):
        """
        Specify a function for determining which object pairs are matching

        :param matcher_func: A function that takes a pair of objects as input and outputs True for matching pairs
        :return: The updated CrowdJoin object

        >>> from reprowd.presenter.text import TextCmp
        >>> from reprowd.utils.simjoin import gramset, jaccard
        >>> object_list = ["iPad 2", "iPad Two", "iPhone 2", "iPad2"]
        >>> # Identify the pairs whose Jaccard similarity is above 0.9 as matching.
        >>> def matcher_func(obj_pair):
        ...     o1, o2 = obj_pair
        ...     return jaccard(gramset(o1, 2), gramset(o2, 2)) >= 0.9
        >>> crowdjoin = cc.CrowdJoin(object_list, table_name = "jointest") \\ #doctest: +SKIP
        ...               .set_presenter(TextCmp(), map_func) \\ #doctest: +SKIP
        ...               .set_matcher(matcher_func) #doctest: +SKIP
        """
        self.matcher_func = matcher_func
        return self


    def set_nonmatcher(self, nonmatcher_func):
        """
        Specify a function for determining which object pairs are **not** matching

        :param nonmatcher_func: A function that takes a pair of objects as input and outputs True for non-matching pairs
        :return: The updated CrowdJoin object

        >>> from reprowd.presenter.text import TextCmp
        >>> from reprowd.utils.simjoin import gramset
        >>> object_list = [("iPad 2", 300), ("iPad Two", 305), ("iPhone 2", 400), ("iPad2", 298)] # (name, price)
        >>> def map_func(obj_pair):
        ...     o1, o2 = obj_pair
        ...     return {'obj1':o1[0] + " | " + str(o1[1]), 'obj2':o2[0] + " | " + str(o2[1])}
        >>> # If the prices of two product differ by more than 80, they will be identified as a non-matching pair
        >>> def nonmatcher_func(obj_pair):
        ...     o1, o2 = obj_pair
        ...     return abs(o1[1]-o2[1]) > 80
        >>>
        >>> crowdjoin = cc.CrowdJoin(object_list, table_name = "jointest") \\ #doctest: +SKIP
        ...               .set_presenter(TextCmp(), map_func) \\#doctest: +SKIP
        ...               .set_nonmatcher(nonmatcher_func) #doctest: +SKIP
        """
        self.nonmatcher_func = nonmatcher_func
        return self


    def set_transitivity(self, score_func = None):
        """
        Use transitivity to reduce the number of the pairs that need to be labeled by workers.
        Two types of transitivity will be considered:

        1. If A and B are matching, B and C are matching, then A and C will be deduced as matching
        2. If A and B are matching, B and C are non-matching, then A and C will be deduced as non-matching.


        :param score_func: A score function that tends to return a high score for matching pairs and a low score for non-matching pairs.
                                        Having this function will increase the effectiveness of transitivity (See [Wang et al. SIGMOD 2013] for more detail).
        :return: The updated CrowdJoin object

        >>> from reprowd.presenter.text import TextCmp
        >>> from reprowd.utils.simjoin import gramset, jaccard
        >>> object_list = ["iPhone 2", "iPad 2", "iPad Two", "iPad2"]
        >>> def score_func(obj_pair):
        ...     o1, o2 = obj_pair
        ...     return jaccard(gramset(o1, 2), gramset(o2, 2))
        >>> crowdjoin = cc.CrowdJoin(object_list, table_name = "jointest") \\ #doctest: +SKIP
        ...               .set_presenter(TextCmp()) \\ #doctest: +SKIP
        ...               .set_transitivity(score_func) #doctest: +SKIP
        """
        self.transitivity_on = True
        self.score_func = score_func
        return self


    def set_task_parameters(self, n_assignments = 1, priority = 0):
        """
        Set the values of the parameters for published tasks

        :param n_assignments: The number of assignments. For example, ``n_assignments`` = 3 means that each task needs to be done by three different workers
        :param priority:  A float number in [0, 1] that indicates the priority of the published tasks. The larger the value, the higher the priority.
        :return: The updated CrowdJoin object
        """
        self.n_assignments = n_assignments
        self.priority = priority
        return self


    def join(self, other_object_list = None):

        """
        If  **other_object_list** is not specified, perform a self-join on **self.object_list**;
        otherwise, perform a join between *self.object_list* and **other_object_list**

        :param other_object_list: A list of objects that will be joined with **self.object_list**
        :return: A dict with the following attributes:

        - **"all"**: All the matching pairs
        - **"human"**: A subset of matching pairs identified by humans
        - **"machine"**: A subset of matching pairs identified by ``matcher_func()`` in :func:`set_matcher`
        - **"transitivity"**: A subset of matching pairs deduced based on transitivity

        Note: No matter in which order  set_simjoin(), set_matcher(), and set_nonmatcher() are being called,
        they will be applied in the **join()** function in the following order:

                 1. set_simjoin()
                 2. set_nonmatcher()
                 3. set_matcher()

        >>> from reprowd.presenter.text import TextCmp
        >>> object_list = ["iPad 2", "iPad Two", "iPhone 2"]
        >>> crowdjoin = cc.CrowdJoin(object_list, table_name = "jointest") \\  #doctest: +SKIP
        ...               .set_presenter(TextCmp()) \\ #doctest: +SKIP
        ...               .join() # Ask workers to check all pairs #doctest: +SKIP
        >>> matches['all'] #doctest: +SKIP
        [('iPad 2', 'iPad Two')]
        >>> matches['human'] #doctest: +SKIP
        [('iPad 2', 'iPad Two')]
        >>> matches['machine'] #doctest: +SKIP
        []
        >>> matches['transitivity'] #doctest: +SKIP
        []
        """
        if self.presenter == None:
            raise Exception("""Presenter has not been specified. Please use set_presenter() func to specifiy a presenter.""")

        pairs = None
        # Apply fast simjoin algorithms to remove obviously non-matching pairs
        if other_object_list == None: # self-join
            if self.simjoin_on:
                k_o_list = [(self.joinkey_func(o), o) for o in self.object_list]
                joined = SimJoin(k_o_list).selfjoin(self.threshold, self.weight_on)
                pairs = [ (x[0][1], x[1][1]) for x in joined]
            else:
                pairs= list(itertools.combinations(self.object_list, 2))
        else: # join between two lists
            if self.simjoin_on:
                k_o_list1 = [(self.joinkey_func(o), o) for o in self.object_list]
                k_o_list2 = [(self.joinkey_func(o), o) for o in other_object_list]
                joined = SimJoin(k_o_list1).join(k_o_list2, self.threshold, self.weight_on)
                pairs = [ (x[0][1], x[1][1]) for x in joined]
            else:
                pairs= list(itertools.product(self.object_list, other_object_list))

        # remove nonmatching pairs
        if self.nonmatcher_func != None:
            pairs = [p for p in pairs if not self.nonmatcher_func(p)]

        # Apply the user-specified matcher to identify obviously matching pairs
        unknown_pairs = pairs
        matching_pairs= []
        if self.matcher_func != None:
            unknown_pairs = []
            for pair in pairs:
                if self.matcher_func(pair):
                    matching_pairs.append(pair)
                else:
                    unknown_pairs.append(pair)
        matching_pairs = self._unique_pair(matching_pairs) # remove duplicate matching pairs

        crowddata = self.crowddata
        # Ask the crowd to label the remaining pairs
        if not self.transitivity_on:
            # Get the unique unknown pairs
            unique_unknown_pairs = self._unique_pair(unknown_pairs)
            crowddata.extend(unique_unknown_pairs).set_presenter(self.presenter, self.map_func) \
                      .publish_task(self.n_assignments, self.priority).get_result().quality_control("em")

            # Return the matching pairs identified by the crowd and the matcher
            crowdsourced_pairs = []
            for object_pair, result in zip(crowddata.data["object"], crowddata.data['em']):
                if result == 'Yes':
                    crowdsourced_pairs.append(object_pair)
            return {"all": crowdsourced_pairs+matching_pairs, "human":crowdsourced_pairs, "machine": matching_pairs, "transitivity": []}
        else:
            crowdsourced_pairs, transitivity_pairs = self._label_pairs_with_transitivity(unknown_pairs, self.score_func)
            return {"all": crowdsourced_pairs+matching_pairs+transitivity_pairs, \
                        "human":crowdsourced_pairs, \
                        "machine": matching_pairs, \
                        "transitivity": transitivity_pairs}


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

        crowddata = self.crowddata.set_presenter(self.presenter, self.map_func)
        while True:
            must_crowdsourced_pairs = self._must_crowdsourced_pairs(sorted_pairs, pair_crowdlabel)

            # Termination condition: No pair can be published
            if len(must_crowdsourced_pairs) == 0:
                break

            # publish tasks and wait for results
            crowddata = crowddata.clear().extend(must_crowdsourced_pairs) \
                .publish_task(self.n_assignments, self.priority).get_result().quality_control('em')

            # Once all the results are collected, add the newly labled pairs into pair_crowdlabel
            for object_pair, result in zip(crowddata.data["object"], crowddata.data['em']):
                if result == 'Yes':
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

    def _unique_pair(self, l):
        seen = set()
        return [x for x in l if self._id(x) not in seen and not seen.add(self._id(x))]

    def _deduce_labels(self, unlabeled_pairs, pair_crowdlabel):
        """
        Deduce the labels of "unlabeled_pairs" based on labeled pairs
        """

        # Init cluster graph
        cluster_graph = self.__ClusterGraph()
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
        cluster_graph = self.__ClusterGraph()
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


    class __ClusterGraph:
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
