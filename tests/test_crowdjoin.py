# -*- coding: utf-8 -*-

from .context import crowdbase, init_context, destroy_context, delete_project, ENABLE_MANUAL_LABEL
from crowdbase.crowdcontext import CrowdContext
from multiprocessing import Process
from crowdbase.presenter.base import BasePresenter
from crowdbase.presenter.text import TextCmp
from crowdbase.presenter.image import ImageCmp, ImageLabel
from nose.plugins.attrib import attr
from crowdbase.utils.simjoin import gramset, wordset, jaccard, editsim

import pbclient
import unittest
import sys
import os
import sqlite3
import dateutil.parser
import time
import itertools


class CrowdJoinTestSuite(unittest.TestCase):

    def setUp(self):
        "set up test fixtures"
        self.cc = init_context()
        presenter = TextCmp()
        self.presenter = presenter
        presenter.set_short_name(presenter.short_name+"_test").set_name(presenter.name+"_test")
        delete_project(short_name = presenter.short_name)


    def tearDown(self):
        "tear down test fixtures"
        delete_project(short_name = self.presenter.short_name)
        destroy_context()

    def test_initialization(self):
        cc = self.cc
        try:
            cj = cc.CrowdJoin([], "tmp")
        except:
            assert False
        try:
            cj = cc.CrowdJoin([], "tmp")
        except:
            assert True


    def do_tasks(self, cc, table_name, answers):
        finished_task = []
        cursor = cc.db.cursor()
        exe_str = "SELECT value FROM '%s' WHERE col_name = 'task' order by id" %(table_name)
        n_tasks = len(answers)
        i = 0
        while len(finished_task) < n_tasks:
            cursor.execute(exe_str)
            records = cursor.fetchall()
            for record in records:
                tid = eval(record[0])['id']
                pid = eval(record[0])['project_id']
                if tid not in finished_task:
                    finished_task.append(tid)
                    ans = answers[i]
                    cc.pbclient._pybossa_req("get", "project", "%s/newtask" %(pid))
                    cc.pbclient._pybossa_req("post", "taskrun", \
                        payload={"project_id":pid,"task_id":tid,"info":ans})
                    i+=1
            time.sleep(0.5)


    def map_func(self, obj_pair):
        o1, o2 = obj_pair
        return {'obj1':o1, 'obj2':o2}


    def test_set_presenter(self):
        presenter = self.presenter
        cc = self.cc
        object_list = ["iPad 2", "iPad Two", "iPhone 2"]
        cj = cc.CrowdJoin(object_list, "test1")
        p = Process(target = self.do_tasks, args = (cc, "test1", ("Yes", "No", "No")))
        p.start()
        matches = cj.set_presenter(presenter, self.map_func).join()
        assert len(matches) == 4
        assert matches["all"][0] == ('iPad 2', 'iPad Two') and matches["human"][0] == ('iPad 2', 'iPad Two') \
            and matches["machine"] == [] and matches['transitivity'] == []


    def test_simjoin(self):
        presenter = self.presenter
        cc = self.cc

        # Test 1:invalid input values
        object_list = ["iPad 2", "iPad Two", "iPhone 2", "iPad-2"]
        cj = cc.CrowdJoin(object_list, "test1")

        try:
            matches = cj.set_presenter(presenter, self.map_func).set_simjoin(lambda x: gramset(x, 2), -0.1).join()
        except:
            assert True
        try:
            matches = cj.set_presenter(presenter, self.map_func).set_simjoin(lambda x: gramset(x, 2), 1.1).join()
        except:
            assert True
        try:
            matches = cj.set_presenter(presenter, self.map_func).set_simjoin(lambda x: gramset(x, 1.1), 1.1).join()
        except:
            assert True

        try:
            matches = cj.set_presenter(presenter, self.map_func).set_simjoin(lambda x: gramset(x, 1.1), 0).join()
        except:
            assert True

        # Test 2: 2-gram , non-weighted, threshold = 0.2
        object_list = ["iPad 2", "iPad Two", "iPhone 2", "iPad2"]
        cj = cc.CrowdJoin(object_list, "test2")

        # Precompute answers
        pairs= list(itertools.combinations(object_list, 2))
        pairs = [(o2, o1) for (o1, o2) in pairs]
        left = []
        for pair in pairs:
            o1, o2 = pair
            if jaccard(gramset(o1, 2), gramset(o2, 2)) >= 0.2:
                left.append((o1, o2))
        answers = []
        true_matches = []
        for pair in left:
            if pair == ('iPad Two', 'iPad 2') or pair == ('iPad2', 'iPad 2') or pair == ('iPad2', 'iPad Two'):
                answers.append("Yes")
                true_matches.append(pair)
            else:
                answers.append('No')

        # Simulate crowd workers
        p = Process(target = self.do_tasks, args = (cc, "test2", answers))
        p.start()
        matches = cj.set_presenter(presenter, self.map_func).set_simjoin(lambda x: gramset(x, 2), 0.2).join()

        assert len(matches['all']) == len(matches['human']) and matches['transitivity'] == [] and matches['machine'] == []

        for m in true_matches:
            assert m in matches['all'] and m in matches['human']

        for m in matches['all']:
            assert m in true_matches

        # Test 3: threshold = 1
        object_list = ["iPad 2", "iPad Two", "iPhone 2", "iPad2"]
        cj = cc.CrowdJoin(object_list, "test3")
        matches = cj.set_presenter(presenter, self.map_func).set_simjoin(lambda x: gramset(x, 2), 1).join()
        assert matches['all'] == [] and matches['human'] == [] and matches['machine'] == [] and matches['transitivity'] == []

        object_list = ["iPad 2", "iPad Two", "iPhone 2", "iPad2"]
        cj = cc.CrowdJoin(object_list, "test4")
        matches = cj.set_presenter(presenter, self.map_func).set_simjoin(lambda x: wordset(x), 1).join()
        assert matches['all'] == [] and matches['human'] == [] and matches['machine'] == [] and matches['transitivity'] == []

    @attr('now')
    def test_set_matcher(self):
        presenter = self.presenter
        cc = self.cc

        object_list = ["iPad 2", "iPad Two", "iPhone 2", "iPad2"]
        cj = cc.CrowdJoin(object_list, "test1")

        # Precompute answers
        pairs= list(itertools.combinations(object_list, 2))
        answers = []
        true_matches = []
        for pair in pairs:
            if pair == ('iPad 2', 'iPad2'):
                continue
            if pair == ('iPad 2', 'iPad Two') or pair == ('iPad Two', 'iPad2'):
                answers.append("Yes")
                true_matches.append(pair)
            else:
                answers.append('No')

        assert len(answers) == 5
        def matcher_func(obj_pair):
            o1, o2 = obj_pair
            return jaccard(gramset(o1, 2), gramset(o2, 2)) >= 0.625

        # Simulate crowd workers
        p = Process(target = self.do_tasks, args = (cc, "test1", answers))
        p.start()
        matches = cj.set_presenter(presenter, self.map_func).set_matcher(matcher_func).join()
        assert len(matches['all']) == 3 \
            and len(matches['human']) == 2 \
            and matches['transitivity'] == []  \
            and len(matches['machine']) == 1 \
            and ('iPad 2', 'iPad2') in matches['machine'] \
            and ('iPad 2', 'iPad Two') in matches['human'] \
            and ('iPad Two', 'iPad2') in matches['human']


    def test_set_nonmatcher(self):
        presenter = self.presenter
        cc = self.cc

        object_list = [("iPad 2", 300), ("iPad Two", 305), ("iPhone 2", 400), ("iPad2", 298)] # (name, price\)
        def map_func(obj_pair):
            o1, o2 = obj_pair
            return {'obj1':o1[0] + " | " + str(o1[1]), 'obj2':o2[0] + " | " + str(o2[1])}

        cj = cc.CrowdJoin(object_list, "test1")

        # Precompute answers
        pairs= list(itertools.combinations(object_list, 2))
        answers = ['Yes', 'Yes']

        def nonmatcher_func(obj_pair):
            o1, o2 = obj_pair
            return abs(o1[1]-o2[1]) > 5

        # Simulate crowd workers
        p = Process(target = self.do_tasks, args = (cc, "test1", answers))
        p.start()
        matches = cj.set_presenter(presenter, self.map_func).set_nonmatcher(nonmatcher_func).join()
        assert len(matches['all']) == 2 \
            and len(matches['human']) == 2 \
            and matches['transitivity'] == []  \
            and len(matches['machine']) == 0 \
            and (("iPad 2", 300), ("iPad Two", 305)) in matches['human'] \
            and (("iPad 2", 300), ("iPad2", 298)) in matches['human']


    def test_set_transitivity(self):
        presenter = self.presenter
        cc = self.cc

        object_list = ["iPhone 2", "iPad 2", "iPad Two", "iPad2"]

        # Test 1: w/o score_func
        cj = cc.CrowdJoin(object_list, "test1")
        answers = ['No', 'No', 'No', 'Yes', 'Yes']

        # Simulate crowd workers
        p = Process(target = self.do_tasks, args = (cc, "test1", answers))
        p.start()
        matches = cj.set_presenter(presenter, self.map_func).set_transitivity().join()

        assert len(matches['all']) == 3 \
            and len(matches['human']) == 2 \
            and len(matches['transitivity']) == 1  \
            and len(matches['machine']) == 0 \
            and ("iPad 2", "iPad Two") in matches['human'] \
            and ("iPad 2", "iPad2") in matches['human'] \
            and ("iPad Two", "iPad2") in matches['transitivity']

        # Test 2: w/ score_func
        pairs= list(itertools.combinations(object_list, 2))

        # Pairs will be check in the following order
        # ('iPad 2', 'iPad2') 0.625
        # ('iPad 2', 'iPad Two') 0.454545454545
        # ('iPad Two', 'iPad2') 0.363636363636
        # ('iPhone 2', 'iPad 2') 0.333333333333
        # ('iPhone 2', 'iPad2') 0.25
        # ('iPhone 2', 'iPad Two') 0.125
        def score_func(obj_pair):
            o1, o2 = obj_pair
            return jaccard(gramset(o1, 2), gramset(o2, 2))

        cj = cc.CrowdJoin(object_list, "test2")
        answers = ['Yes', 'Yes', 'No']
        # Simulate crowd workers
        p = Process(target = self.do_tasks, args = (cc, "test2", answers))
        p.start()
        matches = cj.set_presenter(presenter, self.map_func).set_transitivity(score_func).join()
        assert len(matches['all']) == 3 \
            and len(matches['human']) == 2 \
            and len(matches['transitivity']) == 1  \
            and len(matches['machine']) == 0 \
            and ('iPad 2', 'iPad2')  in matches['human'] \
            and ('iPad 2', 'iPad Two') in matches['human'] \
            and ('iPad Two', 'iPad2') in matches['transitivity']


    def test_set_task_parameters(self):
        if ENABLE_MANUAL_LABEL:
            cc = self.cc
            presenter = TextCmp()
            presenter.set_short_name(presenter.short_name).set_name(presenter.name)

            object_list = ["iPhone 2", "iPad 2", "iPad Two", "iPad2"]
            cj = cc.CrowdJoin(object_list, "test1")
            matches = cj.set_presenter(presenter, self.map_func).set_task_parameters(n_assignments = 3).join()
            assert len(matches['all']) == 3 \
            and len(matches['human']) == 3 \
            and len(matches['transitivity']) == 0  \
            and len(matches['machine']) == 0 \
            and ("iPad 2", "iPad Two") in matches['human'] \
            and ("iPad 2", "iPad2") in matches['human'] \
            and ("iPad Two", "iPad2") in matches['human']\

            cj = cc.CrowdJoin(object_list, "test2")
            matches = cj.set_presenter(presenter, self.map_func).set_task_parameters(n_assignments = 3).set_transitivity().join()

            assert len(matches['all']) == 3 \
            and len(matches['human']) == 2 \
            and len(matches['transitivity']) == 1  \
            and len(matches['machine']) == 0 \
            and ("iPad 2", "iPad Two") in matches['human'] \
            and ("iPad 2", "iPad2") in matches['human'] \
            and ("iPad Two", "iPad2") in matches['transitivity']

            delete_project(short_name = presenter.short_name)


    def test_set_join(self):
        cc = self.cc
        presenter = self.presenter

        #  Test the order of set_simjoin(), set_nonmatcher(), set_matcher()
        object_list = ["iPhone 2", "iPad 2", "iPad Two", "iPad2", "iPad2"]

        def matcher_func(obj_pair):
            return obj_pair == ("iPad Two", "iPad2") or obj_pair == ("iPad2", "iPad Two") or \
                      obj_pair  == ('iPad 2', 'iPad Two') or obj_pair == ('iPad Two', 'iPad 2')

        def nonmatcher_func(obj_pair):
            return obj_pair == ('iPad 2', 'iPad Two') or obj_pair == ('iPad Two', 'iPad 2')

        p = Process(target = self.do_tasks, args = (cc, "test1", ('Yes', 'Yes')))
        p.start()

        cj = cc.CrowdJoin(object_list, "test1").set_presenter(presenter)
        matches = cj.set_matcher(matcher_func) \
            .set_simjoin(lambda o: gramset(o, 2)) \
            .set_nonmatcher(nonmatcher_func) \
            .join()

        assert len(matches['all']) == 2 \
            and len(matches['human']) == 2 \
            and len(matches['machine']) == 0 \
            and len(matches['transitivity']) == 0 \
            and ('iPad2', 'iPad 2') in matches['all']  \
            and ('iPad2', 'iPad2') in matches['all']  \
            and ('iPad2', 'iPad 2') in matches['human'] \
            and ('iPad2', 'iPad2') in matches['human']

        #  Test joining two object lists (without transitivity)
        # Simulate crowd workers
        p = Process(target = self.do_tasks, args = (cc, "test2", ('Yes', )))
        p.start()

        object_list1 = ["iPhone 2", "iPad 2", "iPad 2"]
        object_list2 = ["iPad Two", "iPad2", "iPad2"]
        matches = cc.CrowdJoin(object_list1, "test2").set_presenter(presenter) \
            .set_matcher(matcher_func) \
            .set_simjoin(lambda o: gramset(o, 2)) \
            .join(object_list2)

        assert len(matches['all']) == 2 \
        and len(matches['human']) == 1 \
        and len(matches['machine']) == 1 \
        and len(matches['transitivity']) == 0 \
        and ('iPad 2', 'iPad2') in matches['human'] \
         and ('iPad 2', 'iPad2') in matches['all'] \
        and ('iPad 2', 'iPad Two') in matches['machine']

        #  Test joining two object lists (without transitivity)
        p = Process(target = self.do_tasks, args = (cc, "test3", ('Yes', 'No', 'Yes')))
        p.start()

        object_list1 = ["iPhone 2", "iPad 2", "iPad 2"]
        object_list2 = ["iPad Two", "iPad2", "iPad2"]
        matches = cc.CrowdJoin(object_list1, "test3").set_presenter(presenter) \
            .set_simjoin(lambda o: gramset(o, 2), 0.2) \
            .set_transitivity() \
            .join(object_list2)

        assert len(matches['all']) == 2 \
        and len(matches['human']) == 2 \
        and len(matches['machine']) == 0 \
        and len(matches['transitivity']) == 0 \
        and ('iPad 2', 'iPad Two') in matches['human'] \
        and ('iPad 2', 'iPad2') in matches['human'] \
        and ('iPad 2', 'iPad Two') in matches['all'] \
        and ('iPad 2', 'iPad2') in matches['all']


if __name__ == '__main__':
    unittest.main()



