# -*- coding: utf-8 -*-

from .context import crowdbase, init_context, destroy_context, delete_project
from crowdbase.crowdcontext import CrowdContext
from crowdbase.presenter.base import BasePresenter
from crowdbase.presenter.text import TextCmp
from crowdbase.presenter.image import ImageCmp, ImageLabel

import pbclient
import unittest
import sys
import os
import dateutil.parser


class CrowdDataTestSuite(unittest.TestCase):

    #def test_initialization(self):
    def initialization(self):
        cc = init_context()

        # unusual names
        cc.CrowdData([], "test 1")
        cc.CrowdData([], "123.test 1")
        cc.CrowdData([], "+ - ? ! * @ % ^ & # = / \ : \" ")
        cc.CrowdData([], " ".join(["1"]*1000))

        assert len(cc.show_tables()) == 4

        assert "+ - ? ! * @ % ^ & # = / \ : \" " in cc.show_tables()
        assert "test 1" in cc.show_tables()
        assert "123.test 1" in cc.show_tables()
        assert " ".join(["1"]*1000) in cc.show_tables()

        # invalid names
        try:
            cc.CrowdData([], "123.test' 1")
        except:
            assert True

        assert len(cc.show_tables()) == 4

        # invalid object_list
        try:
            cc.CrowdData(None, "123.test 1")
        except:
            assert True

        # duplicate tables
        try:
            cc.CrowdData([], "test 1")
        except:
            assert True
        assert len(cc.show_tables()) == 4

        destroy_context()


    class NewPresenter(BasePresenter):
        def __init__(self):
            self.name = "Test New Presenter"
            self.short_name = "test_new_presenter"
            self.template = "%s" #"pybossa.run('${short_name}')"
            self.description = "This is for testing adding a new presenter"


    #def test_set_presenter(self):
    def set_presenter(self):
        cc = init_context()
        name = "Test CrowdData"
        short_name = "testcrowddata"
        tmp_short_name = short_name + "_tmp"
        tmp_name = name + "_tmp"

        delete_project(short_name = short_name)
        delete_project(short_name = tmp_short_name)

        presenters = [TextCmp(), ImageLabel(), ImageCmp()]
        crowddata = cc.CrowdData([], "test1")
        for presenter in presenters:
            # Normal test
            crowddata.set_presenter(presenter.set_name(name) \
                .set_short_name(short_name))
            assert len(crowddata.data) == 2
            assert "object" in crowddata.data and "object" in crowddata.cols
            assert "id" in crowddata.data and "id" in crowddata.cols
            assert len(pbclient.find_project(short_name = short_name)) == 1
            assert len(pbclient.find_project(name = name)) == 1
            p = pbclient.find_project(name = name)[0]
            "pybossa.run('%s')" %(short_name) in p.info['task_presenter']

            # update short_name
            crowddata.set_presenter(presenter.set_name(name) \
                    .set_short_name(tmp_short_name))

            assert len(pbclient.find_project(short_name = tmp_short_name)) ==1
            assert len(pbclient.find_project(short_name = short_name)) == 0

            # update name
            crowddata.set_presenter(presenter.set_name(tmp_name) \
                    .set_short_name(tmp_short_name))
            assert len(pbclient.find_project(name = name)) == 0
            assert len(pbclient.find_project(name = tmp_name)) == 1

            assert delete_project(short_name = tmp_short_name) == True

        # Adding a New Presenter
        presenter = CrowdDataTestSuite.NewPresenter()
        try:
            crowddata.set_presenter(presenter)
        except:
            assert False
        crowddata.set_presenter(presenter.set_template("pybossa.run('${short_name}');"))
        p = pbclient.find_project(name = presenter.name)[0]
        assert p.info['task_presenter'] == "pybossa.run('%s');" %(presenter.short_name)

        assert delete_project(short_name = presenter.short_name) == True
        destroy_context()


    #def test_publish_task(self):
    def test_publish_task(self):
        cc = init_context()

        presenter = ImageLabel()
        delete_project(short_name = presenter.short_name)
        object_list = ["1.jpg", "2.jpg"]

        crowddata = cc.CrowdData(object_list, "test1") \
            .set_presenter(presenter, lambda obj: {'url_b':obj}) \
            .publish_task(n_assignments = 3)

        # crowddata:
        #{"id": [0, 1],
        #   "object": ["1.jpg", "2.jpg"],
        #    "task": [
        #       {
        #            "id": integer (e.g., 300),
        #            "task_link": url (e.g., "http://localhost:7000/project/textcmp/task/300"),
        #            "task_data":  {"url_b": "1.jpg"}，
        #            "n_assignment": 1,
        #            "priority": 0,
        #            "project_id": integer (e.g., 4),
        #           "create_time": ISO 8601 time format (e.g., "2016-07-12T03:46:04.622127")
        #       },
        #       {
        #            "id": integer (e.g., 300),
        #            "task_link": url (e.g., "http://localhost:7000/project/textcmp/task/300"),
        #            "task_data":  {"url_b": "1.jpg"}],
        #            "n_assignment": 1,
        #            "priority": 0,
        #            "project_id": integer (e.g., 4)
        #           "create_time": ISO 8601 time format (e.g., "2016-07-12T03:46:04.622127")
        #       }
        #   ]
        # }
        d = crowddata.data
        #check id
        assert len(d["id"]) == 2 and 0 in d["id"] and 1 in d["id"]
        #check object
        assert len(d["object"]) == 2 and "1.jpg" in d["object"] and "2.jpg" in d["object"]

        print d
        #check task
        vals = d["task"]
        i = 1
        for v in vals:
            try:
                print  "%s/project/%s/task/%d" %(cc.endpoint.strip('/'), presenter.short_name, v["id"])
                print v["task_link"]
                assert type(v["id"]) is int
                assert v["task_link"] == "%s/project/%s/task/%d" %(cc.endpoint.strip('/'), presenter.short_name, v["id"])
                assert v["task_data"]["url_b"] == "%d.jpg" %(i) and len(v["task_data"]) == 1
                assert v["n_assignments"] == 3
                assert v["priority"] == 0
                assert type(v["project_id"]) is int
                dateutil.parser.parse(v["create_time"])
            except:
                assert False
            i += 1

        # if tasks are published multiple times, only the first takes effect.
        try:
            crowddata.publish_task()
        except:
            assert False


         # publish task before set_presenter
        try:
            crowddata = cc.CrowdData(object_list, "test2") \
                .publish_task(n_assignments = 3)
        except:
            assert True

        assert delete_project(short_name = presenter.short_name) == True
        destroy_context()


    def test_get_result(self):
        cc = init_context()
        presenter = ImageLabel()
        delete_project(short_name = presenter.short_name)
        object_list = ["1.jpg", "2.jpg"]

        crowddata = cc.CrowdData(object_list, "test1") \
            .set_presenter(presenter, lambda obj: {'url_b':obj}) \
            .publish_task(n_assignments = 3)



        assert delete_project(short_name = presenter.short_name) == True
        destroy_context()









if __name__ == '__main__':
    unittest.main()














