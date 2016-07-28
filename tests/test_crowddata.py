# -*- coding: utf-8 -*-

from .context import crowdbase, init_context, destroy_context, delete_project, ENABLE_MANUAL_LABEL
from crowdbase.crowdcontext import CrowdContext
from crowdbase.presenter.base import BasePresenter
from crowdbase.presenter.text import TextCmp
from crowdbase.presenter.image import ImageCmp, ImageLabel
from nose.plugins.attrib import attr

import pbclient
import unittest
import sys
import os
import dateutil.parser


class CrowdDataTestSuite(unittest.TestCase):

    def test_initialization(self):
    #def initialization(self):
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


    def test_set_presenter(self):
    #def set_presenter(self):
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
            assert crowddata.presenter.short_name == presenter.short_name
            assert crowddata.presenter.name == presenter.name

            # update short_name
            crowddata.set_presenter(presenter.set_name(name) \
                    .set_short_name(tmp_short_name))

            assert crowddata.presenter.short_name == tmp_short_name
            assert crowddata.presenter.name == presenter.name

            # update name
            crowddata.set_presenter(presenter.set_name(tmp_name) \
                    .set_short_name(tmp_short_name))

            assert crowddata.presenter.short_name == tmp_short_name
            assert crowddata.presenter.name == tmp_name

        # Adding a New Presenter
        presenter = CrowdDataTestSuite.NewPresenter()
        try:
            crowddata.set_presenter(presenter)
        except:
            assert False
        crowddata.set_presenter(presenter.set_template("pybossa.run('${short_name}');"))
        assert crowddata.presenter.template == "pybossa.run('${short_name}');"

        destroy_context()


    def test_publish_task(self):
    #def publish_task(self):
        cc = init_context()
        presenter = ImageLabel()
        presenter.set_short_name(presenter.short_name+"_test").set_name(presenter.name+"_test")
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

        #check task
        vals = d["task"]
        i = 1
        for v in vals:
            try:
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
        presenter.set_short_name(presenter.short_name+"_test").set_name(presenter.name+"_test")

        delete_project(short_name = presenter.short_name)
        object_list = ['http://farm4.static.flickr.com/3114/2524849923_1c191ef42e.jpg', \
                         'http://www.7-star-admiral.com/0015_animals/0629_angora_hamster_clipart.jpg']

        crowddata = cc.CrowdData(object_list, "test1") \
            .set_presenter(presenter, lambda obj: {'url_b':obj}) \
            .publish_task(n_assignments = 1).get_result(blocking = False)

        for task in crowddata.data["task"]:
            task_id = task["id"]
            project_id = task["project_id"]
            ans = "YES"

            cc.pbclient._pybossa_req("get", "project", "%s/newtask" %(project_id) )
            cc.pbclient._pybossa_req("post", "taskrun", \
                payload={"project_id":project_id,"task_id":task_id,"info":ans})

        d = crowddata.get_result(False).data


        #check id col
        assert len(d["id"]) == 2 and 0 in d["id"] and 1 in d["id"]
        #check object
        assert len(d["object"]) == 2 and object_list[0] in d["object"] and object_list[1] in d["object"]

        #check task col
        vals = d["task"]
        i = 0
        for v in vals:
            try:
                assert type(v["id"]) is int
                assert v["task_link"] == "%s/project/%s/task/%d" %(cc.endpoint.strip('/'), presenter.short_name, v["id"])
                assert v["task_data"]["url_b"] == "%s" %(object_list[i]) and len(v["task_data"]) == 1
                assert v["n_assignments"] == 1
                assert v["priority"] == 0
                assert type(v["project_id"]) is int
                dateutil.parser.parse(v["create_time"])
            except:
                raise
                assert False
            i += 1

        # test exceptions
        try:
            cc.CrowdData(object_list, "test2").get_result()
        except:
            assert True

        try:
            cc.CrowdData(object_list, "test3") \
                .set_presenter(presenter, lambda obj: {'url_b':obj}) \
                .get_result()
        except:
            assert True

        try:
            cc.CrowdData(object_list, "test4") \
                .set_presenter(presenter, lambda obj: {'url_b':obj}) \
                .get_result()
        except:
            assert True

        # test multiple assignment
        global ENABLE_MANUAL_LABEL
        if ENABLE_MANUAL_LABEL:
            presenter = ImageLabel()
            presenter.set_short_name(presenter.short_name).set_name(presenter.name)
            # In order to test multi-assignments, you have to manually do tasks
            assert False
            n = 2
            crowddata = cc.CrowdData(object_list, "test5") \
                .set_presenter(presenter, lambda obj: {'url_b':obj}) \
                .publish_task(n_assignments = n).get_result(blocking = False)

            # The first user is doing tasks
            for task in crowddata.data["task"]:
                task_id = task["id"]
                project_id = task["project_id"]
                ans = "YES"

                x = cc.pbclient._pybossa_req("get", "project", "%s/newtask" %(project_id) )
                y = cc.pbclient._pybossa_req("post", "taskrun", \
                    payload={"project_id":project_id,"task_id":task_id,"info":ans})

            d = crowddata.get_result(blocking=True).data
            print d
            vals = d["result"]
            i = 0
            for v in vals:
                try:
                    assert type(v["task_id"]) is int
                    assert v["task_link"] == "%s/project/%s/task/%d" %(cc.endpoint.strip('/'), presenter.short_name, v["task_id"])
                    assert type(v["project_id"]) is int
                    assert len(v["assignments"]) == n
                    for a in v["assignments"]:
                        assert type(a["id"]) is int
                        assert type(a["worker_id"]) is str or type(a["worker_id"]) is unicode
                        dateutil.parser.parse(a["start_time"])
                        dateutil.parser.parse(a["finish_time"])
                except:
                    raise
                    assert False
                i += 1

        assert delete_project(short_name = presenter.short_name) == True
        destroy_context()


    def test_quality_control(self):
        cc = init_context()

        presenter = ImageLabel()

        # test quality_control()
        object_list = ["image1.jpg", "image2.jpg"]
        try:
            crowddata = cc.CrowdData(object_list, "test1") \
                .set_presenter(presenter, lambda obj: {'url_b':obj}) \
                .publish_task().quality_control("mv")
        except:
            assert True

        try:
            crowddata = cc.CrowdData(object_list, "test2") \
                .set_presenter(presenter, lambda obj: {'url_b':obj}) \
                .publish_task().get_result(False).quality_control("mv")
        except Exception as error:
            print error
            assert False

        try:
            crowddata = cc.CrowdData(object_list, "test3") \
                .set_presenter(presenter, lambda obj: {'url_b':obj}) \
                .publish_task().get_result(False).quality_control("test")
        except:
            assert True

        import json
        # test __em_col()
        result_col = [\
            {\
                'assignments': [\
                    {\
                        'worker_id': '1',\
                        'worker_response': u'NO'\
                    },\
                    {\
                        'worker_id': u'10.0.2.2',\
                         'worker_response': u'YES'\
                    },\
                    {\
                        'worker_id': u'2',\
                         'worker_response': u'YES'\
                    }\
                ],\
                'task_id': 407\
            },\
           {\
                'assignments': [\
                    {\
                        'worker_id': '1',\
                        'worker_response': u'NO'\
                    },\
                    {\
                        'worker_id': u'10.0.2.2',\
                         'worker_response': u'YES'\
                    }\
                ],\
                'task_id': 408\
            }\
        ]

        em_col = crowddata._CrowdData__em_col(result_col, iteration = 10)
        assert em_col[0] == "YES"
        assert em_col[1] == "YES"

        result_col[1]["assignments"] = [\
                    {\
                        'worker_id': '2',\
                        'worker_response': u'NO'\
                    }
                ]

        print result_col
        em_col = crowddata._CrowdData__em_col(result_col, iteration = 10)
        assert em_col[0] == "YES"
        assert em_col[1] == "NO"

        # test mv
        result_col[1]["assignments"] = [\
            {\
                'worker_id': '1',\
                'worker_response': u'NO'\
            },\
            {\
                'worker_id': u'10.0.2.2',\
                 'worker_response': u'NO'\
            }\
         ]

        print result_col
        em_col = crowddata._CrowdData__mv_col(result_col)
        assert em_col[0] == "YES"
        assert em_col[1] == "NO"

        # Test none values
        result_col[1] = None

        em_col = crowddata._CrowdData__mv_col(result_col)
        assert em_col[0] == "YES"
        assert em_col[1] == None

        em_col = crowddata._CrowdData__em_col(result_col)
        assert em_col[0] == "YES"
        assert em_col[1] == None

        assert delete_project(short_name = presenter.short_name) == True
        destroy_context()


    def test_filter_clear_append(self):
        cc = init_context()

        presenter = ImageLabel()
        presenter.set_short_name(presenter.short_name+"_test").set_name(presenter.name+"_test")
        delete_project(short_name = presenter.short_name)

        object_list = ["image1.jpg", "image2.jpg"]

        crowddata = cc.CrowdData(object_list, "test1") \
            .set_presenter(presenter, lambda obj: {'url_b':obj}) \
            .publish_task()

        for task in crowddata.data["task"]:
            task_id = task["id"]
            project_id = task["project_id"]
            ans = "YES"
            cc.pbclient._pybossa_req("get", "project", "%s/newtask" %(project_id) )
            cc.pbclient._pybossa_req("post", "taskrun", \
                payload={"project_id":project_id,"task_id":task_id,"info":ans})

        d = crowddata.get_result(blocking=True).quality_control("mv").data
        assert len(d) == 5
        assert len(d['result']) == 2 and d['result'][0] != None and d['result'][1] != None
        assert len(d['mv']) == 2 and d['mv'][0] == 'YES' and d['mv'][1] == 'YES'

        d = crowddata.filter(lambda r: r['mv'] == "YES").data
        assert len(d) == 5
        assert len(d['result']) == 2 and d['result'][0] != None and d['result'][1] != None
        assert len(d['mv']) == 2 and d['mv'][0] == 'YES' and d['mv'][1] == 'YES'

        d = crowddata.filter(lambda r: r['object'] == "image2.jpg").data
        assert len(d) == 5
        assert len(d['result']) == 1 and d['result'][0] != None
        assert len(d['mv']) == 1 and d['mv'][0] == 'YES'

        crowddata.extend(["image3.jpg", "image4.jpg", "image5.jpg"]).publish_task()
        crowddata.append("image6.jpg") # not publish image6.jpg

        for task in crowddata.data["task"]:
            if task == None:
                continue
            task_id = task["id"]
            project_id = task["project_id"]
            ans = "NO"
            cc.pbclient._pybossa_req("get", "project", "%s/newtask" %(project_id) )
            cc.pbclient._pybossa_req("post", "taskrun", \
                payload={"project_id":project_id,"task_id":task_id,"info":ans})

        d = crowddata.get_result(blocking=True).quality_control("mv").data
        print crowddata.data
        assert len(d) == 5
        assert len(d['result']) == 5 and d['result'][0] != None and d['result'][1] != None and d['result'][2] != None and d['result'][3] != None
        assert len(d['mv']) == 5 and d['mv'][0] == 'YES' and d['mv'][1] == 'NO' and d['mv'][2] == 'NO' and d['mv'][3] == 'NO'

        d = crowddata.filter(lambda x: x['mv'] == "NO").data
        assert len(d) == 5
        assert len(d['result']) == 3 and d['result'][0] != None and d['result'][1] != None and d['result'][2] != None
        assert len(d['mv']) == 3 and d['mv'][0] == 'NO' and d['mv'][1] == 'NO' and d['result'][2] != None

        crowddata.clear()
        d = crowddata.get_result(blocking=True).quality_control("mv").data
        assert len(d) == 5
        assert len(d['result']) ==0
        assert len(d['mv']) == 0

        assert delete_project(short_name = presenter.short_name) == True
        destroy_context()


if __name__ == '__main__':
    unittest.main()














