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


class CrowdDataTestSuite(unittest.TestCase):

    def test_initialization(self):
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



if __name__ == '__main__':
    unittest.main()














