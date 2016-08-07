# -*- coding: utf-8 -*-

from .context import reprowd, init_context, destroy_context, delete_project
from reprowd.crowdcontext import CrowdContext

import unittest
import sys
import os


class CrowdContextTestSuite(unittest.TestCase):

    def test_table_manipulation(self):
        cc = init_context()

        assert len(cc.show_tables()) == 0
        cc.CrowdData([], "test1")
        assert len(cc.show_tables()) == 1

        try:
            cc.CrowdData([], "test1")
        except:
            assert True

        assert cc.rename_table("test2", "test1") == False
        assert cc.rename_table("test1", "test1") == False
        assert cc.rename_table("test1", "test2") == True
        assert cc.rename_table("test1", "test2") == False

        assert "test1" not in cc.show_tables()
        assert "test2" in cc.show_tables()

        cc.CrowdData([], "test3")
        assert cc.rename_table("test3", "test2") == False

        assert len(cc.show_tables()) == 2

        cc.delete_table("test3")
        cc.delete_table("test2")
        assert len(cc.show_tables()) == 0

        #tmp tables
        cc.CrowdData([], "tmp")
        cc.CrowdData([], "ntmp")
        cc.delete_tmp_tables()
        assert len(cc.show_tables()) == 1
        cc.rename_table("ntmp", "tmp")
        cc.delete_table("ntmp")
        cc.delete_tmp_tables()
        assert len(cc.show_tables()) == 0

        destroy_context()


    def test_operator_initialization(self):
        cc = init_context()

        cc.CrowdData([], "testdata1")
        try:
            cc.CrowdData([], "testdata1")
        except:
            assert True
        cc.rename_table("testdata1", "testdata2")

        try:
            cc.CrowdData([], "testdata1")
        except:
            assert False

        try:
            cc.CrowdData([], "testdata1")
        except:
            assert True
        cc.delete_table("testdata1")
        try:
            cc.CrowdData([], "testdata1")
        except:
            assert False

        cc.CrowdJoin([], "testjoin1")
        try:
            cc.CrowdJoin([], "testdata1")
        except:
            assert True

        try:
            cc.CrowdJoin([], "testjoin2")
        except:
            assert False

        assert cc.delete_table("testdata1") == True
        assert cc.delete_table("testdata2") == True
        assert cc.delete_table("testjoin1") == True
        assert cc.delete_table("testjoin2") == True
        assert len(cc.show_tables()) == 0

        destroy_context()



if __name__ == '__main__':
    unittest.main()
