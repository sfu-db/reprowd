# -*- coding: utf-8 -*-

from .context import crowdbase, ENDPOINT, API_KEY
from crowdbase.crowdcontext import CrowdContext

import unittest
import sys
import os


class CrowdDataTestSuite(unittest.TestCase):

    @classmethod
    def setup_class(cls):
        cache_db = "crowdbase_test.db"
        cls.fname = "crowdbase_test.db"

        if os.path.isfile(cls.fname):
            os.remove(cls.fname)

        assert os.path.isfile(cls.fname) == False

        cls.cc = CrowdContext(ENDPOINT, API_KEY, cache_db)

    @classmethod
    def tearDown(cls):
        assert os.path.isfile(cls.fname) == True
        os.remove(cls.fname)


    def test_initialization(self):

        # unusual names
        self.cc.CrowdData([], "test 1")
        self.cc.CrowdData([], "123.test 1")
        self.cc.CrowdData([], "+ - ? ! * @ % ^ & # = / \ : \" ")
        self.cc.CrowdData([], " ".join(["1"]*1000))

        assert len(self.cc.show_tables()) == 4

        assert "+ - ? ! * @ % ^ & # = / \ : \" " in self.cc.show_tables()
        assert "test 1" in self.cc.show_tables()
        assert "123.test 1" in self.cc.show_tables()
        assert " ".join(["1"]*1000) in self.cc.show_tables()

        # invalid names
        try:
            self.cc.CrowdData([], "123.test' 1")
        except:
            assert True

        assert len(self.cc.show_tables()) == 4

        # invalid object_list
        try:
            self.cc.CrowdData(None, "123.test 1")
        except:
            assert True

        # duplicate tables
        self.cc.CrowdData([], "test 1")
        assert len(self.cc.show_tables()) == 4





if __name__ == '__main__':
    unittest.main()