# -*- coding: utf-8 -*-

from .context import crowdbase, ENDPOINT, API_KEY
from crowdbase.crowdcontext import CrowdContext

import unittest
import sys
import os


class CrowdContextTestSuite(unittest.TestCase):

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


    def test_table_manipulation(self):
        assert len(self.cc.show_tables()) == 0

        # unusual names
        self.cc.CrowdData([], "test 1")



if __name__ == '__main__':
    unittest.main()