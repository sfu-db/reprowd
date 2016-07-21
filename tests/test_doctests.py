# -*- coding: utf-8 -*-

from .context import crowdbase
from crowdbase.crowdcontext import CrowdContext

import unittest
import sys
import os


class CrowdBaseDocTestSuite(unittest.TestCase):

    def test_crowdcontext(self):
        import doctest
        from crowdbase import crowdcontext 
        globs = globals().copy()
        test_db = 'crowdbase.test.db'
        CrowdContext.remove_db_file(test_db)
        globs['cc'] =  CrowdContext(local_db = test_db)
        (failure_count, test_count) = doctest.testmod(crowdcontext, globs=globs)
        CrowdContext.remove_db_file(test_db)
        if failure_count:
            exit(-1)

if __name__ == '__main__':
    unittest.main()
