# -*- coding: utf-8 -*-

from .context import reprowd, delete_project
from reprowd.crowdcontext import CrowdContext

import unittest
import sys
import os


class ReprowdDocTestSuite(unittest.TestCase):

    def test_crowdcontext(self):
        import doctest
        from reprowd import crowdcontext
        globs = globals().copy()
        test_db = 'reprowd.test.db'
        CrowdContext.remove_db_file(test_db)
        globs['cc'] =  CrowdContext(local_db = test_db)
        (failure_count, test_count) = doctest.testmod(crowdcontext, globs=globs)
        CrowdContext.remove_db_file(test_db)
        if failure_count:
            exit(-1)


    def test_crowddata(self):
        import doctest
        from reprowd.operators import crowddata
        from reprowd.presenter.image import ImageLabel
        globs = globals().copy()
        test_db = 'reprowd.test.db'
        CrowdContext.remove_db_file(test_db)
        globs['cc'] =  CrowdContext(local_db = test_db)
        (failure_count, test_count) = doctest.testmod(crowddata, globs=globs)
        delete_project(short_name = ImageLabel().short_name)
        CrowdContext.remove_db_file(test_db)
        if failure_count:
            exit(-1)


    def test_crowdjoin(self):
        import doctest
        from reprowd.operators import crowdjoin
        from reprowd.presenter.image import ImageLabel
        globs = globals().copy()
        test_db = 'reprowd.test.db'
        CrowdContext.remove_db_file(test_db)
        globs['cc'] =  CrowdContext(local_db = test_db)
        (failure_count, test_count) = doctest.testmod(crowdjoin, globs=globs)
        delete_project(short_name = ImageLabel().short_name)
        CrowdContext.remove_db_file(test_db)
        if failure_count:
            exit(-1)


if __name__ == '__main__':
    unittest.main()
