# -*- coding: utf-8 -*-

from crowdbase.operators.crowddata import CrowdData
from crowdbase.operators.crowdjoin import CrowdJoin
import sqlite3
import pbclient
import json
import os

class CrowdContext:

    """
    Main entry point for CrowdBase functionality. Intuitively, a CrowdContext can be
    thought of as a fault-tolerant and reproducible environment for doing crowdsourced
    data processing tasks. Once a CrowdContext is created, it will connect to a pybossa
    server and a local database, providing APIs for creating crowd operators(e.g., CrowdJoin),
    and manipulating cached crowddata.
    """

    # Note that __current_cd may not contain all the cached data in the database,
    # but only contains those created by the current CrowdContext
    __current_cd = {}

    def __init__(self, endpoint=None, api_key=None, local_db = "crowdbase.db"):
        """
        Create a new CrowdContext. The endpoint and api_key should be set,
        either through the named parameters here or through environment variables (
        CROWDBASE_ENDPOINT, CROWDBASE_API_KEY)

        :param endpoint: Pybossa server URL (e.g. http://localhost:7000).
        :param api_key: An api_key to access the pybossa server. You can get an api_key by
            creating an account in the pybossa server, and check the api_key of the account by clicking the
            "account name" --> "My Profile" on the top right of the page.
        :param local_db: The local database name
        :return: A CrowdContext object

        >>> from crowdbase.crowdcontext import CrowdContext
        >>> CrowdContext("http://localhost:7000", api_key = "test", local_db = "crowdbase.test.db")  #doctest: +SKIP
        <crowdbase.crowdcontext.CrowdContext instance at 0x...>
        """

        if not endpoint:
            endpoint = os.environ.get("CROWDBASE_ENDPOINT", "")
        if not api_key:
            api_key = os.environ.get("CROWDBASE_API_KEY", "")

        self.endpoint = endpoint
        self.api_key = api_key
        pbclient.set('endpoint', endpoint)
        pbclient.set('api_key', api_key)
        self.pbclient = pbclient

        self.local_db = local_db
        CrowdContext.__current_cd[local_db] = []
        self.db = sqlite3.connect(local_db)
        self.cursor = self.db.cursor()


    def CrowdData(self, object_list, table_name):
        """
        Return :class:`CrowdData` object

        :param object_list: A list of objects where an object can be anything (e.g., int, string, dict)
        :param table_name:  The table used for caching the crowd tasks/results related to the :class:`CrowdData` object


        >>> # Create a CrowdData object for image labeling
        >>> cc.CrowdData(["image1.jpg", "image2.jpg"], "tmp")   #doctest: +SKIP
        <crowdbase.operators.crowddata.CrowdData instance at 0x...>
        """
        # Check if table_name has been used before
        if table_name in CrowdContext.__current_cd[self.local_db]:
            raise Exception("'%s' has been used before. "
                "Please choose a different name that is not in the list:"
                "[%s]" %(table_name, ",".join(CrowdContext.__current_cd[self.local_db])))
        else:
            CrowdContext.__current_cd[self.local_db].append(table_name)
        return CrowdData(object_list, table_name, self)


    def CrowdJoin(self, object_list, table_name):
        """
        Return :class:`CrowdJoin` object

        :param object_list: A list of objects where an object can be anything (e.g., int, string, dict)
        :param table_name:  The table used for caching the crowd tasks/results related to the :class:`CrowdJoin` object

        >>> # Create a CrowdJoin object for deduplication
        >>> cc.CrowdJoin(["iphone 4", "ipad 2", "ipad two"], "tmp") #doctest: +SKIP
        <crowdbase.operators.crowdjoin.CrowdJoin instance at 0x...>
        """
        # Check if the table_name has been used before
        if table_name in CrowdContext.__current_cd[self.local_db]:
            raise Exception("'%s' has been used before. "
                "Please choose a different name that is not in the list: "
                "[%s]" %(table_name, ",".join(CrowdContext.__current_cd[self.local_db])))
        else:
            CrowdContext.__current_cd[self.local_db].append(table_name)

        return CrowdJoin(object_list, table_name, self)


    def show_tables(self):
        """
        Return the list of the tables cached in the local database

        >>> cc.CrowdData(["image1.jpg", "image2.jpg"], "tmp1") #doctest: +ELLIPSIS
        <crowdbase.operators.crowddata.CrowdData instance at 0x...>
        >>> cc.CrowdJoin(["iphone 4", "ipad 2", "ipad two"], "tmp2") #doctest: +ELLIPSIS
        <crowdbase.operators.crowdjoin.CrowdJoin instance at 0x...>
        >>> tables = cc.show_tables()
        >>> print ", ".join(tables)
        tmp1, tmp2
        >>> cc.delete_tmp_tables()
        2
        """
        exe_str = "SELECT * FROM sqlite_master WHERE type='table'"
        self.cursor.execute(exe_str)
        results = self.cursor.fetchall()
        tables = []
        for result in results:
            tables.append(result[1])
        return tables


    def print_tables(self):
        """
        Print a sorted list of the tables cached in the local database (alphabetical order)

        >>> cc.CrowdData(["image1.jpg", "image2.jpg"], "tmp2") #doctest: +ELLIPSIS
        <crowdbase.operators.crowddata.CrowdData instance at 0x...>
        >>> cc.CrowdJoin(["iphone 4", "ipad 2", "ipad two"], "tmp1") #doctest: +ELLIPSIS
        <crowdbase.operators.crowdjoin.CrowdJoin instance at 0x...>
        >>> cc.print_tables()
        1 tmp1
        2 tmp2
        >>> cc.delete_tmp_tables()
        2
        """
        tables = self.show_tables()
        tables.sort()
        for i, table in enumerate(tables):
            print "%d %s" %(i+1, table)


    def rename_table(self, oldname, newname):
        """
        Rename a cached table

        >>> cc.CrowdData(["image1.jpg", "image2.jpg"], "tmp1") #doctest: +ELLIPSIS
        <crowdbase.operators.crowddata.CrowdData instance at 0x...>
        >>> cc.rename_table("tmp1", "tmp2")
        True
        >>> cc.print_tables()
        1 tmp2
        >>> cc.delete_tmp_tables()
        1
        """
        tables =  self.show_tables()
        if oldname not in tables:
            print "'%s' does not exist. " %(oldname)
            return False
        elif newname in tables:
            print "'%s'  has been used. Please choose another name. " %(newname)
            return False
        else:
            exe_str = "ALTER TABLE '%s' RENAME TO '%s'" %(oldname, newname)
            self.cursor.execute(exe_str)
            self.db.commit()
            # rename the table name in __current_cd
            for i, x in enumerate(CrowdContext.__current_cd[self.local_db]):
                if x == oldname:
                    CrowdContext.__current_cd[self.local_db][i] = newname
            return True


    def delete_table(self, table_name):
        """
        Delete a cached table

        >>> cc.CrowdData(["image1.jpg", "image2.jpg"], "tmp1") #doctest: +ELLIPSIS
        <crowdbase.operators.crowddata.CrowdData instance at 0x...>
        >>> cc.CrowdJoin(["iphone 4", "ipad 2", "ipad two"], "tmp2") #doctest: +ELLIPSIS
        <crowdbase.operators.crowdjoin.CrowdJoin instance at 0x...>
        >>> cc.print_tables()
        1 tmp1
        2 tmp2
        >>> cc.delete_table("tmp1")
        True
        >>> cc.print_tables()
        1 tmp2
        >>> cc.delete_tmp_tables()
        1
        """
        if table_name not in self.show_tables():
            print "'%s' does not exist " %(table_name)
            return False
        else:
            exe_str = "DROP TABLE '%s'" %(table_name)
            self.cursor.execute(exe_str)
            self.db.commit()
            # delete the table from __current_cd if exists
            if table_name in CrowdContext.__current_cd[self.local_db]:
                CrowdContext.__current_cd[self.local_db].remove(table_name)
            return True


    def delete_tmp_tables(self):
        """
        The function deletes all the tables whose names start with "tmp",
        and returns the number of deleted tables

        >>> cc.CrowdData(["image1.jpg", "image2.jpg"], "tmp1") #doctest: +ELLIPSIS
        <crowdbase.operators.crowddata.CrowdData instance at 0x...>
        >>> cc.CrowdJoin(["iphone 4", "ipad 2", "ipad two"], "not_tmp") #doctest: +ELLIPSIS
        <crowdbase.operators.crowdjoin.CrowdJoin instance at 0x...>
        >>> cc.delete_tmp_tables()
        1
        >>> cc.delete_table("not_tmp")
        True
        """
        # Remove the tmp tables from the database
        tables = self.show_tables()
        n = 0
        for table in tables:
            if table.startswith("tmp"):
                self.delete_table(table)
                n += 1
        # Remove the tmp tables from __current_cd
        old_list = CrowdContext.__current_cd[self.local_db]
        new_list = [x for x in  old_list if not x.startswith("tmp")]
        CrowdContext.__current_cd[self.local_db]  = new_list
        return n


    @staticmethod
    def remove_db_file(filename):
        """Remove a database file. Note that it is dangerous to remove a database.
            Please make sure you understand the meaning of the function and only use it
            if you have to.
        """
        try:
            os.remove(filename)
        except OSError:
            pass


    def __del__(self):
        try:
            self.cursor.close()
        except:
            pass

        try:
            self.db.close()
        except:
            pass


def _test():
    import doctest
    from crowdbase.crowdcontext import CrowdContext
    globs = globals().copy()
    test_db = 'crowdbase.test.db'
    CrowdContext.remove_db_file(test_db)
    globs['cc'] =  CrowdContext(local_db = test_db)
    (failure_count, test_count) = doctest.testmod(globs=globs)
    CrowdContext.remove_db_file(test_db)
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()





