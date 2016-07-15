from crowdbase.operator.crowddata import *
from crowdbase.operator.crowdjoin import CrowdJoin
import sqlite3
import pbclient
import json

class CrowdContext:

    all_cache_tables = {}

    def __init__(self, endpoint, api_key, cache_db = "crowdbase.db"):
        self.cache_db = cache_db
        pbclient.set('endpoint', endpoint)
        pbclient.set('api_key', api_key)
        self.pbclient = pbclient

        CrowdContext.all_cache_tables[cache_db] = []

        self.db = sqlite3.connect(cache_db)
        self.cursor = self.db.cursor()


    def list_cache_tables(self):
        exe_str = "SELECT * FROM sqlite_master WHERE type='table'"
        self.cursor.execute(exe_str)
        results = self.cursor.fetchall()
        tables = []
        for result in results:
            tables.append(result[1])
        return tables

    def print_cache_tables(self):
        tables = self.list_cache_tables()
        tables.sort()
        for table in tables:
            print "*", table

    def rename_cache_table(self, oldname, newname):
        cache_tables =  self.list_cache_tables()
        if oldname not in cache_tables:
            print "'%s' does not exist. " %(oldname)
        elif newname in cache_tables:
            print "'%s'  has been used. Please choose another name. " %(newname)
        else:
            exe_str = "ALTER TABLE '%s' RENAME TO '%s'" %(oldname, newname)
            self.cursor.execute(exe_str)
            self.db.commit()

    def delete_cache_tables(self, *names):
        for name in names:
            if name not in self.list_cache_tables():
                print "'%s' does not exist " %(name)
            else:
                exe_str = "DROP TABLE '%s'" %(name)
                self.cursor.execute(exe_str)
                self.db.commit()



    def CrowdData(self, object_list, cache_table):
        # Check if the cache_table name has been used before
        if cache_table in CrowdContext.all_cache_tables[self.cache_db]:
        	raise Exception("'%s' has been used before.  \
        		Please choose a different name that is not in the list: \
        		[%s]" %(cache_table, ",".join(CrowdContext.all_cache_tables[self.cache_db])))

        return CrowdData(object_list, cache_table, self)


    def CrowdJoin(self, object_list, cache_table):
        # Check if the cache_table name has been used before
        if cache_table in CrowdContext.all_cache_tables[self.cache_db]:
            raise Exception("'%s' has been used before.  \
                Please choose a different name that is not in the list: \
                [%s]" %(cache_table, ",".join(CrowdContext.all_cache_tables[self.cache_db])))

        return CrowdJoin(object_list, cache_table, self)

    # def addPresenter():

    def __del__(self):
        self.cursor.close()
        self.db.close()





