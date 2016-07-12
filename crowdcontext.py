from crowdbase.operators.crowddata import *
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


    def CrowdData(self, object_list, cache_table):
        # Check if the cache_table name has been used before
        if cache_table in CrowdContext.all_cache_tables[self.cache_db]:
        	raise Exception("'%s' has been used before.  \
        		Please choose a different name that is not in the list: \
        		[%s]" %(cache_table, ",".join(CrowdContext.all_cache_tables[self.cache_db])))

        return CrowdData(object_list, cache_table, self)


    def CrowdSelfJoin(self, object_list, cache_table):
        # Check if the cache_table name has been used before
        if cache_table in CrowdContext.all_cache_tables[self.cache_db]:
            raise Exception("'%s' has been used before.  \
                Please choose a different name that is not in the list: \
                [%s]" %(cache_table, ",".join(CrowdContext.all_cache_tables[self.cache_db])))

        return CrowdSelfJoin(object_list, cache_table, self)

    # def addPresenter():

    def __del__(self):
        self.cursor.close()
        self.db.close()





