import sys, os
from os.path import dirname, abspath

sys.path.insert(0,    dirname(dirname(abspath(__file__))))


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
        self.presenter_repo = json.loads(open("presenter/presenter_repo.json").read())

        self.db = sqlite3.connect(cache_db)
        self.cursor = self.db.cursor()

        self.start_id = 0
        self.once_id = 0
        try:
            exe_str = "CREATE TABLE once (id integer, value BLOB DEFAULT NULL, PRIMARY KEY(id))"
            self.cursor.execute(exe_str)
        except:
            pass

    def once(self, func, d):
        exe_str = "SELECT * FROM once where id=?"
        self.cursor.execute(exe_str, (self.once_id,))
        data = self.cursor.fetchall()
        if data != []:
            assert len(data) == 1
            self.once_id += 1
            return data[0][1]
        else:
            re = func(d)
            # print "re"
            # print re
            exe_str = "INSERT INTO once VALUES(?,?)"
            self.cursor.execute(exe_str, (self.once_id, str(re),))
            self.once_id += 1
            return re

    def crowddata(self, object_list, cache_table):

        # Check if the cache_table name has been used before
        if cache_table in CrowdContext.all_cache_tables[self.cache_db]:
        	raise Exception("'%s' has been used before.  \
        		Please choose a different name that is not in the list: \
        		[%s]" %(cache_table, ",".join(CrowdContext.all_cache_tables[self.cache_db])))

        return CrowdData(object_list, cache_table, self)

    # def addPresenter():

    def __del__(self):
        self.cursor.close()
        self.db.close()




if __name__ == "__main__":

    print os.path.abspath(os.path.dirname(__file__))

    object_list = ['http://farm4.static.flickr.com/3114/2524849923_1c191ef42e.jpg', \
                         'http://www.7-star-admiral.com/0015_animals/0629_angora_hamster_clipart.jpg']
    cc = CrowdContext('http://localhost:7000/', '8df67fd6-9c9b-4d32-a6ab-b0b5175aba30')
    crowddata = cc.crowddata(object_list, cache_table = "flickr10") \
                            .map_to_presenter("imglabel", map_func = lambda obj: {'url_b':obj}) \
                             .publish_task().get_result()

    print crowddata.table["raw_object"]
    print crowddata.table["presenter_object"]
    print crowddata.table["task"]
    print crowddata.table["result"]


     #\
     # .map_to_presenter("imglabel", map_func = lambda obj: {'url_b':obj}) \
     # .publish_task().get_result()
