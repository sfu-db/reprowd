import pbclient
import sqlite3
import time
from collections import namedtuple


class CrowdData:
    ALL_COMPLETE, MV_COMPLETE = (0, 1) # stop_conditions

    def __init__(self, object_list, cache_table, crowd_context):

        self.cc = crowd_context

        self.table = {'id': range(len(object_list)), 'object':object_list}
        self.start_id = len(object_list)
        self.cols = ["id", "object"]
        self.cache_table = cache_table
        self.project_id = None

        if type(object_list) is not list:
            raise Exception("\"object_list\" should be a list ")

        if cache_table not in self.cc.show_tables():
            try:
                exe_str = "CREATE TABLE '%s' (id integer, col_name BLOB, value BLOB DEFAULT NULL, PRIMARY KEY(id, col_name))" %(cache_table)
                self.cc.cursor.execute(exe_str)
            except sqlite3.OperationalError:
                print "\"%s\" is an invalid name. Please avoid using \' in the name" %(cache_table)
                raise







    def append(self, object_list):
        self.table['object'].extend(object_list)
        self.table['id'].extend(range(self.start_id, self.start_id+len(object_list)))
        self.start_id += len(object_list)
        return self


    def filter(self, func):
        n = len(self.table['id'])
        print self.cols
        Row = namedtuple("Row", self.cols)
        new_table = []
        for i in range(n):
            row = Row(*[self.table[col][i] for col in self.cols])
            if func(row):
                new_table.append(row)

        for col in self.cols:
            self.table[col] = []
            for row in new_table:
                self.table[col].append(getattr(row,col))

        return self

    def clear(self):
        for col in self.cols:
            self.table[col] = []
        return self

    def map_to_presenter(self, presenter, map_func=lambda object: object):

        self.map_func = map_func

        # Get the project id correponding to the input presenter (if not exists, create a new one)
        pbclient = self.cc.pbclient
        if len(pbclient.find_project(short_name = presenter.short_name)) > 0: # the presenter has been created
            p= pbclient.find_project(short_name = presenter.short_name)[0]
            self.project_id = p.id
        elif  len(pbclient.find_project(name = presenter.name)) > 0: # the presenter has been created
            p= pbclient.find_project(name = presenter.name)[0]
            self.project_id = p.id
        else: # create a new project with the presente
            p = pbclient.create_project(presenter.name, presenter.short_name, presenter.description)
            self.project_id = p.id

        p.info['task_presenter'] = presenter.template
        p.long_description = presenter.description
        pbclient.update_project(p)

        return self



    def publish_task(self, n_answers = 1, priority_0 = 0, quorum = 0):

        if self.project_id == None:
            raise Exception("Cannot publish tasks without specifying a presenter."
                                    "Please use map_to_presenter() to specify a presenter.")

        input_col = "object"
        output_col = "task"
        if output_col not in self.cols:
            self.cols.append(output_col)
            self.table[output_col] = [None] * len(self.table["id"])
        else:
            self.table[output_col].extend([None]*(len(self.table["id"] ) - len(self.table[output_col])))

        self.n_answers = n_answers

        cursor = self.cc.cursor
        db = self.cc.db
        pbclient = self.cc.pbclient



        for k, (i, d) in enumerate(zip(self.table["id"], self.table[input_col])):
            exe_str = "SELECT * FROM '%s' WHERE id=? AND col_name=?" %(self.cache_table)
            cursor.execute(exe_str, (i, output_col, ))
            data = cursor.fetchall()
            if data != []:
                assert len(data) == 1
                self.table[output_col][k] = eval(data[0][2])
                continue
            task = pbclient.create_task(self.project_id, self.map_func(d), n_answers, priority_0, quorum)
            exe_str = "INSERT INTO '%s' VALUES(?,?,?)" %(self.cache_table)
            cursor.execute(exe_str, (i, output_col, str(task.data), ))
            db.commit()
            self.table[output_col][k] = task.data

        return self



    def _fetch_result(self, project_id):

        pbclient = self.cc.pbclient
        limit = 100
        last_id = 0
        taskid_to_result = {}
        i = 0
        while True:
            #results = pbclient.get_taskruns(project_id, limit = limit, last_id = last_taskid)
            i += 1
            try:
                results = pbclient.get_taskruns(project_id, limit = limit, last_id = last_id)
            except TypeError:
                print "Cannot connect the server. Try again..."
                break
            if len(results) == 0:
                break
            for result in results:
                taskid_to_result[result.data['task_id']] = result.data
            last_id += limit

        return taskid_to_result



    def _stop(self, result_col, n_answers, stop_condition):
        if stop_condition == CrowdData.ALL_COMPLETE:
            for result in result_col:
                if result == None or len(result) < n_answers:
                    return False
            return True
        elif stop_condition == CrowdData.MV_COMPLETE:
            assert (0)
        else:
            raise Exception("The %s is not defined." %(stop_condition))




    def get_result(self, loop_interval = 10, stop_condition = ALL_COMPLETE):
        input_col = "task"
        output_col = "result"

        if input_col not in self.cols:
            raise Exception("""The 'task' column does not exist. Please use the publish_task method to add a task column""")

        if output_col not in self.cols:
            self.cols.append(output_col)
            self.table[output_col] = [None] * len(self.table[input_col])
        else:
            self.table[output_col].extend([None]*(len(self.table["id"])-len(self.table[output_col])))


        assert len(self.table["id"]) == len(self.table[input_col])

        while True:
            taskid_to_result = self._fetch_result(self.project_id)

            for k, (i, task) in enumerate(zip(self.table["id"], self.table[input_col])):
                if self.table[output_col][k] == None:
                    exe_str = "SELECT * FROM '%s' WHERE id=? AND col_name=?" %(self.cache_table)
                    self.cc.cursor.execute(exe_str, (i, output_col, ))
                    data = self.cc.cursor.fetchall()
                    if data != []:
                        assert len(data) == 1
                        self.table[output_col][k] = eval(data[0][2])
                cache_result = self.table[output_col][k]
                new_result = taskid_to_result.get(task['id'], None)
                if new_result != None and (cache_result == None or len(cache_result) < len(new_result)):
                    exe_str = "INSERT OR REPLACE INTO " + self.cache_table + " (id, col_name, value) VALUES(?,?,?)"
                    self.cc.cursor.execute(exe_str, (i, output_col, str(new_result), ))
                    self.cc.db.commit()
                    self.table[output_col][k] = new_result

            if self._stop(self.table[output_col], self.n_answers,  stop_condition):
                break
            time.sleep(loop_interval)

        return self
