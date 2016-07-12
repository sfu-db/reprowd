import pbclient
import sqlite3
import time


class CrowdData:
    ALL_COMPLETE, MV_COMPLETE = (0, 1) # stop_conditions

    def __init__(self, object_list, cache_table, crowd_context):

        self.cc = crowd_context

        self.table = {'id': range(len(object_list)), 'raw_object':object_list}
        self.cols = ["id", "raw_object"]
        self.cache_table = cache_table
        try:
            exe_str = "CREATE TABLE " + self.cache_table + " (id integer, col_name BLOB, value BLOB DEFAULT NULL, PRIMARY KEY(id, col_name))"
            self.cc.cursor.execute(exe_str)
        except:
            pass


    def map_to_presenter(self, presenter, map_func=lambda object: object):

        input_col = "raw_object"
        output_col = "presenter_object"
        self.cols.append(output_col)

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


        # Map the raw_object col to the presenter_object col.
        n = len(self.table['id'])
        self.table[output_col] = n * [None]
        for i, d in enumerate(self.table[input_col]):
            self.table[output_col][i] = map_func(d)

        return self



    def publish_task(self, n_answers = 1, priority_0 = 0, quorum = 0):
        input_col = "presenter_object"
        if input_col not in self.cols:
            raise Exception("""The 'presenter_object' column does not exist.
                Please use the map_to_presenter method to add a presenter_object column""")


        output_col = "task"
        self.cols.append(output_col)

        self.n_answers = n_answers

        cursor = self.cc.cursor
        db = self.cc.db
        pbclient = self.cc.pbclient

        self.table[output_col] = [None] * len(self.table[input_col])
        assert len(self.table["id"]) == len(self.table[input_col])

        for k, (i, d) in enumerate(zip(self.table["id"], self.table[input_col])):
            exe_str = "SELECT * FROM " + self.cache_table + " WHERE id=? AND col_name=?"
            cursor.execute(exe_str, (i, output_col, ))
            data = cursor.fetchall()
            if data != []:
                assert len(data) == 1
                self.table[output_col][k] = eval(data[0][2])
                continue
            # print d
            # print self.project_id
            task = pbclient.create_task(self.project_id, d, n_answers, priority_0, quorum)
            exe_str = "INSERT INTO " + self.cache_table + " VALUES(?,?,?)"
            cursor.execute(exe_str, (i, output_col, str(task.data), ))
            db.commit()
            self.table[output_col][k] = task.data

        return self



    def _fetch_result(self, project_id):

        pbclient = self.cc.pbclient
        limit = 100
        last_id = 0
        taskid_to_result = {}
        while True:
            #results = pbclient.get_taskruns(project_id, limit = limit, last_id = last_taskid)
            results = pbclient.get_taskruns(project_id, limit = limit, last_id = last_id)
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
            raise Exception("""The 'task' column does not exist.
                Please use the publish_task method to add a task column""")


        self.cols.append(output_col)
        self.table[output_col] = [None] * len(self.table[input_col])


        assert len(self.table["id"]) == len(self.table[input_col])

        while True:
            taskid_to_result = self._fetch_result(self.project_id)

            for k, (i, task) in enumerate(zip(self.table["id"], self.table[input_col])):

                if self.table[output_col][k] == None:
                    exe_str = "SELECT * FROM " + self.cache_table + " WHERE id=? AND col_name=?"
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
