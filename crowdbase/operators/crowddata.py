# -*- coding: utf-8 -*-

import pbclient
import sqlite3
import time
import dateutil.parser
from string import Template
from collections import namedtuple
from crowdbase.quality.mv import MV
from crowdbase.quality.em import EM


class CrowdData:

    """
        A CrowdData is the basic abstraction in CrowdBase. It maps the process of
        crowdsourcing as manipulating a tabular dataset. For example, collecting results
        from the crowd can be considered as adding a new column `result` to the data.

        Furthermore, it provides fault recovery through the data manipulating process.
        That is, when the program crashes, the user can simply rerun the program as if it
        has never crashed.
    """

    def __init__(self, object_list, table_name, crowdcontext):
        """
        It is not recommended to use the constructor to create a CrowdData.
        Please use :func:`crowdbase.crowdcontext.CrowdContext.CrowdData` instead.
        """
        self.cc = crowdcontext
        self.data = {'id': range(len(object_list)), 'object':object_list}
        self.start_id = len(object_list)
        self.cols = ["id", "object"]
        self.table_name = table_name
        self.project_id = None
        self.project_short_name = None
        self.n_assignments = 1

        if type(object_list) is not list:
            raise Exception("'object_list' should be a list")
        if table_name not in self.cc.show_tables():
            try:
                exe_str = "CREATE TABLE '%s' (id integer, col_name BLOB, value BLOB DEFAULT NULL, PRIMARY KEY(id, col_name))" %(table_name)
                self.cc.cursor.execute(exe_str)
            except sqlite3.OperationalError:
                raise


    def set_presenter(self, presenter, map_func = None):
        """
        Specify a presenter for showing tasks to the crowd.

        :param presenter: A Presenter object (e.g., :class:`crowdbase.presenter.test.TextCmp`).
        :param map_func:  map_func() transforms each object (in the *object_list*) to the data format the presenter requires.
                                        If map_func() is not specified, it will use the default map_func = lambda obj: obj

        >>> object_list = ["image1.jpg", "image2.jpg"]
        >>> cc.CrowdData(object_list, table_name = "tmp")
        ...   .set_presenter(ImageLabel(), map_func = lambda obj: {'url_b':obj}) #doctest: +ELLIPSIS
        <crowdbase.operators.crowddata.CrowdData instance at 0x...>
        >>> cc.delete_tmp_tables()
        1
        """
        self.map_func = lambda obj: obj if map_func == None else map_func

        # Get the project id correponding to the input presenter
        # (if it does not exists, create a new one)
        pbclient = self.cc.pbclient
        try:
            if len(pbclient.find_project(short_name = presenter.short_name)) > 0:
                # the presenter has been created
                p= pbclient.find_project(short_name = presenter.short_name)[0]
            elif  len(pbclient.find_project(name = presenter.name)) > 0:
                # the presenter has been created
                p= pbclient.find_project(name = presenter.name)[0]
            else:
                # create a new project with the presente
                p = pbclient.create_project(presenter.name, presenter.short_name, presenter.description)

            self.project_id = p.id
            self.project_short_name = presenter.short_name
            p.info['task_presenter'] = Template(presenter.template).safe_substitute(short_name = presenter.short_name)
            p.long_description = presenter.description
            p.name = presenter.name
            p.short_name = presenter.short_name
            pbclient.update_project(p)
        except:
            print p
            raise Exception("Cannot connect the server. Please try again...")

        return self

    def publish_task(self, n_assignments = 1, priority = 0):

        if self.project_id == None:
            raise Exception("Cannot publish tasks without specifying a presenter."
                                    "Please use set_presenter() to specify a presenter.")

        input_col = "object"
        output_col = "task"
        if output_col not in self.cols:
            self.cols.append(output_col)
            self.data[output_col] = [None] * len(self.data["id"])
        else:
            self.data[output_col].extend([None]*(len(self.data["id"] ) - len(self.data[output_col])))

        self.n_assignments = n_assignments

        cursor = self.cc.cursor
        db = self.cc.db
        pbclient = self.cc.pbclient

        for k, (i, d) in enumerate(zip(self.data["id"], self.data[input_col])):
            exe_str = "SELECT * FROM '%s' WHERE id=? AND col_name=?" %(self.table_name)
            cursor.execute(exe_str, (i, output_col, ))
            data = cursor.fetchall()
            if data != []:
                assert len(data) == 1
                self.data[output_col][k] = eval(data[0][2])
                continue
            task = pbclient.create_task(self.project_id, self.map_func(d), n_assignments, priority)

            format_task = {"id": task.data["id"], \
                            "task_link": self.__task_link(task.data["link"], self.project_short_name, task.data["id"]), \
                            "task_data": task.data["info"], \
                            "n_assignments": task.data["n_answers"], \
                            "priority": task.data["priority_0"], \
                            "project_id": task.data["project_id"], \
                           "create_time": task.data["created"]
                           }

            # BUG. need to test some special chateracters, e.g., single quote.
            exe_str = "INSERT INTO '%s' VALUES(?,?,?)" %(self.table_name)
            cursor.execute(exe_str, (i, output_col, str(format_task), ))
            db.commit()
            self.data[output_col][k] = format_task

        return self


    def append(self, object_list):
        self.data['object'].extend(object_list)
        self.data['id'].extend(range(self.start_id, self.start_id+len(object_list)))
        self.start_id += len(object_list)
        return self


    def filter(self, func):
        n = len(self.data['id'])
        Row = namedtuple("Row", self.cols)
        new_table = []
        for i in range(n):
            row = Row(*[self.data[col][i] for col in self.cols])
            if func(row):
                new_table.append(row)
        for col in self.cols:
            self.data[col] = []
            for row in new_table:
                self.data[col].append(getattr(row,col))
        return self


    def clear(self):
        for col in self.cols:
            self.data[col] = []
        return self





    def __task_link(self, raw_text, task_id, project_short_name):
            pattern = r" href=\'(?P<endpoint>[ -~]+?)\/api"
            m = re.search(pattern, raw_text)
            endpoint =  m.group("endpoint")
            return "%sproject/%s/task/%d" %(endpoint, task_id, project_short_name)





    def __fetch_result(self, project_id):
        pbclient = self.cc.pbclient
        limit = 100
        last_id = 0
        tid_to_result = {}

        while True:
            # The server has a limitation on the number of API calls in every 15mins,
            # so once the limit is reached, we will increase the waiting time progressively
            wait_time = 10
            while True:
                try:
                    results = pbclient.get_taskruns(project_id, limit = limit, last_id = last_id)
                except TypeError:
                    print "Cannot connect the server. Will try again in %d secs..." %(wait_time)
                    time.sleep(wait_time)
                    wait_time *= 2
            if len(results) == 0:
                break
            for result in results:
                tid_to_result[result.data['task_id']] = result.data
            last_id += limit
        return tid_to_result


    def get_result(self, blocking = True):
        input_col = "task"
        output_col = "result"

        if input_col not in self.cols:
            raise Exception("Tasks have not been published. "
                                    "Please call publish_task() to publish tasks first.")

        if output_col not in self.cols:
            self.cols.append(output_col)
            self.data[output_col] = [None] * len(self.data[input_col])
        else:
            self.data[output_col].extend([None]*(len(self.data["id"])-len(self.data[output_col])))

        assert len(self.data["id"]) == len(self.data[input_col])
        while True:
            tid_to_result = self.__fetch_result(self.project_id)
            tid_to_format_result = {}

            for tid, result in tid_to_result.items():
                if tid not in tid_to_format_result:
                    tid_to_format_result[tid] = { \
                        "task_id": result.data["task_id"], \
                         "project_id": self.project_id, \
                         "task_link": self.__task_link(result.data["link"], self.project_short_name, result.data["task_id"]), \
                         "n_assignment": self.n_assignment, \
                         "assignments": []
                    }

                tid_to_format_result[tid]["assignments"].append({ \
                    "id": result.data["id"], \
                    "worker_id": str(result.data["user_id"]) if result.data["user_id"] else result.data["user_ip"], \
                    "worker_response": result.data["info"], \
                    "start_time": result.data["created"], \
                    "finish_time": result.data["finish_time"], \
                    "duration": str(dateutil.parser.parse(result.data["finish_time"]) - dateutil.parser.parse(result.data["created"]))
                    })

            for k, (i, task) in enumerate(zip(self.data["id"], self.data[input_col])):
                if self.data[output_col][k] == None:
                    exe_str = "SELECT * FROM '%s' WHERE id=? AND col_name=?" %(self.table_name)
                    self.cc.cursor.execute(exe_str, (i, output_col, ))
                    data = self.cc.cursor.fetchall()
                    if data != []:
                        assert len(data) == 1
                        self.data[output_col][k] = eval(data[0][2])
                cache_result = self.data[output_col][k]
                new_result = tid_to_format_result.get(task['id'], None)
                if new_result == None:
                    continue

                # Performance issue: Need to do a batch insertion
                if (cache_result == None or len(cache_result["assignments"]) < len(new_result["assignments"])):
                    exe_str = "INSERT OR REPLACE INTO " + self.table_name + " (id, col_name, value) VALUES(?,?,?)"
                    self.cc.cursor.execute(exe_str, (i, output_col, str(new_result), ))
                    self.cc.db.commit()
                    self.data[output_col][k] = new_result

            if blocking == False:
                break
            result_col = self.data[output_col]
            complete = True
            for result in result_col:
                if result == None or len(result["assignments"]) < self.n_assignments:
                    complete = False
                    break
            if complete:
                break

        return self


    def __em_col(self, result_col, **kwargs):

        task_to_worker_label = {}
        worker_to_task_label = {}
        label_set = []

        # Build up initial variables for em
        for result in result_col:
            tid = result["task_id"]
            for a in result["assignments"]:
                wid = a["worker_id"]
                resp = a["worker_response"]
                task_to_worker_label.setdefault(tid, []).append((wid, resp))
                worker_to_task_label.setdefault(wid, []).append((tid, resp))
                if resp not in label_set:
                    label_set.append(resp)

        # EM algorithm
        iteration = kwargs.get("iterartion", 20)
        em = EM(task_to_worker_label, worker_to_task_label, label_set)
        task_to_emlabel = em.quality_contral(iteration)

        # Gather answer
        em_col = [None] * len(result_col)
        for i, r in enumerate(result_col):
            tid = r["task_id"]
            em_col[i] = task_to_emlabel.get(tid, None)

        return em_col


    def __mv_col(self, result_col, **kwargs):
        task_to_label = {}

        # Build up initial variables for mv
        for result in result_col:
            tid = result["task_id"]
            for a in result["assignments"]:
                wid = a["worker_id"]
                resp = a["worker_response"]
                task_to_label.setdefault(tid, []).append(resp)

        mv = MajorityVote(task_to_label)
        task_to_mvlabel = mv.quality_control()

        # Gather answer
        mv_col = [None] * len(result_col)
        for i, r in enumerate(result_col):
            tid = r["task_id"]
            mv_col[i] = task_to_mvlabel.get(tid, None)

        return mv_col

    def quality_control(self, method = "mv", **kwargs):
        input_col = "result"
        if input_col not in self.cols:
            raise Exception("There is no result for quality control. "
                                    "Pease call get_result() to get results first.")

        output_col = None
        if method == "mv":
            output_col = "mv"
            self.data[output_col] = self.__mv_col(self.data[input_col], kwargs)
        elif method == "":
            output_col = "em"
            self.data[output_col] = self.__em_col(self.data[input_col], kwargs)
        if output_col == None:
            raise Exception(str(method)+" is not a valid input.")

        if output_col not in self.cols:
            self.cols.append(output_col)

        return self
