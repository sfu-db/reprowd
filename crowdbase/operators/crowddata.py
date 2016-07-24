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
        A CrowdData is the basic abstraction in CrowdBase. It treats crowdsourcing
        as a process of manipulating a tabular dataset. For example, collecting results
        from the crowd can be considered as adding a new column `result` to the data.

        Furthermore, it provides fault recovery through the data manipulating process.
        That is, when the program crashes, the user can simply rerun the program as if it
        has never crashed.
    """

    def __init__(self, object_list, table_name, crowdcontext):
        """
        Initialize a tabular dataset and return a CrowdData object.
        The initial dataset has two columns: (**id**, **object**), where *id* is an auto-incremental key (starting from 0),
        and *object* is populated by the input ``object_list``.

        Note: It is not recommended to call the constructor directly.
        Please call it through :func:`crowdbase.crowdcontext.CrowdContext.CrowdData`.

        >>> object_list = ["image1.jpg", "image2.jpg"]
        >>> crowddata = cc.CrowdData(object_list, table_name = "tmp")
        >>> crowddata  #doctest: +ELLIPSIS
        <crowdbase.operators.crowddata.CrowdData instance at 0x...>
        >>> crowddata.data  #doctest: +SKIP
        {'id': [0, 1], 'object': ['image1.jpg', 'image2.jpg']}
        >>> cc.delete_tmp_tables()
        1
        """
        self.cc = crowdcontext
        self.data = {'id': range(len(object_list)), 'object':object_list}
        self.start_id = len(object_list)
        self.cols = ["id", "object"]
        self.table_name = table_name
        self.project_id = None
        self.project_short_name = None
        self.project_name = None

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
        Specify a presenter and return the updated CrowdData object. If the presenter does not exist, it will create a new project and link the presenter to the project.


        :param presenter: A Presenter object (e.g., :class:`crowdbase.presenter.test.TextCmp`).
        :param map_func:  map_func() transforms each object (in the *object_list*) to the data format the presenter requires.
                                        If map_func() is not specified, it will use the default map_func = lambda obj: obj

        >>> from crowdbase.presenter.image import ImageLabel
        >>> object_list = ["image1.jpg", "image2.jpg"]
        >>> map_func = lambda obj: {'url_b':obj}
        >>> crowddata = cc.CrowdData(object_list, table_name = "tmp").set_presenter(ImageLabel(), map_func)
        >>> crowddata  #doctest: +ELLIPSIS
        <crowdbase.operators.crowddata.CrowdData instance at 0x...>
        >>> crowddata.data  #doctest: +SKIP
        {'id': [0, 1], 'object': ['image1.jpg', 'image2.jpg']}
        >>> crowddata.project_name
        'Image Label'
        >>> cc.delete_tmp_tables()
        1
        """
        self.map_func = map_func
        if self.map_func == None:
            self.map_func = lambda obj: obj

        # Get the project id correponding to the input presenter
        # (if it does not exists, create a new one)
        pbclient = self.cc.pbclient
        p = None
        #try:
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
        self.project_name = presenter.name
        p.info['task_presenter'] = Template(presenter.template).safe_substitute(short_name = presenter.short_name)
        p.long_description = presenter.description
        p.name = presenter.name
        p.short_name = presenter.short_name
        pbclient.update_project(p)
        #except:
        #    print p
        #    raise Exception("Cannot connect the server. Please try again...")
        return self


    def __task_link(self, endpoint, task_id, project_short_name):
        return "%s/project/%s/task/%d" %(endpoint.strip('/'), task_id, project_short_name)


    def publish_task(self, n_assignments = 1, priority = 0):
        """
        Publish tasks to the pybossa server and return the updated CrowdData object.

        :param n_assignments: The number of assignments. For example, ``n_assignments`` = 3 means that each task needs to be done by three different workers
        :param priority:  A float number in [0, 1] that indicates the priority of the published tasks. The larger the value, the higher the priority.


        The function adds a new column **task** to the tabular dataset. Each value in the **task** column is a dict with the following attributes:

            - *id*: task id (e.g., 400)
            - *task_link*: the URL linked to a task (e.g., "http://localhost:7000/project/textcmp/task/400")
            - *task_data*: the data added to a task (e.g., {"url_b": "image1.jpg"})
            - *n_assignments*: the number of assignments (e.g., 3)
            - *priority*: the priority of a task (e.g., 0)
            - *project_id*: the project id (e.g., 155)
            - *create_time*: the time created a task (e.g., "2016-07-12T03:46:04.622127")



        >>> from crowdbase.presenter.image import ImageLabel
        >>> object_list = ["image1.jpg", "image2.jpg"]
        >>> crowddata = cc.CrowdData(object_list, table_name = "tmp").set_presenter(
        ...  ImageLabel(), lambda obj: {'url_b':obj}).publish_task()
        >>> crowddata  #doctest: +ELLIPSIS
        <crowdbase.operators.crowddata.CrowdData instance at 0x...>
        >>> crowddata.data.keys() #doctest: +SKIP
        ['id', 'object', 'task']
        >>> #
        >>> # print the **task** col
        >>> print crowddata.data['task'] #doctest: +SKIP
        [
            {
                'id': 91,
                'task_link': 'http://localhost:7000/project/imglabel/task/91',
                'task_data': {u'url_b': u'image1.jpg'},
                'n_assignments': 1,
                'priority': 0.0,
                'project_id': 78,
                'create_time': u'2016-07-23T23:52:50.551407',
            },
            {
                'id': 92,
                'task_link': 'http://localhost:7000/project/imglabel/task/92',
                'task_data': {u'url_b': u'image2.jpg'},
                'n_assignments': 1,
                'priority': 0.0,
                'project_id': 78,
                'create_time': u'2016-07-23T23:52:50.570035'
            }
        ]
        >>> first_task = crowddata.data['task'][0]
        >>> sorted(first_task.keys())
        ['create_time', 'id', 'n_assignments', 'priority', 'project_id', 'task_data', 'task_link']
        >>> cc.delete_tmp_tables()
        1
        """
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
                            "task_link": self.__task_link(self.cc.endpoint, self.project_short_name, task.data["id"]), \
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


    def __fetch_result(self, project_id):
        pbclient = self.cc.pbclient
        limit = 100
        last_id = 0
        tid_to_result = {}
        results = []
        while True:
            # The server has a limitation on the number of API calls in every 15mins,
            # so once the limit is reached, we will increase the waiting time progressively
            wait_time = 1
            while True:
                try:
                    results = pbclient.get_taskruns(project_id, limit = limit, last_id = last_id)
                    break
                except TypeError:
                    print "Being blocked by the server. Will fetch results again in %d mins..." %(wait_time)
                    time.sleep(wait_time*60)
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
                        "task_id": result["task_id"], \
                         "project_id": self.project_id, \
                         "task_link": self.__task_link(result["link"], self.project_short_name, result["task_id"]), \
                         "assignments": []
                    }

                tid_to_format_result[tid]["assignments"].append({ \
                    "id": result["id"], \
                    "worker_id": str(result["user_id"]) if result["user_id"] else result["user_ip"], \
                    "worker_response": result["info"], \
                    "start_time": result["created"], \
                    "finish_time": result["finish_time"], \
                    "duration": str(dateutil.parser.parse(result["finish_time"]) - dateutil.parser.parse(result["created"]))
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

            for task, result in zip(self.data[input_col], result_col):
                if result == None or len(result["assignments"]) < task['n_assignments']:
                    complete = False
                    break
            if complete:
                break
            time.sleep(10)

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














    def __em_col(self, result_col, **kwargs):

        task_to_worker_label = {}
        worker_to_task_label = {}
        label_set = []

        # Build up initial variables for em
        for result in result_col:
            if result == None:
                continue
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
        task_to_emlabel = em.quality_control(iteration)

        # Gather answers
        em_col = [None] * len(result_col)
        for i, r in enumerate(result_col):
            if r == None:
                continue
            tid = r["task_id"]
            em_col[i] = task_to_emlabel.get(tid, None)

        return em_col


    def __mv_col(self, result_col, **kwargs):
        task_to_label = {}

        # Build up initial variables for mv
        for result in result_col:
            if result == None:
                continue
            tid = result["task_id"]
            for a in result["assignments"]:
                wid = a["worker_id"]
                resp = a["worker_response"]
                task_to_label.setdefault(tid, []).append(resp)

        mv = MV(task_to_label)
        task_to_mvlabel = mv.quality_control()

        # Gather answers
        mv_col = [None] * len(result_col)
        for i, r in enumerate(result_col):
            if r == None:
                continue
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
            self.data[output_col] = self.__mv_col(self.data[input_col], **kwargs)
        elif method == "em":
            output_col = "em"
            self.data[output_col] = self.__em_col(self.data[input_col], **kwargs)
        if output_col == None:
            raise Exception(str(method)+" is not a valid input.")

        if output_col not in self.cols:
            self.cols.append(output_col)

        return self
