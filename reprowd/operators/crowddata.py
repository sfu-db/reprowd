# -*- coding: utf-8 -*-

import pbclient
import sqlite3
import time
import dateutil.parser
from string import Template
from reprowd.quality.mv import MV
from reprowd.quality.em import EM


class CrowdData:

    """
        A CrowdData is the basic abstraction in Reprowd. It treats crowdsourcing
        as a process of manipulating a tabular dataset. For example, collecting results
        from the crowd is considered as adding a new column **result** to the data.

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
        Please call it through :func:`reprowd.crowdcontext.CrowdContext.CrowdData`.

        >>> object_list = ["image1.jpg", "image2.jpg"]
        >>> crowddata = cc.CrowdData(object_list, table_name = "test")  #doctest: +SKIP
        >>> crowddata  #doctest: +SKIP
        <reprowd.operators.crowddata.CrowdData instance at 0x...>
        >>> crowddata.cols  #doctest: +SKIP
        ['id', 'object']
        >>> crowddata.data  #doctest: +SKIP
        {'id': [0, 1], 'object': ['image1.jpg', 'image2.jpg']}
        """
        self.cc = crowdcontext
        self.data = {'id': range(len(object_list)), 'object':object_list}
        self.start_id = len(object_list)
        self.cols = ["id", "object"]
        self.table_name = table_name
        self.presenter = None
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
        Specify a presenter


        :param presenter: A Presenter object (e.g., :class:`reprowd.presenter.test.TextCmp`).
        :param map_func:  map_func() maps an object into the data format the presenter requires.
                                        If map_func() is not specified, it will use the default ``map_func = lambda obj: obj``
        :return: The updated CrowdData object

        >>> from reprowd.presenter.image import ImageLabel
        >>> object_list = ["image1.jpg", "image2.jpg"]
        >>> map_func = lambda obj: {'url_b':obj}
        >>> crowddata = cc.CrowdData(object_list, table_name = "test") \\  #doctest: +SKIP
        ...               .set_presenter(ImageLabel(), map_func)   #doctest: +SKIP
        >>> crowddata.cols   #doctest: +SKIP
        ['id', 'object']
        >>> crowddata.data  #doctest: +SKIP
        {'id': [0, 1], 'object': ['image1.jpg', 'image2.jpg']}
        >>> crowddata.presenter    #doctest: +SKIP
        <reprowd.presenter.image.ImageLabel object at 0x...>
        """
        self.map_func = map_func
        if self.map_func == None:
            self.map_func = lambda obj: obj
        self.presenter = presenter

        return self


    def __task_link(self, endpoint, task_id, project_short_name):
        return "%s/project/%s/task/%d" %(endpoint.strip('/'), task_id, project_short_name)


    def __init_project(self, presenter):
        """ Create a new project. If there exists a project corresponding to the presenter, use the existing project."""
        # Get the project id correponding to the input presenter
        # (if it does not exists, create a new one)
        if presenter == None:
            raise Exception("Cannot publish tasks without specifying a presenter."
                                    "Please use set_presenter() to specify a presenter.")
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

        try:
            self.project_id = p.id
            self.project_short_name = presenter.short_name
            self.project_name = presenter.name
            p= pbclient.find_project(short_name = presenter.short_name)[0]
            p.info['task_presenter'] = Template(presenter.template).safe_substitute(short_name = presenter.short_name, question = presenter.question)
            p.long_description = presenter.description
            p.name = presenter.name
            p.short_name = presenter.short_name
            pbclient.update_project(p)
        except:
            if type(p) is dict and "exception_msg" in p.keys():
                raise Exception("%s" %(p["exception_msg"]))
            else:
                print p
                raise

    def publish_task(self, n_assignments = 1, priority = 0):
        """
        Publish tasks to the pybossa server

        :param n_assignments: The number of assignments. For example, ``n_assignments`` = 3 means that each task needs to be done by three different workers
        :param priority:  A float number in [0, 1] that indicates the priority of the published tasks. The larger the value, the higher the priority.
        :return: The updated CrowdData object

        The function adds a new column **task** to the tabular dataset. Each item in the **task** column is a dict with the following attributes:

            - *id*: task id (e.g., 400)
            - *task_link*: the URL linked to a task (e.g., "http://localhost:7000/project/textcmp/task/400")
            - *task_data*: the data added to a task (e.g., {"url_b": "image1.jpg"})
            - *n_assignments*: the number of assignments (e.g., 3)
            - *priority*: the priority of a task (e.g., 0)
            - *project_id*: the project id (e.g., 155)
            - *create_time*: the time created a task (e.g., "2016-07-12T03:46:04.622127")

        >>> from reprowd.presenter.image import ImageLabel
        >>> object_list = ["image1.jpg", "image2.jpg"]
        >>> crowddata = cc.CrowdData(object_list, table_name = "test") \\  #doctest: +SKIP
        ...               .set_presenter(ImageLabel(), lambda obj: {'url_b':obj}) \\  #doctest: +SKIP
        ...               .publish_task()  #doctest: +SKIP
        >>> crowddata.cols  #doctest: +SKIP
        ['id', 'object', 'task']
        >>> print crowddata.data['task']  # print the task col #doctest: +SKIP
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
        >>> first_task = crowddata.data['task'][0]  #doctest: +SKIP
        >>> sorted(first_task.keys())  #doctest: +SKIP
        ['create_time', 'id', 'n_assignments', 'priority', 'project_id', 'task_data', 'task_link']
        """

        input_col = "object"
        output_col = "task"

        cursor = self.cc.cursor
        db = self.cc.db
        pbclient = self.cc.pbclient

        init_project_flag = False
        task_col = self.data.get(output_col, [None]*len(self.data["id"]))
        for k, (i, d) in enumerate(zip(self.data["id"], self.data[input_col])):
            exe_str = "SELECT * FROM '%s' WHERE id=? AND col_name=?" %(self.table_name)
            cursor.execute(exe_str, (i, output_col, ))
            records = cursor.fetchall()
            if records != []:
                assert len(records) == 1
                task_col[k] = eval(records[0][2])
                self.project_id = task_col[k]["project_id"]
                continue

            # Only initialize a project if the database does not contain all the tasks.
            if not init_project_flag:
                self.__init_project(self.presenter)
                init_project_flag = True

            task = pbclient.create_task(self.project_id, self.map_func(d), n_assignments, priority)
            if type(task) is dict and "exception_msg" in task.keys():
                raise Exception("%s" %(task["exception_msg"]))
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
            task_col[k] = format_task

        self.data[output_col] = task_col
        if output_col not in self.cols:
            self.cols.append(output_col)
        return self


    def __fetch_result(self, project_id):
        pbclient = self.cc.pbclient
        limit = 100
        last_id = 0
        tid_to_result = {}
        while True:
            try:
                results = pbclient.get_taskruns(project_id, limit = limit, last_id = last_id)
            except:
                print "Too many requests. Will try again in 10s..."
                time.sleep(10)

            if len(results) == 0:
                break

            for result in results:
                tid = result.data['task_id']
                if tid not in tid_to_result:
                    tid_to_result[tid] = []
                tid_to_result[tid].append(result.data)

            for result in results:
                trid = result.data['id']
                if trid > last_id:
                    last_id = trid

        return tid_to_result


    def get_result(self, blocking = True):
        """
        Get results from the pybossa server

        :param blocking: A boolean value that denotes whether the function will be blocked or not.

        - ``blocking = True`` means that the function will continuously collect results from the server until all the tasks are finished by the crowd.
        - ``blocking = False`` means that the function will get the current results of the tasks immediately even if they have not been finished yet.

        :return: The updated CrowdData object

        The function adds a new column **result** to the tabular dataset. Each item in the **result** column is a dict with the following attributes:

            - *assigments*: a list of assignments

                - *id*: assignment id (e.g., 100)
                - *worker_id*: an identifier of a worker (e.g., 2)
                - *worker_response*: the answer of the task given by the worker (e.g., "YES")
                - *start_time*: the time when the assignment is created (e.g., "2016-07-12T03:46:04.622127")
                - *finish_time*: the time when the assignment is finished (e.g., "2016-07-12T03:50:04.622127")

            - *task_id*: task id (e.g., 400)
            - *task_link*: the URL linked to a task (e.g., "http://localhost:7000/project/textcmp/task/400")
            - *project_id*: the project id (e.g., 155)


        >>> from reprowd.presenter.image import ImageLabel
        >>> object_list = ["image1.jpg"]
        >>> crowddata = cc.CrowdData(object_list, table_name = "test") \\ #doctest: +SKIP
        ...               .set_presenter(ImageLabel(), lambda obj: {'url_b':obj}) \\  #doctest: +SKIP
        ...               .publish_task().get_result(blocking=False)  #doctest: +SKIP
        >>> crowddata.cols  #doctest: +SKIP
        ['id', 'object', 'task', 'result']
        >>> d = crowddata.get_result(blocking=True).data  #doctest: +SKIP
        >>> print d['result'] # print the result col #doctest: +SKIP
        {
            'assignments': [
                {
                    'id': 236,
                    'worker_id': '1',
                    'worker_response': u'YES',
                    'start_time': u'2016-07-24T22:53:26.173976',
                    'finish_time': u'2016-07-24T22:53:26.173976'
                },
                {
                    id': 240,
                    'worker_id': u'10.0.2.2',
                     'worker_response': u'Yes',
                     'start_time': u'2016-07-24T22:53:33.943804',
                     'finish_time': u'2016-07-24T22:53:37.175170'
                }
            ]
            'task_id': 407,
            'task_link': 'http://localhost:7000/project/imglabel/task/407',
            'project_id': 190
        }
        """

        input_col = "task"
        output_col = "result"

        if input_col not in self.cols:
            raise Exception("Tasks have not been published. "
                                    "Please call publish_task() to publish tasks first.")

        assert len(self.data["id"]) == len(self.data[input_col])

        wait_time = 1 # Will be adaptively adjusted based on how fast workers are doing tasks
        result_col = self.data.get(output_col, [None]*len(self.data["id"]))
        while True:
            # load results from database
            for k, (i, task) in enumerate(zip(self.data["id"], self.data[input_col])):
                if result_col[k] == None:
                    exe_str = "SELECT * FROM '%s' WHERE id=? AND col_name=?" %(self.table_name)
                    self.cc.cursor.execute(exe_str, (i, output_col, ))
                    data = self.cc.cursor.fetchall()
                    if data != []:
                        assert len(data) == 1
                        result_col[k] = eval(data[0][2])

            # check if it is still necessary to fetch results from the pybossa server
            complete = True
            for task, result in zip(self.data[input_col], result_col):
                if task == None: # The task has not been published
                    continue
                if result == None or len(result["assignments"]) < task['n_assignments']:
                    complete = False
                    break
            if complete:
                break

            # fetch new results
            tid_to_result = self.__fetch_result(self.project_id)
            tid_to_format_result = {}

            # Update crowddata.data and database with new results
            for tid, results in tid_to_result.items():
                for result in results:
                    if tid not in tid_to_format_result:
                        tid_to_format_result[tid] = { \
                            "task_id": result["task_id"], \
                             "project_id": self.project_id, \
                             "task_link": self.__task_link(self.cc.endpoint, self.project_short_name, result["task_id"]), \
                             "assignments": []
                        }

                    tid_to_format_result[tid]["assignments"].append({ \
                        "id": result["id"], \
                        "worker_id": str(result["user_id"]) if result["user_id"] else result["user_ip"], \
                        "worker_response": result["info"], \
                        "start_time": result["created"], \
                        "finish_time": result["finish_time"]
                        })

            updated = False # updated = True means that workers did some new tasks during the waiting time
            for k, (i, task) in enumerate(zip(self.data["id"], self.data[input_col])):
                if task == None: # task has not been published
                    continue
                cache_result = result_col[k]
                new_result = tid_to_format_result.get(task['id'], None)
                if new_result == None: # the result of the task is not available
                    continue

                if (cache_result == None or len(cache_result["assignments"]) < len(new_result["assignments"])):
                    exe_str = "INSERT OR REPLACE INTO '%s' (id, col_name, value) VALUES(?,?,?)" %(self.table_name)
                    self.cc.cursor.execute(exe_str, (i, output_col, str(new_result), ))
                    self.cc.db.commit()
                    result_col[k] = new_result
                    updated = True

            if not updated:
                wait_time = wait_time * 2
                if wait_time > 16:
                    wait_time = 16
            else:
                wait_time = 1

            if blocking == False:
                break

            time.sleep(wait_time)

        self.data[output_col] = result_col
        if output_col not in self.cols:
            self.cols.append(output_col)
        return self


    def __em_col(self, result_col, **kwargs):
        """Using the [Dawid & Skene 1979]'s method"""
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
        """Using Majority Vote"""
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
        """
            Infer the final results using a quality-control method

            :param method: The name of the quality-control method
            :param \*\*kwargs: Parameters for he quality-control method
            :return: The updated CrowdData object

            Quality-control methods:
                - ``mv`` is short for Majority Vote.  It does not have any input parameter
                - ``em`` is short for Expectation-Maximization [Dawid & Skene 1979]. It has one input parameter: iteration (default: 20).

            The function adds a new column (e.g., **mv**, **em**) to the tabular dataset, which consists of the inferred result of each task.

            >>> from reprowd.presenter.image import ImageLabel
            >>> object_list = ["image1.jpg", "image2.jpg"]
            >>> crowddata = cc.CrowdData(object_list, table_name = "test") \\  #doctest: +SKIP
            ...               .set_presenter(ImageLabel(), lambda obj: {'url_b':obj}) \\  #doctest: +SKIP
            ...               .publish_task(n_assignments=3).get_result() \\  #doctest: +SKIP
            ...               .quality_control("em", iteration = 10) #doctest: +SKIP
            >>> crowddata.cols #doctest: +SKIP
            ['id', 'object', 'task', 'result', 'em']
            >>> crowddata.data["em"] #doctest: +SKIP
            ['YES', 'NO']
        """
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


    def append(self, object):
        """
            Add an object to the end of the  **object** column

            :param: An object that can be anything (e.g., int, string, dict)
            :return: The updated CrowdData object

            >>> from reprowd.presenter.image import ImageLabel
            >>> object_list = ["image1.jpg", "image2.jpg"]
            >>> crowddata = cc.CrowdData(object_list, table_name = "test") \\  #doctest: +SKIP
            ...               .set_presenter(ImageLabel(), lambda obj: {'url_b':obj}) \\  #doctest: +SKIP
            ...               .publish_task().get_result().quality_control("mv")  #doctest: +SKIP
            >>> crowddata.data["object"] #doctest: +SKIP
            ['image1.jpg', 'image2.jpg']
            >>> crowddata.append( 'image3.jpg')  #doctest: +SKIP
            >>> crowddata.data["object"] #doctest: +SKIP
            ['image1.jpg', 'image2.jpg', 'image3.jpg']
            >>> crowddata.publish_task().get_result().quality_control("mv") #doctest: +SKIP
            >>> crowddata.data["mv"] #doctest: +SKIP
            ['YES', 'YES', "NO"]
        """
        self.data['object'].append(object)
        self.data['id'].append(self.start_id)
        for col in self.cols:
            if col != 'object' and col != 'id':
                self.data[col].append(None)
        self.start_id += 1
        return self


    def extend(self, object_list):
        """
            Extend the list by appending all the objects in the given list

            :param: A list of objects where an object can be anything (e.g., int, string, dict)
            :return: The updated CrowdData object

            >>> from reprowd.presenter.image import ImageLabel
            >>> object_list = ["image1.jpg", "image2.jpg"]
            >>> crowddata = cc.CrowdData(object_list, table_name = "test") \\   #doctest: +SKIP
            ...  .set_presenter(ImageLabel(), lambda obj: {'url_b':obj}) \\  #doctest: +SKIP
            ...  .publish_task().get_result().quality_control("mv")  #doctest: +SKIP
            >>> crowddata.data["object"] #doctest: +SKIP
            ['image1.jpg', 'image2.jpg']
            >>> crowddata.extend(['image3.jpg', 'image3.jpg'])  #doctest: +SKIP
            >>> crowddata.data["object"] #doctest: +SKIP
            ['image1.jpg', 'image2.jpg', 'image3.jpg', 'image4.jpg']
            >>> crowddata.publish_task().get_result().quality_control("mv") #doctest: +SKIP
            >>> crowddata.data["mv"] #doctest: +SKIP
            ['YES', 'YES', "NO", 'NO']
        """
        self.data['object'].extend(object_list)
        self.data['id'].extend(range(self.start_id, self.start_id+len(object_list)))
        for col in self.cols:
            if col != 'object' and col != 'id':
                self.data[col].extend([None]*(len(self.data["id"] ) - len(self.data[col])))
        self.start_id += len(object_list)
        return self


    def filter(self, func):
        """
            Return the updated crowddata by selecting the rows, for which func returns True

            :param: A function that returns True for the selected rows
            :return: The updated CrowdData object

            >>> from reprowd.presenter.image import ImageLabel
            >>> object_list = ["image1.jpg", "image2.jpg"]
            >>> crowddata = cc.CrowdData(object_list, table_name = "test")  \\  #doctest: +SKIP
            ...               .set_presenter(ImageLabel(), lambda obj: {'url_b':obj}) \\  #doctest: +SKIP
            ...               .publish_task().get_result().quality_control("mv")  #doctest: +SKIP
            >>> crowddata.data["mv"] #doctest: +SKIP
            ['YES', 'NO']
            >>> crowddata.filter(lambda r: r["mv"] == 'YES' ) # Only keeps the images labled by 'YES' #doctest: +SKIP
            >>> crowddata.data["object"] #doctest: +SKIP
            ['image1.jpg']
        """
        n = len(self.data['id'])
        new_table = []
        for i in range(n):
            row = dict([(col, self.data[col][i]) for col in self.cols])
            if func(row):
                new_table.append(row)
        for col in self.cols:
            self.data[col] = []
            for row in new_table:
                self.data[col].append(row[col])
        return self


    def clear(self):
        """
            Remove all the rows in the crowddata

            :return: The updated CrowdData object

            >>> from reprowd.presenter.image import ImageLabel
            >>> object_list = ["image1.jpg", "image2.jpg"]
            >>> crowddata = cc.CrowdData(object_list, table_name = "test")  \\ #doctest: +SKIP
            ...               .set_presenter(ImageLabel(), lambda obj: {'url_b':obj}) \\  #doctest: +SKIP
            ...               .publish_task().get_result().quality_control("mv")  #doctest: +SKIP
            >>> crowddata.data["mv"] #doctest: +SKIP
            ['YES', 'NO']
            >>> crowddata.clear()  #doctest: +SKIP
            <reprowd.operators.crowddata.CrowdData instance at 0x...>
            >>> crowddata.data["object"] #doctest: +SKIP
            []
        """
        for col in self.cols:
            self.data[col] = []
        return self
