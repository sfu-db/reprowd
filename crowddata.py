import pbclient
import sqlite3
import requests
import time

global_class_name = []#used to prevent duplicated class name
class CrowdData:
    def __init__(self, endpoint, api_key, data, presenter, crowd_data_name = "crowdbase",
                short_name = "short_name", description = "description"):
        while crowd_data_name in global_class_name:
            crowd_data_name += "_1"
            print "class name already eixsts, change it to _1"
        global_class_name.append(crowd_data_name)
        self.cd_name = crowd_data_name
        self.short_name = short_name
        self.description = description
        id_list = [i for i in range(len(data))]
        self.cd = {'data':data, 'id_list':id_list}
        self.flag = ["data", "id_list"]
        self.projects_presenter = {}
        self.task_presenter = {}

        # JN: I think we should require the user to input a presenter object (rather than a string). Otherwise, the user will not be able to use a customized presenter. We should discuss this point later.
        for p in presenter:
            self.projects_presenter[p] = -1

        self.endpoint = endpoint
        pbclient.set('endpoint', endpoint)
        pbclient.set('api_key', api_key)
        self.cddb = sqlite3.connect('crowddata.db')
        self.cur = self.cddb.cursor()
        try:
            exe_str = "CREATE TABLE " + self.cd_name + " (id integer, col_name varchar(100), value text DEFAULT NULL, PRIMARY KEY(id, col_name))"
            # jn: what if the table has already existed in the database?
            self.cur.execute(exe_str)
        except:
            print "The table already exists, rerun other functions to recover data"
            # shall we recover the data here, or by running other functions like createTask

                         # jn: see my previous comment on the presenter.
    def createTask(self, presenter, input_col = "data", output_col = "task", n_answers = 30, priority_0 = 0, quorum = 0):
        if input_col not in self.flag:
            print "input error1" #jn: need to return a more clear error info. We can work on this later.
            return
        while output_col in self.flag:
            print "output already exisits" #jn: need to return a more clear error info. We can work on this later
            output_col = output_col + "_1"
        self.flag.append(output_col)
        if self.projects_presenter[presenter] == -1:
            p = pbclient.create_project(self.cd_name + "_" + presenter, self.short_name + "_" + presenter, self.description + "_" + presenter)
            self.projects_presenter[presenter] = p.id
            p.info['task_presenter'] = open("presenter/" + presenter + ".html").read()
            pbclient.update_project(p)

        self.cd[output_col] = [None] * len(self.cd[input_col])
        assert len(self.cd["id_list"]) == len(self.cd[input_col])
        for k, (i, d) in enumerate(zip(self.cd["id_list"], self.cd[input_col])):
            exe_str = "SELECT * FROM " + self.cd_name + " WHERE id=? AND col_name=?"
            self.cur.execute(exe_str, (i, output_col, ))
            data = self.cur.fetchall()
            if data != []:
                assert len(data) == 1
                self.cd[output_col][k] = eval(data[0][2])
                continue

            task_info = {'url_b': d}
            task = pbclient.create_task(self.projects_presenter[presenter], task_info, n_answers, priority_0, quorum)
            exe_str = "INSERT INTO " + self.cd_name + " VALUES(?,?,?)"
            self.cur.execute(exe_str, (i, output_col, str(vars(task)['data']), ))
            self.cd[output_col][k] = vars(task)['data']
            self.task_presenter[str(task.id)] = presenter

        return self

    def getTaskResult(self, input_col = "task", output_col = "result"):
        if input_col not in self.flag:
            print "input error2"  #see my previous comment
            return
        while output_col in self.flag:
            print "output already exisits" #see my previous commemt
            output_col = output_col + "_1"
        self.flag.append(output_col)
        self.cd[output_col] = [None] * len(self.cd[input_col])

        assert len(self.cd["id_list"]) == len(self.cd[input_col])

        # I revised the following code to make sure every completed task can be stored in the self.cd right away
        rn = 0
        while rn < len(self.cd[input_col]):
            for k, (i, d) in enumerate(zip(self.cd["id_list"], self.cd[input_col])):
                exe_str = "SELECT * FROM " + self.cd_name + " WHERE id=? AND col_name=?"
                self.cur.execute(exe_str, (i, output_col, ))
                data = self.cur.fetchall()
                if data != []:
                    assert len(data) == 1
                    self.cd[output_col][k] = eval(data[0][2])
                    continue

                # jn: this part does not look very elegant. Does pbclient provide an API that can return the task result based on a given task id? If not, would be good to consider how to add one.
                url = self.endpoint + "project/" + self.cd_name + "_" + self.task_presenter[str(d['id'])] + "/" + str(d['id']) + "/results.json"
                print url
                number = 0
                result = requests.get(url)
                if result.status_code == 200:
                    result_json = result.json()
                    number = len(result_json)
                    print result_json
                    print number
                    print d['n_answers']
                assert number <= d['n_answers']
                if number == d['n_answers']:
                    rn += 1
                    exe_str = "INSERT INTO " + self.cd_name + " VALUES(?,?,?)"
                    self.cur.execute(exe_str, (i, output_col, str(result_json)))
                    self.cd[output_col][k] = result_json
            time.sleep(60)
        return self

    def __del__(self):
        self.cur.close()
        self.cddb.close()

if __name__ == "__main__":
    presenter = ['img', 'video', 'customized']
    data = ['http://farm4.static.flickr.com/3114/2524849923_1c191ef42e.jpg', 'http://www.7-star-admiral.com/0015_animals/0629_angora_hamster_clipart.jpg']
    cd = CrowdData('http://localhost:7000/', '8df67fd6-9c9b-4d32-a6ab-b0b5175aba30', data, presenter, "test12", "test12", "test12")
    cd.createTask("img", "data", "task", n_answers = 2).getTaskResult("task", "result")
    print cd.cd["task"]
    print cd.cd["result"]
