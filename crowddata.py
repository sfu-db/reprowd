import pbclient
import sqlite3
import requests
import time

class CrowdData:
    def __init__(self, endpoint, api_key, data, presenter, crowd_data_name = "crowdbase",
                short_name = "short name", description = "description"):
        self.cd_name = crowd_data_name
        self.cd = {'data':data}
        self.presenter = presenter
        self.endpoint = endpoint
        pbclient.set('endpoint', endpoint)
        pbclient.set('api_key', api_key)
        self.cddb = sqlite3.connect('crowddata.db')
        self.cur = self.cddb.cursor()
        exe_str = "CREATE TABLE " + self.cd_name + " (id integer PRIMARY KEY AUTOINCREMENT, data varchar(100))"
        self.cur.execute(exe_str)
        for d in self.cd['data']:
            exe_str = "INSERT INTO " + self.cd_name + "(data) VALUES(?)"
            self.cur.execute(exe_str, (d,))
            self.cddb.commit()
        p = pbclient.create_project(crowd_data_name, short_name, description)
        self.project_id = p.id
        p.info['task_presenter'] = open(presenter).read()
        pbclient.update_project(p)

    def createTask(self, task_col = "task_col", n_answers = 30, priority_0 = 0, quorum = 0):
        try:
            exe_str = "ALTER TABLE " + self.cd_name + " ADD COLUMN " + task_col + " integer DEFAULT 0"
            self.cur.execute(exe_str)
            self.cddb.commit()
        except:
            print "Some tasks have already been created"
        exe_str = "SELECT * FROM " + self.cd_name
        self.cur.execute(exe_str)
        data = self.cur.fetchall()
        self.cd[task_col] = []
        for d in data:
            if d[2] == 0:
                task_info = {'url_b': d[1]}
                task = pbclient.create_task(self.project_id, task_info, n_answers, priority_0, quorum)
                exe_str = "UPDATE " + self.cd_name + " SET " + task_col + "=? WHERE id=?"
                print exe_str
                print task.id
                print d[0]
                self.cur.execute(exe_str, (task.id, d[0],))
                self.cddb.commit()
                self.cd[task_col].append(task.id)
        return self

    def getTaskResult(self, task_col = "task_col", result_col = "result_col"):
        try:
            exe_str = "ALTER TABLE " + self.cd_name + " ADD COLUMN " + result_col + " text DEFAULT NULL"
            self.cur.execute(exe_str)
            self.cddb.commit()
        except:
            print "Some tasks have existing results"
        exe_str = "SELECT * FROM " + self.cd_name
        self.cur.execute(exe_str)
        data = self.cur.fetchall()
        self.cd[result_col] = []
        for d in data:
            if d[3] == None:
                url = self.endpoint + "/project/" + self.cd_name + "/" + str(d[2]) + "/results.json"
                print url
                result = requests.get(url)
                while result.status_code != 200:
                    print "waiting for contribution"
                    time.sleep(60)
                    result = requests.get(url)
                result_json = result.json()
                exe_str = "UPDATE " + self.cd_name + " SET " + result_col + "=? WHERE id=?"
                self.cur.execute(exe_str, (str(result_json), d[0],))
                self.cddb.commit()
                self.cd[result_col].append(result_json)
            else:
                self.cd[result_col].append(eval(d[3]))
        return self

    def __del__(self):
        self.cur.close()
        self.cddb.close()

if __name__ == "__main__":
    data = ['http://farm4.static.flickr.com/3114/2524849923_1c191ef42e.jpg', 'http://www.7-star-admiral.com/0015_animals/0629_angora_hamster_clipart.jpg']
    cd = CrowdData('http://localhost:7000/', '8df67fd6-9c9b-4d32-a6ab-b0b5175aba30', data, "presenter.html", "test3", "test3", "test3")
    cd.createTask(n_answers = 1).getTaskResult()
    print cd.cd["task_col"]
    print cd.cd["result_col"]
