import pbclient
import sqlite3
import requests
import time

class CrowdData:
    def __init__(self, endpoint, api_key, data, presenter, crowd_data_name = "crowdbase",
                short_name = "short_name", description = "description"):
        self.cd_name = crowd_data_name
        id_list = [i for i in range(len(data))]
        self.cd = {'data':data, 'id_list':id_list, 'col_name':[], 'value':[], 'results':[]}
        self.presenter = presenter
        self.endpoint = endpoint
        pbclient.set('endpoint', endpoint)
        pbclient.set('api_key', api_key)
        self.cddb = sqlite3.connect('crowddata.db')
        self.cur = self.cddb.cursor()
        exe_str = "CREATE TABLE " + self.cd_name + " (id integer, col_name varchar(100), value integer PRIMARY KEY)"
        self.cur.execute(exe_str)
        p = pbclient.create_project(crowd_data_name, short_name, description)
        self.project_id = p.id
        p.info['task_presenter'] = open(presenter).read()
        pbclient.update_project(p)

    def createTask(self, task_name, n_answers = 30, priority_0 = 0, quorum = 0):
        for d in self.cd['id_list']:
            exe_str = "SELECT * FROM " + self.cd_name + " WHERE id=? AND col_name=?"
            self.cur.execute(exe_str, (d, task_name,))
            data = self.cur.fetchall()
            if (data == []):
                task_info = {'url_b': self.cd['data'][d]}
                task = pbclient.create_task(self.project_id, task_info, n_answers, priority_0, quorum)
                exe_str = "INSERT INTO " + self.cd_name + " VALUES(?,?,?)"
                self.cur.execute(exe_str, (d, task_name, task.id,))
                self.cddb.commit()
                self.cd['col_name'].append(task_name)
                self.cd['value'].append(task.id)
            else:
                self.cd['col_name'].append(data[0][1])
                self.cd['value'].append(data[0][2])
        return self

    def getTaskResult(self, result_col = "result_col"):
        try:
            exe_str = "ALTER TABLE " + self.cd_name + " ADD COLUMN " + result_col + " text DEFAULT NULL"
            self.cur.execute(exe_str)
            self.cddb.commit()
        except:
            print "Some tasks have existing results"
        for v in self.cd['value']:
            exe_str = "SELECT * FROM " + self.cd_name + " WHERE value=?"
            self.cur.execute(exe_str, (v,))
            data = self.cur.fetchall()
            if data[0][3] == None:
                url = self.endpoint + "/project/" + self.cd_name + "/" + str(v) + "/results.json"
                result = requests.get(url)
                while result.status_code != 200:
                    print "waiting for contribution"
                    time.sleep(60)
                    result = requests.get(url)
                result_json = result.json()
                exe_str = "UPDATE " + self.cd_name + " SET " + result_col + "=? WHERE value=?"
                self.cur.execute(exe_str, (str(result_json), v,))
                self.cddb.commit()
                self.cd['results'].append(result_json)
            else:
                self.cd['results'].append(eval(data[0][3]))
        return self

    def __del__(self):
        self.cur.close()
        self.cddb.close()

if __name__ == "__main__":
    data = ['http://farm4.static.flickr.com/3114/2524849923_1c191ef42e.jpg', 'http://www.7-star-admiral.com/0015_animals/0629_angora_hamster_clipart.jpg']
    cd = CrowdData('http://localhost:7000/', '8df67fd6-9c9b-4d32-a6ab-b0b5175aba30', data, "presenter.html", "test7", "test7", "test7")
    cd.createTask("task1", n_answers = 2).getTaskResult()
    print cd.cd["col_name"]
    print cd.cd["results"]
