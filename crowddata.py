
class CrowdData:
    cd = {"colname":"a list of values"} 
    cd_name = # The name of the CrowdData object 

    def __init__(self, data, "crowd_data_name"):
        """
            Here are a list of things to do in this function:
            1. self.cd_name = crowd_data_name
            2. cd["data"] = data
            3. initialize a table called "crowd_data_name" in the sqlite database. In the table, there are two columns and the types of them are string.
               They can be throught of as the hash key and the coresponding value. They serve as a cache to store the values (e.g., task ids and task results) returned from the crowdsourcing platform. 
        """

    def createTask(self, "data_col", presenter, "task_col"):
        """
            Here are a list of things to do in this function:
            1. check if task_col has already existed in the cache. If yes, read it from the database
            2. check if there are still some tasks that are not created. If yes, 
              2.1 create them and update the database (i.e., inserting hashkey:"task_col" and hashvalue:json.dump(task_ids) into the cache)
              2.2 cd["task_col"] = the list of task ids
            3. return self for chaining
        """

    def getTaskResult(self, "task_col", "result_col"):
        """
            task_col: This should be an existing column in the crowd_data. It represents the col name of the task id
            result_col: This is the newly added column. It represents the col name that you want to write the result to 

            Here are a list of things to do in this function:
            1. check if result_col has already existed in the cache. If yes, read if from the database 
            2 check if there are still some tasks whose results are not collected. If yes, 
              2.1 wait for their results and update the database (i.e., inserting hashkey:"result_col" and hashvalue:json.dump(task_results) into the cache)
              2.2 cd["result_col"] = the list of task results
            3. return self for chaining
        """


    def __def__(self):
        """
            close the connection of database
        """

if __name__ == "__main__":
    data = [("image1.jpg"), ("image2.jpg")]
    cd = CrowdData(data, "image_labeling")
    cd.createTask("data", presenter, "task_id").getTaskResult("task_id", "results")
