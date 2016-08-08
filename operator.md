---
layout: site
title: Operators API
---
# Operators API
{% include toc.md %}
## reprowd.operators.crowddata module
### class reprowd.operators.crowddata.CrowdData(object_list, table_name, crowdcontext)

* A CrowdData is the basic abstraction in Reprowd. It treats crowdsourcing as a process of manipulating a tabular dataset. For example, collecting results from the crowd is considered as adding a new column result to the data.
Furthermore, it provides fault recovery through the data manipulating process. That is, when the program crashes, the user can simply rerun the program as if it has never crashed.

* **__init__**(object_list, table_name, crowdcontext)
	* Initialize a tabular dataset and return a CrowdData object. The initial dataset has two columns: (id, object), where id is an auto-incremental key (starting from 0), and object is populated by the input object_list.
Note: It is not recommended to call the constructor directly. Please call it through reprowd.crowdcontext.CrowdContext.CrowdData().
	* **Example:**

			>>> object_list = ["image1.jpg", "image2.jpg"]
			>>> crowddata = cc.CrowdData(object_list, table_name = "test")  
			>>> crowddata  
			<reprowd.operators.crowddata.CrowdData instance at 0x...>
			>>> crowddata.cols  
			['id', 'object']
			>>> crowddata.data  
			{'id': [0, 1], 'object': ['image1.jpg', 'image2.jpg']}
* **set_presenter**(presenter, map_func=None)
	* Specify a presenter
	* **Parameters:**
		* **presenter** – A Presenter object (e.g., reprowd.presenter.test.TextCmp).
		* **map_func** – map_func() maps an object into the data format the presenter requires. If map_func() is not specified, it will use the default map_func = lambda obj: obj
	* **Returns:**	The updated CrowdData object
	* **Example:**

			>>> from reprowd.presenter.image import ImageLabel
			>>> object_list = ["image1.jpg", "image2.jpg"]
			>>> map_func = lambda obj: {'url_b':obj}
			>>> crowddata = cc.CrowdData(object_list, table_name = "test") \  
			...               .set_presenter(ImageLabel(), map_func)   
			>>> crowddata.cols   
			['id', 'object']
			>>> crowddata.data  
			{'id': [0, 1], 'object': ['image1.jpg', 'image2.jpg']}
			>>> crowddata.presenter    
			<reprowd.presenter.image.ImageLabel object at 0x...>
* **publish_task**(n_assignments=1, priority=0)
	* Publish tasks to the pybossa server
	* **Parameters:**
		* **n_assignments** – The number of assignments. For example, n_assignments = 3 means that each task needs to be done by three different workers
		* **priority** – A float number in [0, 1] that indicates the priority of the published tasks. The larger the value, the higher the priority.
	* **Returns:**	The updated CrowdData object
The function adds a new column task to the tabular dataset. Each item in the task column is a dict with the following attributes:
	* **Example:**

			id: task id (e.g., 400)
			task_link: the URL linked to a task (e.g., “http://localhost:7000/project/textcmp/task/400”)
			task_data: the data added to a task (e.g., {“url_b”: “image1.jpg”})
			n_assignments: the number of assignments (e.g., 3)
			priority: the priority of a task (e.g., 0)
			project_id: the project id (e.g., 155)
			create_time: the time created a task (e.g., “2016-07-12T03:46:04.622127”)

			>>> from reprowd.presenter.image import ImageLabel
			>>> object_list = ["image1.jpg", "image2.jpg"]
			>>> crowddata = cc.CrowdData(object_list, table_name = "test") \  
			...               .set_presenter(ImageLabel(), lambda obj: {'url_b':obj}) \  
			...               .publish_task()  
			>>> crowddata.cols  
			['id', 'object', 'task']
			>>> print crowddata.data['task']  # print the task col
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
* **get_result**(blocking=True)
	* Get results from the pybossa server

	* **Parameters:**
		* **blocking** – A boolean value that denotes whether the function will be blocked or not.
blocking = True means that the function will continuously collect results from the server until all the tasks are finished by the crowd.
blocking = False means that the function will get the current results of the tasks immediately even if they have not been finished yet.
	* **Returns:**	The updated CrowdData object
The function adds a new column result to the tabular dataset. Each item in the result column is a dict with the following attributes:
	* **Example:**

			assigments: a list of assignments

			id: assignment id (e.g., 100)
			worker_id: an identifier of a worker (e.g., 2)
			worker_response: the answer of the task given by the worker (e.g., “YES”)
			start_time: the time when the assignment is created (e.g., “2016-07-12T03:46:04.622127”)
			finish_time: the time when the assignment is finished (e.g., “2016-07-12T03:50:04.622127”)
			task_id: task id (e.g., 400)

			task_link: the URL linked to a task (e.g., “http://localhost:7000/project/textcmp/task/400”)

			project_id: the project id (e.g., 155)

			>>> from reprowd.presenter.image import ImageLabel
			>>> object_list = ["image1.jpg"]
			>>> crowddata = cc.CrowdData(object_list, table_name = "test") \
			...               .set_presenter(ImageLabel(), lambda obj: {'url_b':obj}) \  
			...               .publish_task().get_result(blocking=False)  
			>>> crowddata.cols  
			['id', 'object', 'task', 'result']
			>>> d = crowddata.get_result(blocking=True).data  
			>>> print d['result'] # print the result col
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
			    'task_link': "http://localhost:7000/project/imglabel/task/407"
			    'project_id': 190
			}
* **quality_control**(method='mv', **kwargs)
	* Infer the final results using a quality-control method
	* **Parameters:	**
		* **method** – The name of the quality-control method
		* ****kwargs** – Parameters for he quality-control method
	* **Returns:**	The updated CrowdData object
	* **Quality-control methods:**
		* **mv** is short for Majority Vote. It does not have any input parameter
		* **em** is short for Expectation-Maximization [Dawid & Skene 1979]. It has one input parameter: iteration (default: 20).
The function adds a new column (e.g., mv, em) to the tabular dataset, which consists of the inferred result of each task.
	* **Example:**

			>>> from reprowd.presenter.image import ImageLabel
			>>> object_list = ["image1.jpg", "image2.jpg"]
			>>> crowddata = cc.CrowdData(object_list, table_name = "test") \  
			...               .set_presenter(ImageLabel(), lambda obj: {'url_b':obj}) \  
			...               .publish_task(n_assignments=3).get_result() \  
			...               .quality_control("em", iteration = 10)
			>>> crowddata.cols
			['id', 'object', 'task', 'result', 'em']
			>>> crowddata.data["em"]
			['YES', 'NO']
			append(object)[source]
			Add an object to the end of the object column

			Param:	An object that can be anything (e.g., int, string, dict)
			Returns:	The updated CrowdData object
			>>> from reprowd.presenter.image import ImageLabel
			>>> object_list = ["image1.jpg", "image2.jpg"]
			>>> crowddata = cc.CrowdData(object_list, table_name = "test") \  
			...               .set_presenter(ImageLabel(), lambda obj: {'url_b':obj}) \  
			...               .publish_task().get_result().quality_control("mv")  
			>>> crowddata.data["object"]
			['image1.jpg', 'image2.jpg']
			>>> crowddata.append( 'image3.jpg')  
			>>> crowddata.data["object"]
			['image1.jpg', 'image2.jpg', 'image3.jpg']
			>>> crowddata.publish_task().get_result().quality_control("mv")
			>>> crowddata.data["mv"]
			['YES', 'YES', "NO"]
* **extend**(object_list)
	* Extend the list by appending all the objects in the given list
	* **Parameter:**	A list of objects where an object can be anything (e.g., int, string, dict)
	* **Returns:**	The updated CrowdData object
	* **Example:**

			>>> from reprowd.presenter.image import ImageLabel
			>>> object_list = ["image1.jpg", "image2.jpg"]
			>>> crowddata = cc.CrowdData(object_list, table_name = "test") \   
			...  .set_presenter(ImageLabel(), lambda obj: {'url_b':obj}) \  
			...  .publish_task().get_result().quality_control("mv")  
			>>> crowddata.data["object"]
			['image1.jpg', 'image2.jpg']
			>>> crowddata.extend(['image3.jpg', 'image3.jpg'])  
			>>> crowddata.data["object"]
			['image1.jpg', 'image2.jpg', 'image3.jpg', 'image4.jpg']
			>>> crowddata.publish_task().get_result().quality_control("mv")
			>>> crowddata.data["mv"]
			['YES', 'YES', "NO", 'NO']
* **filter**(func)
	* Return the updated crowddata by selecting the rows, for which func returns True
	* **Parameter:**	A function that returns True for the selected rows
	* **Returns:**	The updated CrowdData object
	* **Example:**

			>>> from reprowd.presenter.image import ImageLabel
			>>> object_list = ["image1.jpg", "image2.jpg"]
			>>> crowddata = cc.CrowdData(object_list, table_name = "test")  \  
			...               .set_presenter(ImageLabel(), lambda obj: {'url_b':obj}) \  
			...               .publish_task().get_result().quality_control("mv")  
			>>> crowddata.data["mv"]
			['YES', 'NO']
			>>> crowddata.filter(lambda r: r["mv"] == 'YES' ) # Only keeps the images labled by 'YES'
			>>> crowddata.data["object"]
			['image1.jpg']
* **clear()**
	* Remove all the rows in the crowddata
	* **Returns:**	The updated CrowdData object
	* **Example:**

			>>> from reprowd.presenter.image import ImageLabel
			>>> object_list = ["image1.jpg", "image2.jpg"]
			>>> crowddata = cc.CrowdData(object_list, table_name = "test")  \
			...               .set_presenter(ImageLabel(), lambda obj: {'url_b':obj}) \  
			...               .publish_task().get_result().quality_control("mv")  
			>>> crowddata.data["mv"]
			['YES', 'NO']
			>>> crowddata.clear()  
			<reprowd.operators.crowddata.CrowdData instance at 0x...>
			>>> crowddata.data["object"]
			[]
      ## reprowd.operators.crowdjoin module
      ### class reprowd.operators.crowdjoin.CrowdJoin(object_list, table_name, crowdcontext)
      * Given a list of objects (or two lists of objects), the CrowdJoin operator finds matching objects in the list (or between the two lists).
      * **__init__**(object_list, table_name, crowdcontext)
      	* Initialize a CrowdJoin object.
      	Note: It is not recommended to call the constructor directly. Please call it through reprowd.crowdcontext.CrowdContext.CrowdJoin().
      	* **Example:**

      			>>> object_list = ["iPad 2", "iPad Two", "iPhone 2", "iPad2"]
      			>>> cc.CrowdJoin(object_list, table_name = "jointest")  
      			<reprowd.operators.crowdjoin.CrowdJoin instance at 0x...>
      * **set_presenter**(presenter, map_func=<function <lambda>>)
      	* Specify a presenter
      	* **Parameters:**
      		* **presenter** – A Presenter object (e.g., reprowd.presenter.test.TextCmp).
      		* **map_func** – map_func() maps a pair of objects into the data format the presenter requires. If map_func is not specified, it will use the default map_func = lambda op: {'obj1':op[0], 'obj2':op[1]}
      	* **Returns:**	The updated CrowdJoin object
      	* **Example:**

      			>>> from reprowd.presenter.text import TextCmp
      			>>> object_list = ["iPad 2", "iPad Two", "iPhone 2", "iPad2"]
      			>>> def map_func(obj_pair):
      			...     o1, o2 = obj_pair
      			...     return {'obj1':o1, 'obj2':o2}
      			>>> cc.CrowdJoin(object_list, table_name = "jointest") \
      			...   .set_presenter(TextCmp(), map_func)
      			<reprowd.operators.crowdjoin.CrowdJoin instance at 0x...>
      * **set_simjoin**(joinkey_func, threshold=0.4, weight_on=False)
      	 * Set a simjoin operator
      	 * **Parameters:**
      		 * **joinkey_func** – A function that takes an object as input and outputs a join key on which simjoin will perform. The join key has to be a set of elements (e.g., bag of words, n-grams)
      		 * **threshold** – A float number in [0, 1]. The higher the value, the more the number of object pairs that are removed.
      		 * **weight_on** – A boolean value that indicates which similarity function, non-weighted Jaccard (weight_on = False) or weighted Jaccard (weight_on = True), will be used to compute similarity.
      	 * **Returns:**	The updated CrowdJoin object
           * **Note:**  Why do we need this? Consider a list of n objects. A naive implementation of CrowdJoin is to ask workers to label all n^2 object pairs. In fact, among these pairs, most of them look quite dissimilar and can be easily identified as non-matching pairs. Setting a simjoin operator will help us to remove these obviously non-matching pairs. Specifically, when it is set, all the object pairs whose Jaccard similarity values are below the threshold will be removed.
           * **Example:**

      				>>> from reprowd.presenter.text import TextCmp
      				>>> from reprowd.utils.simjoin import gramset
      				>>> object_list = ["iPad 2", "iPad Two", "iPhone 2", "iPad2"]
      				>>> def joinkey_func(obj):
      				...     # Use a 2-gram set as a joinkey
      				...     return gramset(obj, 2)
      				>>> crowdjoin = cc.CrowdJoin(object_list, table_name = "jointest") \
      				...               .set_presenter(TextCmp()) \
      				...               .set_simjoin(joinkey_func, 0.2)
      * **set_matcher**(matcher_func)
      	* Specify a function for determining which object pairs are matching
      	* **Parameters:**
      		* **matcher_func** – A function that takes a pair of objects as input and outputs True for matching pairs
      	* **Returns:**	The updated CrowdJoin object
      	* **Example:**

      			>>> from reprowd.presenter.text import TextCmp
      			>>> from reprowd.utils.simjoin import gramset, jaccard
      			>>> object_list = ["iPad 2", "iPad Two", "iPhone 2", "iPad2"]
      			>>> # Identify the pairs whose Jaccard similarity is above 0.9 as matching.
      			>>> def matcher_func(obj_pair):
      			...     o1, o2 = obj_pair
      			...     return jaccard(gramset(o1, 2), gramset(o2, 2)) >= 0.9
      			>>> crowdjoin = cc.CrowdJoin(object_list, table_name = "jointest") \
      			...               .set_presenter(TextCmp(), map_func) \
      			...               .set_matcher(matcher_func)
      * **set_nonmatcher**(nonmatcher_func)
      	* Specify a function for determining which object pairs are not matching
      	* **Parameters:**
      		* **nonmatcher_func** – A function that takes a pair of objects as input and outputs True for non-matching pairs
      	* **Returns:**	The updated CrowdJoin object
      	* **Example:**

      			>>> from reprowd.presenter.text import TextCmp
      			>>> from reprowd.utils.simjoin import gramset
      			>>> object_list = [("iPad 2", 300), ("iPad Two", 305), ("iPhone 2", 400), ("iPad2", 298)] # (name, price)
      			>>> def map_func(obj_pair):
      			...     o1, o2 = obj_pair
      			...     return {'obj1':o1[0] + " | " + str(o1[1]), 'obj2':o2[0] + " | " + str(o2[1])}
      			>>> # If the prices of two product differ by more than 80, they will be identified as a non-matching pair
      			>>> def nonmatcher_func(obj_pair):
      			...     o1, o2 = obj_pair
      			...     return abs(o1[1]-o2[1]) > 80
      			>>>
      			>>> crowdjoin = cc.CrowdJoin(object_list, table_name = "jointest") \
      			...               .set_presenter(TextCmp(), map_func) \
      			...               .set_nonmatcher(nonmatcher_func)
      * **set_transitivity**(score_func=None)
      	* Use transitivity to reduce the number of the pairs that need to be labeled by workers. Two types of transitivity will be considered:
      	If A and B are matching, B and C are matching, then A and C will be deduced as matching
      	If A and B are matching, B and C are non-matching, then A and C will be deduced as non-matching.
      	* **Parameters:**
      		* 	**score_func** – A score function that tends to return a high score for matching pairs and a low score for non-matching pairs. Having this function will increase the effectiveness of transitivity (See [Wang et al. SIGMOD 2013] for more detail).
      	* **Returns:**	The updated CrowdJoin object
      	* **Example:**

      			>>> from reprowd.presenter.text import TextCmp
      			>>> from reprowd.utils.simjoin import gramset, jaccard
      			>>> object_list = ["iPhone 2", "iPad 2", "iPad Two", "iPad2"]
      			>>> def score_func(obj_pair):
      			...     o1, o2 = obj_pair
      			...     return jaccard(gramset(o1, 2), gramset(o2, 2))
      			>>> crowdjoin = cc.CrowdJoin(object_list, table_name = "jointest") \
      			...               .set_presenter(TextCmp()) \
      			...               .set_transitivity(score_func)
      * **set_task_parameters**(n_assignments=1, priority=0)
      	* Set the values of the parameters for published tasks
      	* **Parameters:**
      		* **n_assignments** – The number of assignments. For example, n_assignments = 3 means that each task needs to be done by three different workers
      		* **priority** – A float number in [0, 1] that indicates the priority of the published tasks. The larger the value, the higher the priority.
      	* **Returns:**	The updated CrowdJoin object
      * **join**(other_object_list=None)
      	* If other_object_list is not specified, perform a self-join on self.object_list; otherwise, perform a join between self.object_list and other_object_list
      	* **Parameters:**
      		* **other_object_list** – A list of objects that will be joined with self.object_list
      	* **Returns:**	A dict with the following attributes:
      		* **“all”**: All the matching pairs
      		* **“human”**: A subset of matching pairs identified by humans
      		* **“machine”**: A subset of matching pairs identified by matcher_func() in set_matcher()
      		* **“transitivity”**: A subset of matching pairs deduced based on transitivity
      	* **Note: ** No matter in which order set_simjoin(), set_matcher(), and set_nonmatcher() are being called, they will be applied in the join() function in the following order:

      			set_simjoin()
      			set_nonmatcher()
      			set_matcher()

      			>>> from reprowd.presenter.text import TextCmp
      			>>> object_list = ["iPad 2", "iPad Two", "iPhone 2"]
      			>>> crowdjoin = cc.CrowdJoin(object_list, table_name = "jointest") \  
      			...               .set_presenter(TextCmp()) \
      			...               .join() # Ask workers to check all pairs
      			>>> matches['all']
      			[('iPad 2', 'iPad Two')]
      			>>> matches['human']
      			[('iPad 2', 'iPad Two')]
      			>>> matches['machine']
      			[]
      			>>> matches['transitivity']
      			[]
