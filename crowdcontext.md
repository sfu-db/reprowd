---
layout: site
title: CrowdContext API
---
# CrowdContext API

### class reprowd.crowdcontext.CrowdContext(endpoint=None, api_key=None, local_db='reprowd.db')

* Main entry point for Reprowd functionality. Intuitively, a CrowdContext can be thought of as a fault-tolerant and reproducible environment for doing crowdsourced data processing tasks. Once a CrowdContext is created, it will connect to a pybossa server and a local database, providing APIs for creating crowd operators(e.g., CrowdJoin), and manipulating cached crowddata.

* **__init__**(endpoint=None, api_key=None, local_db='reprowd.db')
    * Create a new CrowdContext. The endpoint and api_key should be set, either through the named parameters here or through environment variables ( REPROWD_ENDPOINT, REPROWD_API_KEY)
    * **Parameters:**
      * **endpoint** – Pybossa server URL (e.g. http://localhost:7000).
      * **api_key** – An api_key to access the pybossa server. You can get an api_key by creating an account in the pybossa server, and check the api_key of the account by clicking the “account name” –> “My Profile” on the top right of the page.
      * **local_db** – The local database name
    * **Returns :** A CrowdContext object
    * **Example:**
		    
		    >>> from reprowd.crowdcontext import CrowdContext
			>>> CrowdContext("http://localhost:7000", api_key = "test", local_db = "reprowd.test.db")
			<reprowd.crowdcontext.CrowdContext instance at 0x...>

* **CrowdData**(object_list, table_name)
    * Return CrowdData object
    * **Parameters:**
      * **object_list** – A list of objects where an object can be anything (e.g., int, string, dict)
      * **table_name** – The table used for caching the crowd tasks/results related to the CrowdData object
    * **Example:**
    
		    >>> # Create a CrowdData object for image labeling
	        >>> cc.CrowdData(["image1.jpg", "image2.jpg"], "tmp")   
	        <reprowd.operators.crowddata.CrowdData instance at 0x...>

* **CrowdJoin**(object_list, table_name)[source]
    * Return CrowdJoin object
    * **Parameters:**
      * **object_list** – A list of objects where an object can be anything (e.g., int, string, dict)
      * **table_name** – The table used for caching the crowd tasks/results related to the CrowdJoin object
    * **Example:**

	        >>> # Create a CrowdJoin object for deduplication
	        >>> cc.CrowdJoin(["iphone 4", "ipad 2", "ipad two"], "tmp")
	        <reprowd.operators.crowdjoin.CrowdJoin instance at 0x...>

* **show_tables()**
    * Return the list of the tables cached in the local database
    * **Example:**

	        >>> cc.CrowdData(["image1.jpg", "image2.jpg"], "tmp1")
	        <reprowd.operators.crowddata.CrowdData instance at 0x...>
	        >>> cc.CrowdJoin(["iphone 4", "ipad 2", "ipad two"], "tmp2")
	        <reprowd.operators.crowdjoin.CrowdJoin instance at 0x...>
	        >>> tables = cc.show_tables()
	        >>> print ", ".join(tables)
	        tmp1, tmp2
	        >>> cc.delete_tmp_tables()
	        2

* **print_tables()**
    * Print a sorted list of the tables cached in the local database (alphabetical order)
    * **Example:**

	        >>> cc.CrowdData(["image1.jpg", "image2.jpg"], "tmp2")
	        <reprowd.operators.crowddata.CrowdData instance at 0x...>
	        >>> cc.CrowdJoin(["iphone 4", "ipad 2", "ipad two"], "tmp1")
	        <reprowd.operators.crowdjoin.CrowdJoin instance at 0x...>
	        >>> cc.print_tables()
	        1 tmp1
	        2 tmp2
	        >>> cc.delete_tmp_tables()
	        2

* **rename_table**(oldname, newname)
    * Rename a cached table
    * **Example:**

	        >>> cc.CrowdData(["image1.jpg", "image2.jpg"], "tmp1")
	        <reprowd.operators.crowddata.CrowdData instance at 0x...>
	        >>> cc.rename_table("tmp1", "tmp2")
	        True
	        >>> cc.print_tables()
	        1 tmp2
	        >>> cc.delete_tmp_tables()
	        1

* **delete_table**(table_name)
    * Delete a cached table
    * **Example:**

	        >>> cc.CrowdData(["image1.jpg", "image2.jpg"], "tmp1")
	        <reprowd.operators.crowddata.CrowdData instance at 0x...>
	        >>> cc.CrowdJoin(["iphone 4", "ipad 2", "ipad two"], "tmp2")
	        <reprowd.operators.crowdjoin.CrowdJoin instance at 0x...>
	        >>> cc.print_tables()
	        1 tmp1
	        2 tmp2
	        >>> cc.delete_table("tmp1")
	        True
	        >>> cc.print_tables()
	        1 tmp2
	        >>> cc.delete_tmp_tables()
	        1

* **delete_tmp_tables()**
    * The function deletes all the tables whose names start with “tmp”, and returns the number of deleted tables
    * **Example:**

	        >>> cc.CrowdData(["image1.jpg", "image2.jpg"], "tmp1")
	        <reprowd.operators.crowddata.CrowdData instance at 0x...>
	        >>> cc.CrowdJoin(["iphone 4", "ipad 2", "ipad two"], "not_tmp")
	        <reprowd.operators.crowdjoin.CrowdJoin instance at 0x...>
	        >>> cc.delete_tmp_tables()
	        1
	        >>> cc.delete_table("not_tmp")
	        True

* **static remove_db_file(filename)**
    * Remove a database file. Note that it is dangerous to remove a database. Please make sure you understand the meaning of the function and only use it if you have to.
