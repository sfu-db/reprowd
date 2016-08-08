---
layout: site
title: CrowdContext API
---
# CrowdContext API

* class reprowd.crowdcontext.CrowdContext(endpoint=None, api_key=None, local_db='reprowd.db')

  Main entry point for Reprowd functionality. Intuitively, a CrowdContext can be thought of as a fault-tolerant and reproducible environment for doing crowdsourced data processing tasks. Once a CrowdContext is created, it will connect to a pybossa server and a local database, providing APIs for creating crowd operators(e.g., CrowdJoin), and manipulating cached crowddata.

  * __init__(endpoint=None, api_key=None, local_db='reprowd.db')

    Create a new CrowdContext. The endpoint and api_key should be set, either through the named parameters here or through environment variables ( REPROWD_ENDPOINT, REPROWD_API_KEY)

    <b> Parameters: </b>

      * endpoint – Pybossa server URL (e.g. http://localhost:7000).
      * api_key – An api_key to access the pybossa server. You can get an api_key by creating an account in the pybossa server, and check the api_key of the account by clicking the “account name” –> “My Profile” on the top right of the page.
      * local_db – The local database name

    <b> Returns : </b>

      * A CrowdContext object

    Example:

        > from reprowd.crowdcontext import CrowdContext
        > CrowdContext("http://localhost:7000", api_key = "test", local_db = "reprowd.test.db")  
        <reprowd.crowdcontext.CrowdContext instance at 0x...>
