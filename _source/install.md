---
layout: site
title: Downloading and Installation
---
# Downloading and Installation

## Pybossa
1. [Install your own pybossa server with Juju](http://docs.pybossa.com/en/latest/juju_pybossa.html) (Please ignore this step if you are using a public pybossa server)
2. Install [pybossa-client](https://github.com/PyBossa/pybossa-client)

        $pip install pybossa-client

## Reprowd
1. Install reprowd package

         $git clone https://github.com/sfu-db/reprowd.git
         $cd reprowd
         $python setup.py install


## Setting
1. Get endpoint (e.g., http://localhost:7000/) and api_key by registering on pybossa server
2. Set the following user environment variables. (You can also set them in your code using the [reprowd API](http://sfu-db.github.io/reprowd/docs/_build/html/crowdcontext.html#reprowd.crowdcontext.CrowdContext.__init__), but the drawback is that when you share the code to the public, everyone else will see your api-key.)  

        REPROWD_ENDPOINT="your-endpoint"
        REPROWD_API_KEY="your-api-key"
