---
layout: site
title: Installation
---
# Installation and Setting Instructions

# Install Pybossa
1. [Install your own Pybossa Server with Juju](http://docs.pybossa.com/en/latest/juju_pybossa.html)
2. Install Pybossa-client

        $pip install pybossa-client

# Install Reprowd
  install Reprowd package

         $git clone https://github.com/sfu-db/reprowd.git
         $cd reprowd
         $python setup.py

# Setting
1. Get endpoint(http://localhost:7000/) and api_key by registering on pybossa server
2. Set python environment variables
  Add reprowd folder into your PythonPath environment variable
  Add endpoint and api_key into environment variable

        REPROWD_ENDPOINT="your-endpoint"
        REPROWD_API_KEY="your-api-key"
