---
layout: site
title: Installation
---
# Installation and Setting Instructions

## Install Pybossa
1. [Install your own pybossa server with Juju](http://docs.pybossa.com/en/latest/juju_pybossa.html)
2. Install pybossa-client

        $pip install pybossa-client

## Install Reprowd
1. Install reprowd package

         $git clone https://github.com/sfu-db/reprowd.git
         $cd reprowd
         $python setup.py

## Setting
1. Get endpoint (e.g., http://localhost:7000/) and api_key by registering on pybossa server
2. Set the following user environment variables

        REPROWD_ENDPOINT="your-endpoint"
        REPROWD_API_KEY="your-api-key"
