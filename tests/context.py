# -*- coding: utf-8 -*-

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import crowdbase
import pbclient
from crowdbase.crowdcontext import CrowdContext



def init_context():
    # Create a crowdbase context
    local_db = "crowdbase.test.db"
    CrowdContext.remove_db_file(local_db)
    assert os.path.isfile(local_db) == False
    cc = CrowdContext(local_db = local_db)
    assert os.path.isfile(local_db) == True
    return cc

def delete_project(short_name = "", name = ""):
    p = pbclient.find_project(short_name = short_name)
    print p
    if len(p) > 0:
        pbclient.delete_project(p[0].id)
        return True
    p = pbclient.find_project(name = name)
    if len(p) > 0:
        pbclient.delete_project(p[0].id)
        return True
    return False


def destroy_context():
    # Clear up the context
    local_db = "crowdbase.test.db"
    assert os.path.isfile(local_db) == True
    CrowdContext.remove_db_file(local_db)
    assert True
