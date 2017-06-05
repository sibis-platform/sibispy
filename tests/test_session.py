#!/usr/bin/env python

##
##  Copyright 2016 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##
import os
import pandas as pd
import sys
import sibispy
from sibispy import sibislogger as slog

timeLogFile = '/tmp/test_session-time_log.csv'
if os.path.isfile(timeLogFile) : 
    os.remove(timeLogFile) 

slog.init_log(False, False,'test_session', 'test_session','/tmp')


path = os.path.join(os.path.dirname(sys.argv[0]), 'data', '.sibis-general-config.yml')

def test_session_init_path():
    # setting explicitly
    session = sibispy.Session()
    assert(session.configure(config_file=path))
    assert(session.config_file == path)

test_session_init_path()

# Test when variable is set 
os.environ.update(SIBIS_CONFIG=path)
session = sibispy.Session()
assert(session.configure())
os.environ.pop('SIBIS_CONFIG')
assert(session.config_file == path)

for project in ['xnat', 'data_entry','redcap_mysql_db'] :
    server = session.connect_server(project, True)
    if not server:
        print "Error: could not connect server! Make sure " + project + " is correctly defined in " + path 

    try :
        if project == 'xnat': 
            server._get_json('/data/config/pyxnat/check_new_sessions')
        elif project == 'data_entry' :
            server.export_fem( format='df' )
        elif project == 'redcap_mysql_db' : 
            pd.read_sql_table('redcap_projects', server)

    except Exception as err_msg: 
        print "Error: Failed to retrieve content from " + project + ". Server responded :"
        print str(err_msg)


print "Info: Time log writen to " + timeLogFile 


