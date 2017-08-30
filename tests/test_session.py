#!/usr/bin/env python

##
##  Copyright 2016 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##

# if test script is run with argument then it will run script with the sibis config file defined by that argument 
# for example test_session.py ~/.sibis-general-config.yml 
# otherwise will run with data/.sibis-general-config.yml


import os
import pandas as pd
import sys
import sibispy
from sibispy import sibislogger as slog

#
# MAIN
#

if sys.argv.__len__() > 1 : 
    config_file = sys.argv[1]
else :
    config_file = os.path.join(os.path.dirname(sys.argv[0]), 'data', '.sibis-general-config.yml')

timeLogFile = '/tmp/test_session-time_log.csv'
if os.path.isfile(timeLogFile) : 
    os.remove(timeLogFile) 

slog.init_log(False, False,'test_session', 'test_session','/tmp')

session = sibispy.Session()
assert(session.configure(config_file))

assert(session.xnat_get_subject_attribute('blub','blub','blub') == None)

for project in ['xnat', 'data_entry','redcap_mysql_db'] :
    server = session.connect_server(project, True)
    if not server:
        print "Error: could not connect server! Make sure " + project + " is correctly defined in " + config_file
        continue 

    try :
        if project == 'xnat':
            assert(session.xnat_get_subject_attribute('blub','blub','blub') == None)
            session.xnat_export_general( 'xnat:subjectData', ['xnat:subjectData/SUBJECT_LABEL', 'xnat:subjectData/SUBJECT_ID','xnat:subjectData/PROJECT'], [ ('xnat:subjectData/SUBJECT_LABEL','LIKE', '%')],"subject_list")
        elif project == 'data_entry' :
            server.export_fem( format='df' )
        elif project == 'redcap_mysql_db' : 
            pd.read_sql_table('redcap_projects', server)

    except Exception as err_msg: 
        print "Error: Failed to retrieve content from " + project + ". Server responded :"
        print str(err_msg)


print "Info: Time log writen to " + timeLogFile 


