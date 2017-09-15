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
import traceback
from sibispy import sibislogger as slog
from sibispy import session as sess

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
assert session.configure(config_file)

with sess.Capturing() as xnat_output: 
    assert (session.xnat_get_subject_attribute('blub','blub','blub') == None)
if "Error: XNAT api not defined" not in xnat_output.__str__():
    print "Error: session.xnat_get_subject_attribute: Test did not return correct error message"
    print xnat_output.__str__()
    sys.exit(1)

for project in ['xnat', 'data_entry','redcap_mysql_db'] :
    print "==== Testing " + project + " ====" 
    try : 
        server = session.connect_server(project, True)

        if not server:
            print "Error: could not connect server! Make sure " + project + " is correctly defined in " + config_file
            continue 

        if project == 'xnat':
            assert (session.xnat_export_general( 'xnat:subjectData', ['xnat:subjectData/SUBJECT_LABEL', 'xnat:subjectData/SUBJECT_ID','xnat:subjectData/PROJECT'], [ ('xnat:subjectData/SUBJECT_LABEL','LIKE', '%')],"subject_list") != None)
            with sess.Capturing() as xnat_output: 
                assert (session.xnat_get_subject_attribute('blub','blub','blub') == None)
            
            if "ERROR: attribute could not be found" not in xnat_output.__str__():
                print "Error: session.xnat_get_subject_attribute: Test did not return correct error message"
                print xnat_output.__str__()
                sys.exit(1)

        elif project == 'data_entry' :
            assert not server.export_fem( format='df' ).empty 
            assert len(server.export_records(fields=['study_id'],event_name='unique',format='df'))

        elif project == 'redcap_mysql_db' : 
            pd.read_sql_table('redcap_projects', server)

    except AssertionError:
        _, _, tb = sys.exc_info()
        # traceback.print_tb(tb) # Fixed format
        tb_info = traceback.extract_tb(tb)
        filename, line, func, text = tb_info[-1]
        print "Error: Assertion: occurred on line {} in statement '{}'".format(line, text)

    except Exception as err_msg: 
        print "Error: Failed to retrieve content from " + project + ". Server responded :"
        print str(err_msg)


print "Info: Time log writen to " + timeLogFile 


