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
from sibispy import config_file_parser as cfg_parser
import tempfile
import shutil

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

session = sess.Session()
assert session.configure(config_file)

# Make sure that config file is correctly defined 
assert(session.get_log_dir())

# Check that the file infrastructure is setup correctly
for DIR in [session.get_log_dir(), session.get_operations_dir(), session.get_xnat_dir(), session.get_cases_dir(), session.get_summaries_dir(), session.get_laptop_dir(), session.get_dvd_dir(), session.get_datadict_dir()] : 
    if not os.path.exists(DIR) : 
        print "ERROR: " + DIR + " does not exist" 
 
# Load in test specific settings : 
(sys_file_parser,err_msg) = session.get_config_sys_parser()
if err_msg :
    print "Error: session.get_config_sys_parser:" + err_msg
    sys.exit(1)
 
config_test_data = sys_file_parser.get_category('test_session')
if not config_test_data : 
    print "Warning: test_session specific settings not defined!"
    config_test_data = dict()

# Check that the servers are accessible 
with sess.Capturing() as xnat_output: 
    assert (session.xnat_get_subject_attribute('blub','blub','blub') == None)
if "Error: XNAT api not defined" not in xnat_output.__str__():
    print "Error: session.xnat_get_subject_attribute: Test did not return correct error message"
    print xnat_output.__str__()
    sys.exit(1)

for project in ['browser_penncnp', 'import_laptops', 'redcap_mysql_db', 'data_entry', 'xnat'] :
    print "==== Testing " + project + " ====" 
    try : 
        server = session.connect_server(project, True)

        if not server:
            print "Error: could not connect server! Make sure " + project + " is correctly defined in " + config_file
            continue 

        if project == 'xnat':
            # 1. XNAT Test: Non-Empty querry  
            with sess.Capturing() as xnat_output: 
                searchResult = session.xnat_export_general( 'xnat:subjectData', ['xnat:subjectData/SUBJECT_LABEL', 'xnat:subjectData/SUBJECT_ID','xnat:subjectData/PROJECT'], [ ('xnat:subjectData/SUBJECT_LABEL','LIKE', '%')],"subject_list")

            if xnat_output.__str__() != '[]' :
                print "Error: session.xnat_export_general: failed to perform querry"
                if '"err_msg": "Apache Tomcat' in xnat_output.__str__():
                    print "Info: username or password might be incorrect - check crudentials by using them to manually log in XNAT! "
                
                print xnat_output.__str__()

            elif searchResult == None : 
                print "Error: session.xnat_export_general: Test returned empty record"


            # 2. Stress Test:
            if "xnat_stress_test" in config_test_data.iterkeys() :  
                [xnat_eid, resource_id, resource_file_bname] = config_test_data["xnat_stress_test"].split('/')
                tmpdir = tempfile.mkdtemp()

                print "Start XNAT stress test ..." 
                slog.startTimer2() 
                # If fails, MIKE solution 
                server.select.experiment(xnat_eid).resource(resource_id).file(resource_file_bname).get_copy(os.path.join(tmpdir, "blub.tar.gz"))
                slog.takeTimer2("XNATStressTest","XNAT Stress Test")            
                print "... completed" 

                shutil.rmtree(tmpdir)
            else :
                print "Warning: Skipping XNAT stress test as it is not defined" 


            # 3. XNAT Test: Failed querry  
            with sess.Capturing() as xnat_output: 
                assert (session.xnat_get_subject_attribute('blub','blub','blub') == None)
                
            if "ERROR: attribute could not be found" not in xnat_output.__str__():
                print "Error: session.xnat_get_subject_attribute: Test returned wrong error message"
                print xnat_output.__str__()

            # no xnat tests after this one as it breaks the interface for some reason
            server = None


        elif project == 'browser_penncnp' :
            wait = session.initialize_penncnp_wait()
            assert session.get_penncnp_export_report(wait)

        elif project == 'import_laptops' :
            if "redcap_version_test" in config_test_data.iterkeys() :  
                (form_prefix, form_name) = config_test_data["redcap_version_test"].split(',')
                complete_label = '%s_complete' % form_name
                exclude_label = '%s_exclude' % form_prefix

                # If test fails Mike with message that redord_id is missing than it uses wrong redcap lib - use egg version  
                import_complete_records = server.export_records( fields = [complete_label, exclude_label], format='df' )
            else : 
                print "Warning: Skipping REDCap version test as it is not defined" 
            
        elif project == 'data_entry' :
            assert not server.export_fem( format='df' ).empty
            assert len(server.export_records(fields=['study_id'],event_name='unique',format='df'))

            if "redcap_stress_test" in config_test_data.iterkeys() :  
                all_forms = config_test_data["redcap_stress_test"]
                form_prefixes = all_forms.keys()
                form_names = all_forms.values()

                entry_data_fields = [('%s_complete' % form) for form in form_names] + [('%s_missing' % form) for form in form_prefixes] + [('%s_record_id' % form) for form in form_prefixes]
                entry_data_fields += ['study_id', 'dob', 'redcap_event_name', 'visit_date', 'exclude', 'sleep_date']
                entry_data_fields += ['parentreport_manual'] 
                print "Start REDCap stress test ..."  
                slog.startTimer2()  
                # If tests fails, Mike
                server.export_records(fields=entry_data_fields,event_name='unique',format='df')
                slog.takeTimer2("RCStressTest","REDCap Stress Test")            
                print ".... completed" 
            else : 
                print "Warning: Skipping REDCap stress test as it is not defined" 
        
        elif project == 'redcap_mysql_db' : 
            pd.read_sql_table('redcap_projects', server)
            # more detailed testing in test_redcap_locking_data

    except AssertionError:
        _, _, tb = sys.exc_info()
        # traceback.print_tb(tb) # Fixed format
        tb_info = traceback.extract_tb(tb)
        filename, line, func, text = tb_info[-1]
        print "Error: Assertion: occurred on line {} in statement '{}'".format(line, text)

    except Exception as err_msg: 
        print "Error: Failed to retrieve content from " + project + ". Server responded :"
        print str(err_msg)

    if project == 'browser_penncnp' :
        session.disconnect_penncnp()

print "Info: Time log writen to " + timeLogFile 


