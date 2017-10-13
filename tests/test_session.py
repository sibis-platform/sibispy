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

session = sess.Session()
assert session.configure(config_file)

# Make sure that config file is correctly defined 
assert(session.get_log_dir())

# Check that the file infrastructure is setup correctly
for DIR in [session.get_log_dir(), session.get_operations_dir(), session.get_xnat_dir(), session.get_cases_dir(), session.get_summaries_dir(), session.get_laptop_dir(), session.get_dvd_dir(), session.get_datadict_dir(), session.get_config_sys_file()] : 
    if not os.path.exists(DIR) : 
        print "ERROR: " + DIR + " does not exist" 
 
# Check that the servers are accessible 
with sess.Capturing() as xnat_output: 
    assert (session.xnat_get_subject_attribute('blub','blub','blub') == None)
if "Error: XNAT api not defined" not in xnat_output.__str__():
    print "Error: session.xnat_get_subject_attribute: Test did not return correct error message"
    print xnat_output.__str__()
    sys.exit(1)

all_forms = {
             # Forms for Arm 1: Standard Protocol
             'dd100': 'delayed_discounting_100',
             'dd1000': 'delayed_discounting_1000',

             'pasat': 'paced_auditory_serial_addition_test_pasat',
             'stroop': 'stroop',
             'ssaga_youth': 'ssaga_youth',
             'ssaga_parent': 'ssaga_parent',
             'youthreport1': 'youth_report_1',
             'youthreport1b': 'youth_report_1b',
             'youthreport2': 'youth_report_2',
             'parentreport': 'parent_report',
             'mrireport': 'mri_report',
             'plus': 'participant_last_use_summary',

             'myy': 'midyear_youth_interview',

             'lssaga1_youth': 'limesurvey_ssaga_part_1_youth',
             'lssaga2_youth': 'limesurvey_ssaga_part_2_youth',
             'lssaga3_youth': 'limesurvey_ssaga_part_3_youth',
             'lssaga4_youth': 'limesurvey_ssaga_part_4_youth',

             'lssaga1_parent': 'limesurvey_ssaga_part_1_parent',
             'lssaga2_parent': 'limesurvey_ssaga_part_2_parent',
             'lssaga3_parent': 'limesurvey_ssaga_part_3_parent',
             'lssaga4_parent': 'limesurvey_ssaga_part_4_parent',

             # Forms for Arm 3: Sleep Studies
             'sleepeve': 'sleep_study_evening_questionnaire',
             'sleeppre': 'sleep_study_presleep_questionnaire',
             'sleepmor': 'sleep_study_morning_questionnaire',

             # Forms for Recovery project
             'recq': 'recovery_questionnaire'}

for project in ['xnat', 'import_laptops', 'data_entry', 'redcap_mysql_db'] :
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

            # 2. XNAT Test: Failed querry  
            with sess.Capturing() as xnat_output: 
                assert (session.xnat_get_subject_attribute('blub','blub','blub') == None)
            
            if "ERROR: attribute could not be found" not in xnat_output.__str__():
                print "Error: session.xnat_get_subject_attribute: Test returned wrong error message"
                print xnat_output.__str__()

        elif project == 'import_laptops' :
            # Test that forms are accessible in import project
            (form_prefix, form_name) = all_forms.items()[0]
            complete_label = '%s_complete' % form_name
            exclude_label = '%s_exclude' % form_prefix
            fields_list = [complete_label, exclude_label]
            try :  
                import_complete_records = server.export_records( fields = fields_list, format='df' )
            except Exception as err_msg:
                    print "ERROR: Failed exporting", fields_list, "for form", form_name, "!"
                    print "Error msg from server:",  err_msg

        elif project == 'data_entry' :
            assert not server.export_fem( format='df' ).empty
            assert len(server.export_records(fields=['study_id'],event_name='unique',format='df'))

            form_prefixes = all_forms.keys()
            form_names = all_forms.values()

            entry_data_fields = [('%s_complete' % form) for form in form_names] + [('%s_missing' % form) for form in form_prefixes] + [('%s_record_id' % form) for form in form_prefixes]
            entry_data_fields += ['study_id', 'dob', 'redcap_event_name', 'visit_date', 'exclude', 'sleep_date']
            entry_data_fields += ['parentreport_manual'] 
            print "Start REDCap stress test ..."  
            slog.startTimer2()            
            server.export_records(fields=entry_data_fields,event_name='unique',format='df')
            slog.takeTimer2("RCStressTest","REDCap Stress Test")            
            print ".... completed" 
        
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


