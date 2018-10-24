#!/usr/bin/env python

##
##  Copyright 2017 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##

from __future__ import print_function
import os
import sys
import glob
import pandas

import sibispy
from sibispy import sibislogger as slog
from sibispy import redcap_locking_data 

if sys.argv.__len__() > 1 : 
    config_file = sys.argv[1]
else :
    config_file = os.path.join(os.path.dirname(sys.argv[0]), 'data', '.sibis-general-config.yml')

slog.init_log(False, False,'test_redcap_locking_data', 'test_redcap_locking_data',None)

session = sibispy.Session()
assert(session.configure(config_file=config_file))

redcap_project = session.connect_server('data_entry', True) 
assert(redcap_project)
form_key = session.get_redcap_form_key()

assert(session.connect_server('redcap_mysql_db', True)) 
red_lock = redcap_locking_data.redcap_locking_data()
red_lock.configure(session)

event_def = redcap_project.events[0]
event_unique = str(event_def['unique_event_name'])
event_name = str(event_def['event_name'])

form_event_mapping = redcap_project.export_fem( format='df' )
forms = form_event_mapping[form_event_mapping.unique_event_name == event_unique][form_key].tolist()

arm_name = str(redcap_project.arm_names[0])

# Load in test specific settings : 
(sys_file_parser,err_msg) = session.get_config_test_parser()
if err_msg :
    print("Error: session.get_config_test_parser:" + err_msg)
    sys.exit(1)
 
config_test_data = sys_file_parser.get_category('test_redcap_locking_data')
if not config_test_data : 
    print("Error: test_session specific settings not defined!")
    sys.exit(1)

project_name = config_test_data.get('project_name')

all_subject_ids = session.get_mysql_project_records(project_name,arm_name, event_name) 
# if it returned no subject ids than something went wrong
assert(not all_subject_ids.empty)
test_subject = all_subject_ids.record.iloc[0]
print(red_lock.report_locked_forms(test_subject,test_subject, forms, project_name, arm_name, event_name))


