#!/usr/bin/env python

##
##  Copyright 2017 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##

import os
import sys
import glob
import pandas

import sibispy
from sibispy import sibislogger as slog
from sibispy import redcap_to_casesdir as r2c

if sys.argv.__len__() > 1 : 
    config_file = sys.argv[1]
else :
    config_file = os.path.join(os.path.dirname(sys.argv[0]), 'data', '.sibis-general-config.yml')

slog.init_log(False, False,'test_check_dti_gradient', 'test_check_dti_gradient',None)

session = sibispy.Session()
assert(session.configure(config_file=config_file,ordered_config_load_flag = True))

redcap_project = session.connect_server('data_entry', True) 
assert(redcap_project)

red2cas = r2c.redcap_to_casesdir() 
if not red2cas.configure(session,redcap_project.metadata) :
    sys.exit(1)

outdir = "/tmp/test_redcap_to_casedir"
if not os.path.exists(outdir) :
    os.makedirs(outdir)
    
# Test creating data dictionaries
assert(red2cas.create_demographic_datadict(outdir))

# Test creating another form dictionary 
form_name = red2cas.get_export_form_names()[0]
assert(red2cas.create_datadict(form_name,outdir))

# Test writing out a case specific demographic file 
# NCANDA SPECIFIC
visit_log_fields = ['study_id', 'redcap_data_access_group', 'visit_date',
                    'mri_qa_completed', 'mri_t1_age', 'mri_dti_age',
                    'mri_rsfmri_age','mri_scanner', 'visit_ignore','mri_xnat_sid']
visit_log_redcap = redcap_project.export_records(fields=visit_log_fields,
                                                 event_name='unique',
                                                 export_data_access_groups=True,
                                                 format='df')
# also referred to as key
subject_key = visit_log_redcap.index[0]
subject_red_id =  subject_key[0]
subject_event_id =  subject_key[1]
subject_site_id = str(subject_red_id.split('-')[0])

# also referred to as row 
subject_visit_data = visit_log_redcap.ix[subject_key]
visit_date = str(subject_visit_data['visit_date'])
subject_red_visit_id =  subject_red_id + "-" + visit_date
subject_xnat_id =  str(subject_visit_data['mri_xnat_sid'])

baseline_events = ['baseline_visit_arm_1','baseline_visit_arm_4']
subject_fields = ['study_id', 'dob',  'exclude', 'enroll_exception',
                  'siblings_enrolled', 'siblings_id1', 'hispanic', 'race',
                  'race_other_code']
subject_data = redcap_project.export_records(fields=subject_fields,
                                             events=baseline_events,
                                             event_name='unique',
                                             format='df')
subject_data = pandas.concat([subject_data.xs(event, level=1) for event in baseline_events])


this_subject_data = subject_data.ix[subject_red_id]
subject_dob_str = str( this_subject_data['dob'] )

visit_age = red2cas.days_between_dates(subject_dob_str, visit_date ) / 365.242

#subject_code
(arm_code,visit_code,subject_datadir_rel) = red2cas.translate_subject_and_event(subject_xnat_id, subject_event_id)
assert(red2cas.export_demographics(subject_red_visit_id, subject_xnat_id,arm_code,visit_code,subject_site_id, visit_age , this_subject_data, subject_visit_data, outdir))

print "Wrote results to " + outdir
