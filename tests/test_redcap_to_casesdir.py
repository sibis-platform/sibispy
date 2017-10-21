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

slog.init_log(False, False,'test_redcap_to_casesdir', 'test_redcap_to_casesdir',None)

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

assert(red2cas.schedule_cluster_job('touch /tmp/blubber.log', 'test_redcap_to_casesdir', log_file= os.path.join(outdir,'cluster-test.log'), verbose = False))

# Test creating data dictionaries
assert(red2cas.create_demographic_datadict(outdir))

#
# Test writing out a case specific demographic file 
# NCANDA SPECIFIC
#
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
# subject_red_visit_id =  subject_red_id + "-" + visit_date
subject_xnat_id =  str(subject_visit_data['mri_xnat_sid'])


subject_fields = ['study_id', 'dob',  'exclude', 'enroll_exception',
                  'siblings_enrolled', 'siblings_id1', 'hispanic', 'race',
                  'race_other_code']
#baseline_events = ['baseline_visit_arm_1','baseline_visit_arm_4']
#for speed up just the single subject- records flag 
baseline_events = ['baseline_visit_arm_1']
subject_data = redcap_project.export_records(fields=subject_fields,
                                             events=baseline_events,
                                             event_name='unique',
                                             records = [subject_red_id],
                                             format='df')
subject_data = pandas.concat([subject_data.xs(event, level=1) for event in baseline_events])
this_subject_data = subject_data.ix[subject_red_id]

subject_dob_str = str( this_subject_data['dob'] )

visit_age = red2cas.days_between_dates(subject_dob_str, visit_date ) / 365.242

(arm_code,visit_code,subject_datadir_rel) = red2cas.translate_subject_and_event(subject_xnat_id, subject_event_id)
assert(red2cas.export_subject_demographics(subject_red_id, subject_xnat_id,arm_code,visit_code, subject_site_id, visit_age , this_subject_data, subject_visit_data, outdir))

#
# Test creating another form dictionary 
#
form_event_mapping = redcap_project.export_fem( format='df' )
forms_this_event = set( form_event_mapping[form_event_mapping['unique_event_name'] ==  subject_event_id ]['form_name'].tolist() )
form_name = next(iter(forms_this_event))
assert(red2cas.create_datadict(form_name,outdir))

#
# Test writing out a case specific measurement file 
# 


(all_records,export_list) = red2cas.get_subject_specific_form_data(subject_red_id, subject_event_id, forms_this_event, redcap_project, select_exports=[form_name])
assert((not all_records.empty) and export_list)

assert(red2cas.export_subject_form(form_name, subject_red_id, subject_xnat_id,arm_code,visit_code, all_records, outdir, verbose=True))

red2cas.export_subject_all_forms(redcap_project,  subject_site_id, subject_red_id, subject_event_id, this_subject_data, visit_age, subject_visit_data, arm_code, visit_code, subject_xnat_id, outdir,forms_this_event)

print "Wrote results to " + outdir
