#!/usr/bin/env python

##
##  Copyright 2017 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##

from __future__ import print_function
from __future__ import division
from builtins import next
from builtins import str
from past.utils import old_div
import os
import sys
import glob
import pandas

import sibispy
from sibispy import sibislogger as slog
from sibispy import redcap_to_casesdir as r2c
import pwd

# =============================
# Test Functions 
# =============================

def test_cluster():
  cluster_submit_log = os.path.join(outdir,'cluster-submit.log')
  cluster_job_log = os.path.join(session.get_beta_dir(),'cluster-job.log')
  if os.path.exists(cluster_job_log):
     os.remove(cluster_job_log)

  cluster_test_file = os.path.join(session.get_cases_dir(),"test_redcap_to_casesdir.cluster")
  user_id =  os.getuid()
  user_name = pwd.getpwuid(user_id )[ 0 ]
  cmd = 'echo "Submitting User:' + user_name + '(' + str(user_id) + ')"; echo "Exe  cuting User: ${LOGNAME}(${UID})"; touch ' + str(cluster_test_file) + '; rm -f '  + str(cluster_test_file)

  print("== test_cluster only needs to work on backend ==")
  assert(red2cas.schedule_cluster_job(cmd, "test_redcap_to_casesdir", submit_log=cluster_submit_log, job_log=cluster_job_log, verbose = False))
  print("Please check " +  cluster_job_log + " if cluster job was successfully executed !")

#=====================================
def test_save_demographics_to_file():
  assert(red2cas.create_demographic_datadict(outdir))
  
  # Note : output writen to file by this function does not necessarily respond to real subject data 
  assert(red2cas.export_subject_demographics(subject_red_id, subject_xnat_id,arm_code,visit_code, subject_site_id, visit_age , this_subject_data, subject_visit_data, -1, -1, -1, outdir))
  
#=====================================
def test_save_form_to_file(name_of_form):

  assert(red2cas.create_datadict(name_of_form,outdir))
  
  # Test writing out a case specific measurement file
  
  (all_records,export_list) = red2cas.get_subject_specific_form_data(subject_red_id, subject_event_id, forms_this_event, redcap_project, select_exports=[name_of_form])
  assert((not all_records.empty) and export_list)
  
  assert(red2cas.export_subject_form(name_of_form, subject_red_id, subject_xnat_id,arm_code,visit_code, all_records, outdir, verbose=True))
  
  red2cas.export_subject_all_forms(redcap_project,  subject_site_id, subject_red_id, subject_event_id, this_subject_data, visit_age, subject_visit_data, arm_code, visit_code, subject_xnat_id, outdir,forms_this_event, -1, -1, -1)

# =============================
# Main 
# =============================
specific_subject=None
if sys.argv.__len__() > 1 :
    config_file = sys.argv[1]
    # execute for a specific subject, e.g. B-00000-M-6
    if sys.argv.__len__() > 2 :
      specific_subject=[sys.argv[2]]
else :
    config_file = os.path.join(os.path.expanduser("~"),'.sibis-general-config.yml')


slog.init_log(False, False,'test_redcap_to_casesdir', 'test_redcap_to_casesdir',None)

session = sibispy.Session()
assert(session.configure(config_file=config_file,ordered_config_load_flag = True))

redcap_project = session.connect_server('data_entry', True)
assert(redcap_project)
form_key = session.get_redcap_form_key()

red2cas = r2c.redcap_to_casesdir()
if not red2cas.configure(session,redcap_project.metadata) :
    sys.exit(1)

outdir = "/tmp/test_redcap_to_casedir"
if not os.path.exists(outdir) :
    os.makedirs(outdir)

#
# SUBJECT SPECIFIC INFO 
#

visit_log_fields = ['study_id', 'redcap_data_access_group', 'visit_date',
                      'mri_qa_completed', 'mri_t1_age', 'mri_dti_age',
                      'mri_rsfmri_age','mri_scanner', 'visit_ignore','mri_xnat_sid']

if specific_subject:
  visit_log_redcap = redcap_project.export_records(fields=visit_log_fields,
                                                   event_name='unique',
                                                   export_data_access_groups=True,
                                                   records=specific_subject,
                                                   format='df')
else :
  visit_log_redcap = redcap_project.export_records(fields=visit_log_fields,
                                                   event_name='unique',
                                                   export_data_access_groups=True,
                                                   format='df')
# also referred to as key
subject_key = visit_log_redcap.index[0]
subject_red_id =  subject_key[0]
subject_event_id =  subject_key[1]
subject_site_id = str(subject_red_id.split('-')[0])
subject_visit_data = visit_log_redcap.ix[subject_key]
subject_xnat_id =  str(subject_visit_data['mri_xnat_sid'])
(arm_code,visit_code,subject_datadir_rel) = red2cas.translate_subject_and_event(subject_xnat_id, subject_event_id)

subject_fields = ['study_id', 'dob',  'exclude', 'enroll_exception',
                  'siblings_enrolled', 'siblings_id1', 'hispanic', 'race','race_other_code']
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

visit_date = str(subject_visit_data['visit_date'])
subject_dob_str = str( this_subject_data['dob'] )
visit_age = old_div(red2cas.days_between_dates(subject_dob_str, visit_date ), 365.242)

#
# Tests
#

if 1 : 
  test_cluster()
else :
  print("DEBUG: skipping job submission")

if 1 :
    test_save_demographics_to_file()
else :
    print("DEBUG: skip saving demographic data")

#
# Writing out another dictionary
#

if 1 :
    form_event_mapping = redcap_project.export_fem( format='df' )
    forms_this_event = set( form_event_mapping[form_event_mapping['unique_event_name'] ==  subject_event_id ][form_key].tolist())

    # intersect between the two forms - sorted is importent so you always get the same output
    forms_this_event_datadict=sorted(set(red2cas.get_export_names_of_forms()) & forms_this_event)

    test_save_form_to_file(forms_this_event_datadict[0])
else :
    print("DEBUG: skip saving form data")


print("Wrote results to " + outdir)
