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
import shutil
import pandas
import numpy as np

import sibispy
from sibispy import sibislogger as slog
from sibispy import redcap_to_casesdir as r2c
import pwd

# =============================
# Test Functions 
# =============================

def test_cluster():
  print("== test_cluster only needs to work on backend ==")
  cluster_submit_log = os.path.join(outdir,'cluster-submit.log')
  cluster_job_log = os.path.join(session.get_beta_dir(),'cluster-job.log')
  if os.path.exists(cluster_job_log):
    print("INFO:Removing", cluster_job_log) 
    os.remove(cluster_job_log)

  cluster_test_file = os.path.join(session.get_cases_dir(),"test_redcap_to_casesdir.cluster")
  user_id =  os.getuid()
  user_name = pwd.getpwuid(user_id )[ 0 ]
  cmd = 'echo "Submitting User:' + user_name + '(' + str(user_id) + ')"; echo "Executing User: ${LOGNAME}(${UID})"; touch ' + str(cluster_test_file) + '; rm -f '  + str(cluster_test_file)

  assert(red2cas.schedule_cluster_job(cmd, "test_redcap_to_casesdir", submit_log=cluster_submit_log, job_log=cluster_job_log, verbose = False))
  print("INFO:Please check " +  cluster_job_log + " if cluster job was successfully executed !")

#=====================================
def test_save_demographics_to_file():
  assert(red2cas.create_demographic_datadict(outdir))
  
  # Note : output writen to file by this function does not necessarily respond to real subject data 
  assert(red2cas.export_subject_demographics(
    subject_red_id,
    subject_xnat_id,
    arm_code,
    visit_code,
    subject_site_id,
    visit_age ,
    this_subject_data,
    subject_visit_data,
    -1,
    -1,
    None,
    measures_dir=outdir,
    conditional=False
  ))

#=====================================
def test_save_form_to_file(name_of_form):

  assert(red2cas.create_datadict(name_of_form,outdir))
  
  # Test writing out a case specific measurement file
  
  (all_records,export_list) = red2cas.get_subject_specific_form_data(subject_red_id, subject_event_id, forms_this_event, redcap_project, select_exports=[name_of_form])
  assert((not all_records.empty) and export_list)
  
  assert(red2cas.export_subject_form(name_of_form, subject_red_id, subject_xnat_id,arm_code,visit_code, all_records, outdir, verbose=True))
  
  red2cas.export_subject_all_forms(redcap_project,  subject_site_id, subject_red_id, subject_event_id, this_subject_data, visit_age, subject_visit_data, arm_code, visit_code, subject_xnat_id, outdir,forms_this_event, -1, -1, None)

# =============================
# Main 
# =============================

#
# do not run via pytest but simply the command directly 
#
specific_subject=None
config_file = os.path.join('/', *'/fs/storage/share/operations/secrets/.sibis/'.split('/'), '.sibis-general-config.yml')
if sys.argv.__len__() > 1 :
    config_file = sys.argv[1]
    # execute for a specific subject, e.g. B-00000-M-6
    if sys.argv.__len__() > 2 :
        specific_subject=[sys.argv[2]]

slog.init_log(False, False,'test_redcap_to_casesdir', 'test_redcap_to_casesdir',None)
# slog.init_log(True, True,'test_redcap_to_casesdir', 'testing',None)

session = sibispy.Session()
assert(session.configure(config_file=config_file,ordered_config_load_flag = True))

redcap_project = session.connect_server('data_entry', True)
assert(redcap_project)
form_key = session.get_redcap_form_key()

red2cas = r2c.redcap_to_casesdir()
if not red2cas.configure(session,redcap_project.metadata) :
    sys.exit(1)

outdir = "/tmp/test_redcap_to_casedir"
if os.path.exists(outdir) :
  shutil.rmtree(outdir)
  


os.makedirs(outdir)

#
# SUBJECT SPECIFIC INFO 
#

visit_log_fields = ['study_id', 'visit_date', 'family_id',
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
subject_visit_data = visit_log_redcap.loc[subject_key]
subject_xnat_id =  str(subject_visit_data['mri_xnat_sid'])
(arm_code,visit_code,subject_datadir_rel) = red2cas.translate_subject_and_event(subject_xnat_id, subject_event_id)

subject_fields = ['study_id', 'dob',  'exclude', 'enroll_exception',
                  'siblings_enrolled', 'siblings_id1', 'hispanic', 
                  'race','race_other_code', 'family_id','ndar_guid_id',
                  'ndar_consent', 'ndar_guid_anomaly', 'ndar_guid_anomaly_visit',
                  'ndar_guid_aud_dx_followup', 'ndar_guid_aud_dx_initial']
#baseline_events = ['baseline_visit_arm_1','baseline_visit_arm_4']
#for speed up just the single subject- records flag
baseline_events = ['baseline_visit_arm_1']
subject_data = redcap_project.export_records(fields=subject_fields,
                                             events=baseline_events,
                                             event_name='unique',
                                             records = [subject_red_id],
                                             format='df')
subject_data = pandas.concat([subject_data.xs(event, level=1) for event in baseline_events])
this_subject_data = subject_data.loc[subject_red_id]

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


# Midyear mock data and test
mock_longitudinal_data = pandas.DataFrame({
  'study_id': ['X-00001-X-9'] * 4,
  'redcap_event_name': ['5y_visit_arm_1', '66month_followup_arm_1',
                        '6y_visit_arm_1', '72month_followup_arm_1'],
  'visit_date': [np.nan, '2015-06-01', '2016-01-01', '2016-06-01'],
  'mri_qa_completed': [np.nan, np.nan, 1, np.nan],
  'mri_t1_age': [np.nan, np.nan, 16, np.nan],
  'mri_dti_age': [np.nan, np.nan, 16, np.nan],
  'mri_rsfmri_age': [np.nan, np.nan, 16, np.nan],
  'mri_scanner': [np.nan, np.nan, 'SIEMENS TrioTim MRC35217', np.nan],
  'visit_ignore___yes': [1, 0, 0, 0],
  'mri_xnat_sid': ['NCANDA_S09999'] * 4,
}).set_index(['study_id', 'redcap_event_name'])
mock_baseline_data = pandas.DataFrame({
  'study_id': ['X-00001-X-9'],
  'dob': ['1999-01-01'],
  'race': [4],
  'race_other_code': [np.nan],
  'hispanic': [1],
  'siblings_enrolled___true': [0],
  'siblings_id1': [np.nan],
  'family_id': [10],
  'enroll_exception___drinking': [0],
  'exclude': [0],
  'ndar_guid_id':['TEST_GUID'],
  'ndar_consent':[5],
  'ndar_guid_anomaly': [0],
  'ndar_guid_anomaly_visit': [np.nan],
  'ndar_guid_aud_dx_followup': [0],
  'ndar_guid_aud_dx_initial': [np.nan]  
}).set_index('study_id').loc['X-00001-X-9']

from functools import partial
export_conditional_demographics = partial(
  red2cas.export_subject_demographics,
  subject='X-00001-X-9',
  subject_code='NCANDA_S09999',
  arm_code=arm_code,
  # visit_code='followup_5y',
  site='A',
  # visit_age=visit_age,
  subject_data=mock_baseline_data,
  # visit_data=mock_longitudinal_data,
  exceeds_criteria_baseline=-1,
  siblings_enrolled_yn_corrected=-1,
  siblings_id_first_corrected=None,
  # measures_dir=outdir,
  #conditional=conditional,
)

# Prepare export paths
os.makedirs(os.path.join(outdir, "NCANDA_S09999", "followup_5y"), exist_ok=True)
os.makedirs(os.path.join(outdir, "NCANDA_S09999", "followup_6y"), exist_ok=True)
# Delete the file that is only created conditionally, if one doesn't already exist
try:
  os.remove(os.path.join(outdir, "NCANDA_S09999", "followup_5y", "demographics.csv"))
except OSError:
  pass

# assert export_conditional_demographics(
#   visit_code='followup_5y',
#   visit_age=mock_longitudinal_data.loc['5y_visit_arm_1'].get('visit_age'),
#   visit_data=mock_longitudinal_data.loc['5y_visit_arm_1'],
#   conditional=False,
# ) is None
assert export_conditional_demographics(
  visit_code='followup_5y',
  visit_age=mock_longitudinal_data.xs('66month_followup_arm_1', level='redcap_event_name', drop_level=False).get('visit_age'),
  visit_data=mock_longitudinal_data.xs('66month_followup_arm_1', level='redcap_event_name', drop_level=False).squeeze(),
  measures_dir=os.path.join(outdir, 'NCANDA_S09999', 'followup_5y'),
  conditional=True,
) is True, "66-month midyear should be saved despite conditional=True"
assert export_conditional_demographics(
  visit_code='followup_6y',
  visit_age=mock_longitudinal_data.xs('6y_visit_arm_1', level='redcap_event_name', drop_level=False).get('visit_age'),
  visit_data=mock_longitudinal_data.xs('6y_visit_arm_1', level='redcap_event_name', drop_level=False).squeeze(),
  measures_dir=os.path.join(outdir, 'NCANDA_S09999', 'followup_6y'),
  conditional=False,
) is True, "6y main-year visit should be saved always"
assert export_conditional_demographics(
  visit_code='followup_6y',
  visit_age=mock_longitudinal_data.xs('72month_followup_arm_1', level='redcap_event_name', drop_level=False).get('visit_age'),
  visit_data=mock_longitudinal_data.xs('72month_followup_arm_1', level='redcap_event_name', drop_level=False).squeeze(),
  measures_dir=os.path.join(outdir, 'NCANDA_S09999', 'followup_6y'),
  conditional=True,
) is None, "72-month midyear should not be saved, given the presence of 6y main"

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
