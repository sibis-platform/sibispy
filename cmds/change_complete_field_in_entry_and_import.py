#!/usr/bin/env python

##
##  See COPYING file distributed along with the ncanda-data-integration package
##  for the copyright and license terms
##

import os
import re
import sys
import datetime
import argparse

import pandas
import redcap
import requests
import hashlib 

import sibispy
from sibispy import sibislogger as slog

#
# Variables 
# 
date_format_ymd = '%Y-%m-%d'

# List of forms imported from the laptops
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
             'lssaga1_youth': 'limesurvey_ssaga_part_1_youth',
             'lssaga2_youth': 'limesurvey_ssaga_part_2_youth',
             'lssaga3_youth': 'limesurvey_ssaga_part_3_youth',
             'lssaga4_youth': 'limesurvey_ssaga_part_4_youth',
             'lssaga1_parent': 'limesurvey_ssaga_part_1_parent',
             'lssaga2_parent': 'limesurvey_ssaga_part_2_parent',
             'lssaga3_parent': 'limesurvey_ssaga_part_3_parent',
             'lssaga4_parent': 'limesurvey_ssaga_part_4_parent'}

# Because of all of the issues currently not included: 
# 'plus': 'participant_last_use_summary'}

# Upload new data to REDCap
def to_redcap(session, form_name, subject_id, event, timelabel, upload_records, record_id=None, verbose=False):
    if verbose :
        print "to_redcap: ", form_name, subject_id, event, timelabel, record_id 
        id_label = '%s_missing' % form_name

    error_label = subject_id +"-"+ event + "-" + form_name 

    import_response =  session.redcap_import_record(error_label,subject_id,event,timelabel,[upload_records],record_id) 
 
    if verbose :
        print "... done" 

    # If there were any errors, try to print them as well as possible
    if import_response:
        if 'error' in import_response.keys():
            slog.info(error_label + "-" + hashlib.sha1(str(import_response['error'])).hexdigest()[0:6], "ERROR: Uploading Data", error_msg = str(import_response['error']))
        if 'fields' in import_response.keys():
            slog.info(error_label + "-" + hashlib.sha1(str(import_response['fields'])).hexdigest()[0:6], "Info: something wrong with fields ! Not sure what to do !", fields =  str(import_response['fields']))
        if 'records' in import_response.keys():
            slog.info(error_label + "-" + hashlib.sha1(str(import_response['records'])).hexdigest()[0:6], "Info: something wrong with redcords ! Not sure what to do !", records =  str(import_response['records']))
 
    return 0

#
# MAIN 
#


# Setup command line parser
parser = argparse.ArgumentParser(description="Set status of a specific form to complete in entry project and if their is a reference to a form in import project than sets it to complete too",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("-v", "--verbose",
                    help="Verbose operation",
                    action="store_true")
parser.add_argument("--forms",
                    help="Select specific forms to update the status to complete. Separate multiple forms with commas.",
                    action="store",
                    default=None )
parser.add_argument("--study-id",
                    help="Define study id - multiple seperated by comma (e.g., 'E-00000-M-2') ",
                    action="store",
                    default=None)
parser.add_argument("-e", "--event", dest="event", required=False,
                        choices=['baseline', '1y', '2y', '3y'],
                        help="Event Name")
parser.add_argument("-a", "--all-forms",
                    help="Update all forms of arm 1",
                    action="store_true")
args = parser.parse_args()


slog.init_log(args.verbose, None,'change_status_of_complete_field', 'change_status', None)
session = sibispy.Session()

if not session.configure():
    if args.verbose:
        print "Error: session configure file was not found"

    sys.exit()

forms = None
if args.forms:
    forms = dict()
    for f in args.forms.split(','):
        if f in all_forms.keys():
            forms[f] = all_forms[f]
        elif f in all_forms.values():
            lookup = [k for (k, v) in all_forms.iteritems() if v == f]
            forms[lookup[0]] = f
        else:
            print "WARNING: no form with name or prefix '%s' defined.\n" % f

if args.all_forms: 
    forms = all_forms

if forms == None : 
    print "Please define forms to run this script for" 
    sys.exit(1)

if args.verbose:
    print "Processing the following forms:\n\t", '\n\t'.join( sorted(forms.values()))

form_prefixes = forms.keys()
form_names = forms.values()

# Open connection with REDCap server - Import Project
import_project = session.connect_server('import_laptops', True)
if not import_project :
    if args.verbose:
        print "Error: Could not connect to Redcap for Import Project"

    sys.exit()

# Open connection with REDCap server - Data Entry
redcap_project = session.connect_server('data_entry', True)
if not redcap_project :
    if args.verbose:
        print "Error: Could not connect to Redcap for Data Entry"

    sys.exit()

form_event_mapping = redcap_project.export_fem(format='df')
#
# MAIN LOOP
#

for form_prefix, form_name in forms.iteritems():
    if args.verbose:
        print "Processing form",form_prefix,"/",form_name

    complete_label = '%s_complete' % form_name
    record_label = '%s_record_id' % form_prefix 

    # Select the events that actually have this form (first, to handle summary forms,
    # figure out what actual form the "FORM_complete" field is in)
    try:
        summary_form_name = [ field['form_name'] for field in redcap_project.metadata if field['field_name'] == complete_label ][0]
    except:
        # If the above failed (due to empty list, presumably), then this is not a
        # hierarchical form and we should just use the given form name
        summary_form_name = form_name

    event_mapping_tmp = form_event_mapping[form_event_mapping['form_name'] == summary_form_name ]['unique_event_name']
    event_mapping =  event_mapping_tmp[event_mapping_tmp.str.startswith(args.event, na=False)].tolist()

    if len(event_mapping) == 0 : 
        print "ERROR: Event name starting with '"+ args.event + "' for '" + form_name + "' could not be found !"
        continue 

    fields_list = [complete_label,record_label,'visit_ignore']
    entry_records = session.redcap_export_records_from_api(time_label= None, api_type = 'data_entry', fields = fields_list, format='df')
    if args.study_id : 
        entry_records = entry_records[entry_records.index.map( lambda key: key[0] in  args.study_id.split(',')) ]

    entry_records = entry_records[entry_records.index.map( lambda key: key[1] in event_mapping) ]
    if entry_records.empty : 
        print "No records could be found for form '" + form_name + "'" 
        continue 

    # print entry_records.columns
    entry_records = entry_records[entry_records['visit_ignore___yes'] != 1 ]

    # drop all those where status of complete label is not defined 
    entry_records = entry_records.dropna(axis=0,subset=[complete_label])

    # currently only those that are in unverified status but not complete 

    # print entry_records[entry_records[complete_label] == 0  ]
    # sys.exit() 
    entry_records_not_complete = entry_records[entry_records[complete_label] ==1 ]
    # check if their is an import record associated with it 
    if  entry_records_not_complete.empty :
        print "No entries for form '" + form_name + "' found that were unverified" 
        # continue
 
    if record_label in entry_records_not_complete.columns :
        # drop any that have import record defined 
        import_records = entry_records_not_complete.dropna(axis=0,subset=[record_label])
        if not import_records.empty : 
            import_complete_records = session.redcap_export_records_from_api(time_label= None, api_type = 'import_laptops', fields = [complete_label], format='df', records=import_records[record_label].tolist())
            # for import_id in :
            #    import_complete_records = session.redcap_export_records_from_api(time_label= None, api_type = 'import_laptops', fields = [complete_label], format='df', records=[import_id])

            # for all records that the complete label is not 2 turn it into 2 
            for import_id in  import_complete_records[import_complete_records[complete_label] < 2].index :
                import_response = session.redcap_import_record_to_api([{'record_id': import_id, '%s_complete' % form_name: '2'}], 'import_laptops', import_id)
                # import_response = "TEST"
                print "REDCAP response:", import_id, import_response 
    else : 
        print "Warning: '" + record_label + "' does not exist in form '" + form_name + "'"   
    

    # Now set entry record to complete
    for key in entry_records_not_complete.index :
        print "Modifying ", key 
        to_redcap(session,form_name, key[0],key[1], '', {'study_id': key[0], 'redcap_event_name': key[1], '%s_complete' % form_name: '2'})
