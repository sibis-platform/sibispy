#!/usr/bin/env python
"""
Set to Complete all Import project forms that are referenced from Data Entry.

Technically, only Data Entry forms that have the completion status of
Unverified or Complete are considered. Typically, though, the import process
would set the Entry form to Unverified, so it would take explicit human action
to set the Entry form to Incomplete.
"""

##
##  See COPYING file distributed along with the ncanda-data-integration package
##  for the copyright and license terms
##

from __future__ import print_function
from builtins import range
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
from sibispy import cli

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
             'lssaga4_parent': 'limesurvey_ssaga_part_4_parent',
             'plus': 'participant_last_use_summary',
             'myy': 'midyear_youth_interview'}

# Preferred format for cli.add_form_param:
all_forms_tuple = [(k, v) for k, v in all_forms.items()]

# Because of all of the issues currently not included: 
# 'plus': 'participant_last_use_summary'}

def batch(iterable, n=1):
        """
        For batch processing of records

        :param iterable:
        :param n: batch size
        :return: generator
        """
        l = len(iterable)
        for ndx in range(0, l, n):
            yield iterable[ndx:min(ndx + n, l)]

#
# MAIN 
#


# Setup command line parser
parser = argparse.ArgumentParser(description="Set status of a specific form to complete in entry project and if their is a reference to a form in import project than sets it to complete too",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("-v", "--verbose",
                    help="Verbose operation",
                    action="store_true")

form_subgroup = parser.add_mutually_exclusive_group(required=True)
form_subgroup.add_argument("-a", "--all-forms",
                           help="Update all forms of arm 1",
                           action="store_true")
cli.add_form_param(form_subgroup, eligible_forms=all_forms_tuple)

cli.add_subject_param(parser, dest="study_id")
cli.add_event_param(parser, accepted_regex=r'^(baseline|\d{1,2}y|\d{1,3}m)$', template='{}')
args = parser.parse_args()

if len(args.event) > 0:
    args.event = args.event[0]
    if len(args.event) > 1:
        print("Currently only handling a single event; picking {}"
              .format(args.event))

slog.init_log(args.verbose, None,'change_status_of_complete_field', 'change_status', None)
session = sibispy.Session()

if not session.configure():
    if args.verbose:
        print("Error: session configure file was not found")

    sys.exit()

forms = None
if args.forms:
    forms = dict()
    for f in args.forms:
        if f in list(all_forms.keys()):
            forms[f] = all_forms[f]
        elif f in list(all_forms.values()):
            lookup = [k for (k, v) in all_forms.items() if v == f]
            forms[lookup[0]] = f
        else:
            print("WARNING: no form with name or prefix '%s' defined.\n" % f)
elif args.all_forms:
    forms = all_forms

if forms == None : 
    print("Please define forms to run this script for") 
    sys.exit(1)

if args.verbose:
    print("Processing the following forms:\n\t", '\n\t'.join( sorted(forms.values())))

form_prefixes = list(forms.keys())
form_names = list(forms.values())

# Open connection with REDCap server - Import Project
import_project = session.connect_server('import_laptops', True)
if not import_project :
    if args.verbose:
        print("Error: Could not connect to Redcap for Import Project")

    sys.exit()

# Open connection with REDCap server - Data Entry
redcap_project = session.connect_server('data_entry', True)
if not redcap_project :
    if args.verbose:
        print("Error: Could not connect to Redcap for Data Entry")

    sys.exit()

form_event_mapping = redcap_project.export_instrument_event_mappings(format_type='df')
fem_form_key = session.get_redcap_form_key()
#
# MAIN LOOP
#

for form_prefix, form_name in forms.items():
    print("Processing form",form_prefix,"/",form_name)

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

    event_mapping_tmp = form_event_mapping[form_event_mapping[fem_form_key] == summary_form_name ]['unique_event_name']
    event_mapping =  event_mapping_tmp[event_mapping_tmp.str.startswith(args.event, na=False)].tolist()

    if len(event_mapping) == 0 : 
        print(f"ERROR: Event name starting with {args.event} for {form_name} could not be found !")
        continue 

    fields_list = [complete_label,record_label,'visit_ignore']
    entry_records = session.redcap_export_records_from_api(time_label= None, api_type = 'data_entry', fields = fields_list, format_type='df')
    if args.study_id : 
        entry_records = entry_records[entry_records.index.map( lambda key: key[0] in  args.study_id) ]

    entry_records = entry_records[entry_records.index.map( lambda key: key[1] in event_mapping) ]
    if entry_records.empty : 
        print("No records could be found for form {}; onto next form"
              .format(form_name))
        continue

    # print entry_records.columns
    entry_records = entry_records[entry_records['visit_ignore___yes'] != 1 ]

    # drop all those where status of complete label is not defined 
    entry_records = entry_records.dropna(axis=0,subset=[complete_label])

    # currently only those that are in unverified status but not complete 

    # print entry_records[entry_records[complete_label] == 0  ]
    # sys.exit() 
 
    if record_label in entry_records.columns :
        # check all links of unverivied or complete records out
        entry_records_unv_or_comp = entry_records[entry_records[complete_label] > 0 ]

        # drop any that do not have import record defined 
        import_records = entry_records_unv_or_comp.dropna(axis=0,subset=[record_label])
        if not import_records.empty : 
            import_complete_records = session.redcap_export_records_from_api(time_label= None, api_type = 'import_laptops', fields = [complete_label], format_type='df', records=import_records[record_label].tolist())

            # for all records that the complete label is not 2 turn it into 2 
            upload_id_list = import_complete_records[import_complete_records[complete_label] < 2].index
            upload_id_len = len(upload_id_list)
            if upload_id_len :  
                print("Number of Records (Import project)", upload_id_len)
                # Upload in batches as if one problem is in one record all of them are not uploaded in the batch
                for  upload_id_batch in batch(upload_id_list,50): 
                    upload_records=list() 
                    for import_id in upload_id_batch :
                        upload_records.append({'record_id': import_id, '%s_complete' % form_name: '2'})
                        # import_response = session.redcap_import_record_to_api([{'record_id': import_id, '%s_complete' % form_name: '2'}], 'import_laptops', import_id)
                    if len(upload_records) : 
                        import_response = session.redcap_import_record_to_api(upload_records, 'import_laptops', '')
                        # import_response = "TEST"
                        print("Upload Records (Import project):", upload_id_batch) 
                        print("REDCAP response:", import_response) 
    else : 
        print("Warning: '" + record_label + "' does not exist in form '" + form_name + "'")   
    

    # Now set entry record to complete
    entry_records_not_complete = entry_records[entry_records[complete_label] ==1 ]
    # check if their is an import record associated with it 
    if  entry_records_not_complete.empty :
        print("Entry project: No entries for form '" + form_name + "' found that were unverified") 
        continue

    
    print("Number of Records", len(entry_records_not_complete))
    print("Upload Records (Entry project):", entry_records_not_complete.index) 
    upload_records=list() 
    for key in entry_records_not_complete.index :
        upload_records.append({'study_id': key[0], 'redcap_event_name': key[1], '%s_complete' % form_name: '2'})
        #to_redcap(session,form_name, '','','', {'study_id': key[0], 'redcap_event_name': key[1], '%s_complete' % form_name: '2'})

    import_response = session.redcap_import_record_to_api(upload_records, 'data_entry', '')
    print("REDCAP response:", import_response) 

