#!/usr/bin/env python

##
##  See COPYING file distributed along with the ncanda-data-integration package
##  for the copyright and license terms
##

from __future__ import print_function
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

#
# MAIN 
#


# Setup command line parser
parser = argparse.ArgumentParser(description="Set status of a specific form to complete",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("-v", "--verbose",
                    help="Verbose operation",
                    action="store_true")
parser.add_argument("--forms",
                    help="Select specific forms to update the status to complete. Separate multiple forms with commas.",
                    action="store",
                    default=None )
parser.add_argument("--import-id",
                    help="Define the id of the form in the import project (e.g., 'E-00000-M-2-2015-01-06') ",
                    action="store",
                    default=None)
args = parser.parse_args()


slog.init_log(args.verbose, None,'change_status_of_complete_field', 'change_status', None)
session = sibispy.Session()

if not session.configure():
    if args.verbose:
        print("Error: session configure file was not found")

    sys.exit()

if args.forms:
    forms = dict()
    for f in args.forms.split(','):
        if f in list(all_forms.keys()):
            forms[f] = all_forms[f]
        elif f in list(all_forms.values()):
            lookup = [k for (k, v) in all_forms.items() if v == f]
            forms[lookup[0]] = f
        else:
            print("WARNING: no form with name or prefix '%s' defined.\n" % f)
else:
    print("Please define form") 
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

#
# MAIN LOOP
#

for form_prefix, form_name in forms.items():
    if args.verbose:
        print("Processing form",form_prefix,"/",form_name)

    complete_label = '%s_complete' % form_name
    fields_list = [complete_label]

    # Just make sure record exists 
    complete_records = session.redcap_export_records_from_api(time_label= None, api_type = 'import_laptops', fields = fields_list, format='df', records=[args.import_id])

    if complete_records is None :
        error_id = form_name 
        # if you stop here then missing data might be not discovered 
        slog.info(error_id, "Error: stop processing as no records in import project for this form",
                  import_id_list = args.import_id)
        continue

    # If it does than change value to 2 
    import_response = session.redcap_import_record(args.import_id,"","","", [{'record_id': args.import_id, '%s_complete' % form_name: '2'}])
    print("REDCAP response:", import_response) 
