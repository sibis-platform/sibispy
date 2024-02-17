#!/usr/bin/env python3

"""
Given a path to a cases directory, will search for all instances
of the export measures log file and check which ones haven't been updating.
"""

import pathlib
import sys
import pandas as pd
import argparse
import glob
import re
from datetime import datetime

import sibispy
from sibispy import utils as sutils
from sibispy import sibislogger as slog

def create_err_df(missing_logs, outdated_visits):
    """
    Using the list of visits missing log files and visits that were last updated outside
    of the update window, create an error dataframe that lists path to visit and error.
    """
    missing_log_data = {file_path: "missing log" for file_path in missing_logs}
    outdated_visit_data = {file_path: "outdated" for file_path in outdated_visits}

    df1 = pd.DataFrame(list(missing_log_data.items()), columns=["File Path", "Status"])
    df2 = pd.DataFrame(list(outdated_visit_data.items()), columns=["File Path", "Status"])

    err_df = pd.concat([df1, df2], ignore_index=True)
    return err_df

def is_within_update_window(num_days, file_data):
    """Checks if datestring is within number of days of today"""
    file_date = datetime.strptime(file_data.strip(), '%Y-%m-%d %H:%M:%S')
    diff = datetime.now() - file_date
    return diff.days <= num_days

def check_update_history(args, visit_dirs, num_days, log_file, excluded_subjects, excluded_visits):
    """
    Return list of export_measures log files that haven't been 
    updated within the window of number of days (from args)
    """
    if args.verbose:
        print(f"Checking that all logs present have been updated within {num_days} days.")

    out_of_update_window = []
    for visit in visit_dirs:
        log = visit / log_file
        with log.open() as file:
            file_data = file.read()
            if not is_within_update_window(num_days, file_data):
                if not is_excluded(args, visit, excluded_subjects, excluded_visits):
                    out_of_update_window.append(log)
    return out_of_update_window

def is_excluded(args, visit, excluded_subjects, excluded_visits):
    """Returns true if the given visit or subject is excluded"""
    if args.dir_base == '/fs/ncanda-share/cases':
        match = re.search(r'NCANDA_S\d+', str(visit))
        if match:
            ncanda_s_number = match.group(0)
        pid_res = sutils.ncanda_id_lookup(ncanda_s_number)
        if pid_res:
            pid = pid_res.rstrip('\n')
        else:
            print(f"Couldn't get pid for visit: {visit}")
            return False
        is_excluded_subject = any(item['study_id'] == pid for item in excluded_subjects)
        excluded_visit = [item for item in excluded_visits if item['study_id'] == pid]
        if is_excluded_subject:
            return True
        elif excluded_visit:# check that the visit is not ignored first
            # determine if current visit is the one to be ignored
            for ev in excluded_visit:
                redcap_event = ev['redcap_event_name']
                year_digit_match = re.search(r'(\d+)[y]', redcap_event)
                if year_digit_match:
                    year_digit = year_digit_match.group(1)
                    curr_visit_year = ''.join(re.findall(r'\d', visit.parent.name))
                    if curr_visit_year == year_digit:
                        return True
        return False
    # TODO: implement hivalc check
    return False

def get_missing_log_dirs(args, visit_dirs, log_file, excluded_subjects, excluded_visits):
    """
    Given all visit dirs, pop out visits missing log files and return
    as seperate list, along with updated visit dir list.
    """
    visits_with_log = []
    visits_without_log = []

    if args.verbose:
        print("Finding all instances of missing log files.")

    for visit in visit_dirs:
        if not (visit / log_file).is_file():
            # check if visit is from excluded subject
            if not is_excluded(args, visit, excluded_subjects, excluded_visits):
                visits_without_log.append(visit)
        else:
            visits_with_log.append(visit)
            
    return visits_with_log, visits_without_log

def get_excluded(records):
    """Drop visits where the visit or subject is marked as excluded"""
    excluded_subjects = []
    excluded_visits = []
    for record in records:
        if record['exclude'] not in ['', '0']:
            excluded_subjects.append(record)
        elif record['visit_ignore___yes'] == '1':
            excluded_visits.append(record)

    return excluded_subjects, excluded_visits

def set_path_pattern(base_path: str) -> pathlib.Path:
    """Set path to log file depending on whether lab or ncanda data"""
    ncanda_cases_base = '/fs/ncanda-share/cases'
    lab_cases_base = '/fs/share/cases_next'

    if base_path == ncanda_cases_base:
        pattern = '*/standard/*/measures'
        return pattern
    elif base_path == lab_cases_base:
        pattern = '*/*/*/redcap'
        return pattern
    else:
        print(f"ERROR: Couldn't find cases w/ base: {base_path}")
        sys.exit(1)

def get_visit_dirs(args, base_path):
    """
    Given base path to cases directory, return list of all visit dirs that 
    could potentially contain export_measures.log files
    """
    # get pattern to log file based on cases base
    pattern = set_path_pattern(base_path)
    base_path = pathlib.Path(base_path)

    if args.verbose:
        print(f"Searching for all visits in {base_path}")

    visits = base_path.glob(pattern)
    visits = [visit for visit in visits if visit.is_dir()]

    return visits

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('-d', '--dir_base', type=str, 
        help='Specify path to base cases directory. Lab == "/fs/share/cases_next", ncanda == "/fs/ncanda-share/cases"')
    parser.add_argument('-t', '--time_window', type=int,
        default=7,
        help='Number of days that logs can be outdated by. Default == 7.')
    parser.add_argument("-v", "--verbose",
                    help="Verbose operation",
                    action="store_true")
    parser.add_argument("-p", "--post-to-github", help="Post all issues to GitHub instead of std out.", action="store_true")

    return parser.parse_args()

def main():
    # take in the path to cases dir
    args = parse_args()

    slog.init_log(args.verbose, args.post_to_github,'check_updated_cases', 'check_updated_cases', None)

    session = sibispy.Session()
    if not session.configure():
        if args.verbose:
            print("Error: session configure file was not found")
        sys.exit(1)

    data_entry = session.connect_server('data_entry')
    records = data_entry.export_records(fields=['study_id', 'exclude', 'visit_ignore', 'mri_xnat_sid'])

    excluded = [record for record in records if record['exclude'] == '1' or record['visit_ignore___yes'] == '1']

    base_path = args.dir_base
    log_file = 'export_measures.log'

    # get all visit dirs in base_path
    visit_dirs = get_visit_dirs(args, base_path)

    # drop visit dirs for excluded subjects / visits
    excluded_subjects, excluded_visits = get_excluded(records)

    # First store all directories that don't have an export measures.log file
    visit_dirs, missing_logs = get_missing_log_dirs(args, visit_dirs, log_file, excluded_subjects, excluded_visits)

    # then for all dirs that do have it, check the date it was last updated
    outdated_visits = check_update_history(args, visit_dirs, args.time_window, log_file, excluded_subjects, excluded_visits)

    # create error dataframe that lists visits missing logs and those that are outdated
    err_df = create_err_df(missing_logs, outdated_visits)

    if not err_df.empty:
        if args.verbose:
            print(f"Found {len(err_df)} instances of missing/outdated logs. Posting:")

        # post error df to github or print to log
        curr_date = str(datetime.now().date())
        for idx, row in err_df.iterrows():
            header = "Checking " + str(row['File Path']).replace(base_path +'/', '')
            error = row['Status']
            slog.info(header,
                error,
                info="Consult with scan logs in Redcap to verify that scan instance exists. If not \
                    present in Redcap, remove scan.")

if __name__ == "__main__":
    main()
