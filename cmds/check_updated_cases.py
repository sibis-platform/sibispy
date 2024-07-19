#!/usr/bin/env python3

"""
Given a path to a cases directory, will search for all instances
of the export measures log file and check which ones haven't been updating.

This script is assumed to be ran post full export_measures run.
If export_measures hasn't been run within the last 7 days, override the 
default expected time window via the -t, --time_window argument.
"""

import os
import sys
import argparse
import subprocess
import pandas as pd
from datetime import datetime

import sibispy
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

    # iterate through every directory in cases and check if it has a log file
    log_file = 'export_measures.log'
    base_path = args.dir_base

    if 'ncanda' in base_path:
        cmd = 'ls -d /fs/ncanda-share/cases/NCANDA_S0*/standard/*/measures'
    else:
        cmd = 'ls -d /fs/share/LAB_S0*/*/*/redcap'
    
    result = subprocess.run([cmd], shell=True, capture_output=True, text=True)
    # note: splitting off last value because it is just empty string
    matching_dirs = result.stdout.split('\n')[:-1]

    missing_log = []
    outdated_log = []

    # for each dir, see if it has a log file and that it is within acceptable update window
    for visit in matching_dirs:
        log_path = visit + '/' + log_file
        if not os.path.isfile(log_path):
            missing_log.append(visit)
        else:
            # check log is not out of date
            with open(log_path, 'r') as file:
                file_data = file.read()
                if not is_within_update_window(args.time_window, file_data):
                    outdated_log.append(visit)

    # create error dataframe that lists visits missing logs and those that are outdated
    err_df = create_err_df(missing_log, outdated_log)

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
                info="Consult with scan logs in Redcap to verify that scan instance exists. If not present in Redcap, remove scan.",
            )
    elif args.verbose:
        print("Found no instances of outdated or missing log files. Exiting.\n")

if __name__ == "__main__":
    main()
