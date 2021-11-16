#!/usr/bin/env python

##
##  See COPYING file distributed along with the ncanda-data-integration package
##  for the copyright and license terms
##

from __future__ import print_function
from builtins import str
import sys
import argparse

import pandas
import sibispy
from sibispy import sibislogger as slog
from sibispy import redcap_compute_summary_scores as red_scores

#
# Main 
#

def main():
    # Setup command line parser
    parser = argparse.ArgumentParser(description="Update longitudinal project forms"
                                                 " from data imported from the data capture laptops",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-v", "--verbose",
                        help="Verbose operation",
                        action="store_true")
    parser.add_argument("-i", "--instruments",
                        help="Select specific instruments to update. Separate multiple forms with commas.",
                        action="store", default=None)
    parser.add_argument("-s", "--subject_id",
                        help="Only run for specific subject (multiple subject seperate with ',') .",
                        action="store", default=None)
    parser.add_argument("-a", "--update-all",
                        help="Update all summary records, regardless of current completion status "
                             "(otherwise, only update records where incoming data completion status "
                             "exceeds existing summary data status)",
                        action="store_true")
    parser.add_argument("-n", "--no-upload",
                        help="Do not upload any scores to REDCap server; instead write to CSV file with given path.",
                        action="store")
    parser.add_argument("-p", "--post-to-github", help="Post all issues to GitHub instead of std out.", action="store_true")
    parser.add_argument("-t", "--time-log-dir",
                        help="If set then time logs are written to that directory",
                        action="store",
                        default=None)
    args = parser.parse_args()

    slog.init_log(args.verbose, args.post_to_github,'NCANDA REDCap', 'redcap_update_summary_scores', args.time_log_dir)
    slog.startTimer1()

    count_uploaded = 0

    # First REDCap connection for the Summary project (this is where we put data)
    session = sibispy.Session()
    if not session.configure():
        if args.verbose:
            print("Error: session configure file was not found")

        sys.exit(1)

    red_score_update = red_scores.redcap_compute_summary_scores() 
    if not red_score_update.configure(session): 
        if args.verbose:
            print("Error: could not configure redcap_compute_summary_scores")
        sys.exit(1)

    # If list of forms given, only update those
    instrument_list = red_score_update.get_list_of_instruments()
    if args.instruments:
        tmp_instrument_list = []
        for inst in args.instruments.split(','):
            if inst in instrument_list:
                tmp_instrument_list.append(inst)
            else:
                print("WARNING: no instrument with name '%s' defined." % inst)
                print("         Options:", instrument_list,"\n") 
        instrument_list = tmp_instrument_list
        

    # Import scoring module - this has a list of all scoring instruments with input fields, scoring functions, etc.
    for instrument in instrument_list:
        slog.startTimer2()
        if args.verbose:
            print('Scoring instrument', instrument)

        (scored_records, errorFlag) = red_score_update.compute_summary_scores(
            instrument, args.subject_id, args.update_all, args.verbose, log=slog)
        if errorFlag:
            if args.verbose:
                print("Error occured when scoring", instrument) 
            continue

        len_scored_records = len(scored_records)
        if not len_scored_records : 
            if args.verbose:
                print("Nothing was scored due to, e.g., missing values!") 
            continue  

        if args.verbose:
            print(len_scored_records, 'scored records to upload')

        if args.no_upload:
            scored_records.to_csv(args.no_upload)
            continue  

        uploaded = red_score_update.upload_summary_scores_to_redcap(instrument,scored_records)
        if not uploaded : 
            continue 

        if not 'count' in list(uploaded.keys()) or  uploaded['count'] == 0:
            if args.verbose :
                if args.update_all :
                    print('No updates for instrument "%s"' % instrument)
                else : 
                    print('No unscored records for instrument "%s"' % instrument)

            continue 

        count = uploaded['count']
        count_uploaded += count               
        if args.verbose:
            print('Updated', uploaded, 'records of "%s"' % instrument)

        slog.takeTimer2(instrument + "_time","{'uploads': " +  str(count) + "}")

    slog.takeTimer1("script_time","{'records': " + str(len(instrument_list)) + ", 'uploads': " +  str(count_uploaded) + "}")

if __name__ == "__main__":
    main()
