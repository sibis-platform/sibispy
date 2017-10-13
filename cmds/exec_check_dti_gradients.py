#!/usr/bin/env python
import argparse 
import os
import sys
import math
import hashlib
import pandas as pd
import glob 

import sibispy
from sibispy import sibislogger as slog
from sibispy import check_dti_gradients as chk_dti

def get_cases(cases_root, arm, event, case=None):
    """
    Get a list of cases from root dir, optionally for a single case
    """
    match = 'NCANDA_S*'
    if case:
        match = case

    case_list = list()
    for cpath in glob.glob(os.path.join(cases_root, match)):
        if os.path.isdir(os.path.join(cpath,arm,event)) : 
            case_list.append(cpath)
  
    return case_list


def main(args,sibis_session):
    # Get the gradient tables for all cases and compare to ground truth

    slog.startTimer1()
    cases_dir =  sibis_session.get_cases_dir() 
    if args.verbose:
      print "Checking cases in " + cases_dir 

    cases = get_cases(cases_dir, arm=args.arm, event=args.event, case=args.case)

    if cases == [] : 
        if args.case :
            case=  args.case
        else :
            case = "*"

        print "Error: Did not find any cases matching :" + "/".join([cases_dir,case,args.arm,args.event])
        sys.exit(1)

    # Demographics from pipeline to grab case to scanner mapping
    demo_path = os.path.join(sibis_session.get_summaries_dir(),'redcap/demographics.csv')
    demographics = pd.read_csv(demo_path, index_col=['subject',
                                                     'arm',
                                                     'visit'])

    checker = chk_dti.check_dti_gradients()
    if not checker.configure(sibis_session,check_decimals = args.decimals) :
        slog.info('exec_check_dti_gradients.main',"Configuration of check_dti_gradients failed !")
        sys.exit(1)
        
    for case in cases:
        # Get the case's site
        dti_path = os.path.join(case, args.arm, args.event,'diffusion/native',args.sequence)
        if not os.path.exists(dti_path) :
            if args.verbose:
                print "Warning: " + dti_path + " does not exist!"

            continue 

        if args.verbose:
            print "Processing: " + "/".join([case,args.arm, args.event])

        sid = os.path.basename(case)
        try:
            scanner = demographics.xs([sid, args.arm, args.event])['scanner']
            scanner_model = demographics.xs([sid, args.arm, args.event])['scanner_model']
        except :
            print "Error: case " + case + "," +  args.arm + "," + args.event +" not in " + demo_path +"!"
            error = 'Case, arm and event not in demo_path'
            slog.info(hashlib.sha1('check_gradient_tables {} {} {}'.format(case, args.arm, args.event)).hexdigest()[0:6], error,
                      case=str(case),
                      arm=str(args.arm),
                      event=str(args.event),
                      demo_path=str(demo_path))
            continue

        if (isinstance(scanner, float) and math.isnan(scanner)) or (isinstance(scanner_model, float) and math.isnan(scanner_model)) :
            print "Error: Did not find scanner or model for " + sid + "/" +  args.arm + "/" + args.event +" so cannot check gradient for that scan!"
            error = "Did not find any cases matching cases_dir, case, arm, event"
            slog.info(hashlib.sha1('check_gradient_tables {} {} {}'.format(args.base_dir, args.arm, args.event)).hexdigest()[0:6], error,
                      cases_dir=cases_dir,
                      case=str(case),
                      arm=str(args.arm),
                      event=str(args.event))
            continue

        xml_file_path = checker.get_dti_stack_path(args.sequence, case, arm=args.arm, event=args.event)
        checker.check_diffusion(dti_path,"",glob.glob(xml_file_path),scanner, scanner_model, "", args.sequence)

    slog.takeTimer1("script_time", "{'records': " + str(len(cases)) + "}")


#
# =======================================
#

if __name__ == "__main__":
    sibis_session = sibispy.Session()
    if not sibis_session.configure() :
        if verbose:
            print "Error: session configure file was not found"
 
        sys.exit()



    formatter = argparse.RawDescriptionHelpFormatter
    default = 'default: %(default)s'
    parser = argparse.ArgumentParser(prog="check_gradient_tables.py",
                                     description=__doc__,
                                     formatter_class=formatter)
    parser.add_argument('-a', '--arm', dest="arm",
                        help="Study arm. {}".format(default),
                        default='standard')
    parser.add_argument('-d', '--decimals', dest="decimals",
                        help="Number of decimals. {}".format(default),
                        default=2)
    parser.add_argument('-e', '--event', dest="event",
                        help="Study event. {}".format(default),
                        default='baseline')
    parser.add_argument('-c', '--case', dest="case",
                        help="Case to check - if none are defined then it checks all cases in that directory. {}".format(default), default=None)
    parser.add_argument('-v', '--verbose', dest="verbose",
                        help="Turn on verbose", action='store_true')
    parser.add_argument("-p", "--post-to-github", help="Post all issues to GitHub instead of std out.",
                        action = "store_true", default = False)
    parser.add_argument('-s', '--sequence',
                        help="Type of sequence to check: dti6b500pepolar, dti30b400, dti60b1000 . {}".format(default),
                        default='dti60b1000')
    parser.add_argument("-t", "--time-log-dir",help = "If set then time logs are written to that directory",
                        action = "store",
                        default = None)
    argv = parser.parse_args()

    # Setting up logging 
    slog.init_log(argv.verbose, argv.post_to_github, 'NCANDA XNAT', 'check_gradient_tables', argv.time_log_dir)
    
    sys.exit(main(argv,sibis_session))

