#!/usr/bin/env python

##
##  Copyright 2017 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##

import sys
import os 
import sibispy
from sibispy import sibislogger as slog
from sibispy import redcap_compute_summary_scores as red_scores
import argparse

parser = argparse.ArgumentParser(description="testing redcap compute scores",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("configFile", help=".sibis-general-config.yml", action="store", default='data/.sibis-general-config.yml')
parser.add_argument("--id-list", help="subject id list seperated with comma (e.g. E-00000-F-0)", action="store", required=False, default=None)
parser.add_argument("--dir", help="if defined will write output to that dir", action="store", required=False, default=None)

args = parser.parse_args()

slog.init_log(False, False,'test_redcap_compute_summary_scores', 'test_redcap_compute_summary_scores',None)

session = sibispy.Session()
assert(session.configure(config_file=args.configFile))

red_score_update = red_scores.redcap_compute_summary_scores() 
assert(red_score_update.configure(session)) 

# Load in test specific settings : 
(sys_file_parser,err_msg) = session.get_config_sys_parser()
if err_msg :
    print "Error: session.get_config_sys_parser:" + err_msg
    sys.exit(1)
 
config_test_data = sys_file_parser.get_category('test_redcap_compute_summary_scores')
if not config_test_data : 
    print "Error: test_session specific settings not defined!"
    sys.exit(1)

if args.id_list : 
    subject_id_list =  args.id_list.split(',') 
else : 
    subject_id_list = [config_test_data.get('subject_id')]

instruments = config_test_data.get('instruments').split(",")
# instruments = red_score_update.get_list_of_instruments()

for subj in subject_id_list :
    for inst in instruments: 
        (recorded_scores,errorFlag) = red_score_update.compute_summary_scores(inst, subject_id = subj, update_all = True, verbose = False)
        if not errorFlag and args.dir: 
            fileName = os.path.join(args.dir,inst + "_" + subj + '_out.csv') 
            with open(fileName, 'w') as csvfile:
                recorded_scores.to_csv(csvfile)

            print "Wrote output to", fileName

    # assert(not errorFlag)
    # print recorded_scores
