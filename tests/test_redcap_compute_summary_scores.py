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
import difflib
import filecmp

parser = argparse.ArgumentParser(description="testing redcap compute scores",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("configFile", help=".sibis-general-config.yml", action="store", default='data/.sibis-general-config.yml')
parser.add_argument("--id-list", help="subject id list seperated by comma (e.g. E-00000-F-0)", action="store", required=False, default=None)
parser.add_argument("--form-list", help="list of forms seperated by comma (e.g. ssaga_dsm4,ssaga_dsm5) ", action="store", required=False, default=None)
parser.add_argument("--dir", help="if defined will write output to that dir", action="store", required=False, default=None)
parser.add_argument("--allForms", help="should all forms be checked", action="store_true", required=False, default=False)
parser.add_argument("--uploadScores", help="should computed scores be uploaded to redcap", action="store_true", required=False, default=False)
parser.add_argument("--snapshotDir", help="should compare computed scores with versions specified dir", action="store", required=False, default=None)

args = parser.parse_args()

slog.init_log(False, False,'test_redcap_compute_summary_scores', 'test_redcap_compute_summary_scores',None)

session = sibispy.Session()
assert(session.configure(config_file=args.configFile))

red_score_update = red_scores.redcap_compute_summary_scores() 
assert(red_score_update.configure(session)) 

# Load in test specific settings : 
(sys_file_parser,err_msg) = session.get_config_test_parser()
if err_msg :
    print "Error: session.get_config_test_parser:" + err_msg
    sys.exit(1)
 
config_test_data = sys_file_parser.get_category('test_redcap_compute_summary_scores')
if not config_test_data : 
    print "Error: test_session specific settings not defined!"
    sys.exit(1)

if args.id_list : 
    subject_id_list =  args.id_list.split(',') 
else : 
    subject_id_list = [config_test_data.get('subject_id')]

if args.allForms: 
  instruments = red_score_update.get_list_of_instruments()  
else :
    if args.form_list: 
        instruments =  args.form_list.split(',') 
    else :
        instruments = config_test_data.get('instruments').split(",")

for subj in subject_id_list :
    for inst in instruments: 
        (recorded_scores,errorFlag) = red_score_update.compute_summary_scores(inst, subject_id = subj, update_all = True, verbose = False)

        if args.uploadScores:
            red_score_update.upload_summary_scores_to_redcap(inst, recorded_scores)

        if not errorFlag and args.dir: 
            fileName = os.path.join(args.dir,inst + "_" + subj + '_out.csv') 
            with open(fileName, 'w') as csvfile:
                recorded_scores.to_csv(csvfile)

            print "Wrote output to", fileName
            
            if args.snapshotDir:
                snapshotFilename = os.path.join(args.snapshotDir, inst + "_" + subj + '_out.csv')

                if not os.path.isfile(snapshotFilename):
                    print "ERROR: Missing snapshot file", snapshotFilename
                    
                else:
                    is_match = filecmp.cmp(fileName, snapshotFilename, shallow=False)
                    if not is_match:
                        print "ERROR: snapshot differs"
                        with open(fileName, 'r') as current:
                            with open(snapshotFilename) as snapshot:
                                curLines = current.readlines()
                                snapLines = snapshot.readlines()
                                sys.stdout.writelines(difflib.unified_diff(snapLines, curLines, snapshotFilename, fileName))
                                print "\n"

                    

                


    # assert(not errorFlag)
    # print recorded_scores
