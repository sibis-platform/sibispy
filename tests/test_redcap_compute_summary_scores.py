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

if sys.argv.__len__() > 1 : 
    config_file = sys.argv[1]
else :
    config_file = os.path.join(os.path.dirname(sys.argv[0]), 'data', '.sibis-general-config.yml')

slog.init_log(False, False,'test_redcap_compute_summary_scores', 'test_redcap_compute_summary_scores',None)

session = sibispy.Session()
assert(session.configure(config_file=config_file))

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

subject_id = config_test_data.get('subject_id')
# instruments = config_test_data.get('instruments').split(",")
instruments = red_score_update.get_list_of_instruments()
for inst in instruments: 
    (recorded_scores,errorFlag) = red_score_update.compute_summary_scores(inst, subject_id = subject_id, update_all = True, verbose = False)
    # assert(not errorFlag)
    # print recorded_scores
