#!/usr/bin/env python

##
##  Copyright 2017 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##


from __future__ import print_function
from builtins import str
import os
import sys
import sibispy
import glob
from sibispy import sibislogger as slog
from sibispy import check_dti_gradients as chk


if sys.argv.__len__() < 3 :
    print("run with two arugments <sibis-config-file> <scp log in>")
    print("./download_dti_groundtruth /fs/storage/share/operations/secrets/.sibis/.sibis-general-config.yml blub@blubber.com")
    sys.exit(1)
    
config_file = sys.argv[1]
ucred = sys.argv[2]

slog.init_log(False, False,'get_dti_ground_truth_path', 'get_dti_ground_truth_path',None)

session = sibispy.Session()
session.configure(config_file)

check = chk.check_dti_gradients()

print("==== IGNORE ERROR MESSAGES ===")
check.configure(session,check_decimals=2)
print("==== DO NOT IGNORE ERRORS BELOW===")
gt_path_dict = check.get_ground_truth_gradient_path_dict()
for SCANNER in  gt_path_dict.keys():
    scn_dic= gt_path_dict[SCANNER]
    for MODEL in scn_dic.keys():
        mod_dic= scn_dic[MODEL]
        mod_search_path = list(mod_dic.values())[0]
        gt_path=os.path.dirname(os.path.dirname(mod_search_path))
        loc_path=os.path.dirname(gt_path)
        if len(glob.glob(mod_search_path)) :
            print(loc_path + " installed!")
        else: 
            print("Install " + loc_path + " by executing:") 
            print('mkdir -p ' + str(loc_path) + '; scp -r ' + ucred + ':' + gt_path + ' ' +  loc_path + '/.') 
        print(" ") 

demFile = os.path.join(session.get_summaries_dir(),'redcap/demographics.csv')
if not os.path.exists(demFile) : 
    red_dir=os.path.dirname(demFile)
    print("Install " + demFile + " by executing:") 
    print('mkdir -p ' + str(red_dir) + '; scp -r ' + ucred + ':' + demFile + ' ' +  red_dir + '/.')


