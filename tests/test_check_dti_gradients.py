#!/usr/bin/env python

##
##  Copyright 2017 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##

import os
import sys
import glob
import sibispy
from sibispy import sibislogger as slog
from sibispy import check_dti_gradients as chk

if sys.argv.__len__() > 1 : 
    config_file = sys.argv[1]
else :
    config_file = os.path.join(os.path.dirname(sys.argv[0]), 'data', '.sibis-general-config.yml')

slog.init_log(False, False,'test_check_dti_gradient', 'test_check_dti_gradient',None)

session = sibispy.Session()
assert(session.configure(config_file))

check = chk.check_dti_gradients()
if not check.configure(session,check_decimals=2) :
    print "If it fails bc it cannot find ground truth files please execute ../cmd/download_dti_groundtruth.py"
    sys.exit(1)

gt_path_dict = check.get_ground_truth_gradient_path_dict()
assert(len(gt_path_dict))
 
# Make sure ground truth is defined 
check_stack_scn = next(iter(gt_path_dict))
scn_dic= gt_path_dict[check_stack_scn]
check_stack_mod = next(iter(scn_dic))
mod_dic= scn_dic[check_stack_mod]
check_stack_seq = next(iter(mod_dic))
check_stack_xml = glob.glob(mod_dic[check_stack_seq])

assert(check.check_diffusion('TEST','TEST',check_stack_xml,check_stack_scn,check_stack_mod,"",check_stack_seq))
