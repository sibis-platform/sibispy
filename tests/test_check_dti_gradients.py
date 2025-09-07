#!/usr/bin/env python

##
##  Copyright 2017 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##

from __future__ import print_function
from builtins import next
import os
import sys
import glob
import pytest
import sibispy
from sibispy import sibislogger as slog
from sibispy import check_dti_gradients as chk

from . import utils

@pytest.fixture
def session(config_file):
    '''
    Return a sibispy.Session configured by the provided config_file fixture.
    '''
    return utils.get_session(config_file)


@pytest.fixture
def slog():
    '''
    Return a sibislogger instance initialized for a test session.
    '''
    from sibispy import sibislogger as slog
    slog.init_log(False, False,'test_check_dti_gradient', 'test_check_dti_gradient',None)
    return slog


@pytest.mark.xnat
def test_check_dti_gradients(session, slog):

    check = chk.check_dti_gradients()
    if not check.configure(session,check_decimals=2) :
        print("If it fails bc it cannot find ground truth files please execute ../cmd/download_dti_groundtruth.py")
        sys.exit(1)

    gt_path_dict = check.get_ground_truth_gradient_path_dict()
    assert(len(gt_path_dict))
 
    # Make sure ground truth is defined
    
    # go through scanner 
    # check_stack_scn = next(iter(gt_path_dict))
    print(" ")
    for check_stack_scn in gt_path_dict.keys() :
        print("Scanner:", check_stack_scn) 
        scn_dic= gt_path_dict[check_stack_scn]

        # check_stack_mod = next(iter(scn_dic))
        for check_stack_mod in  scn_dic.keys() :
            # default is first one
            print("   Mode:", check_stack_mod)
            mod_dic= scn_dic[check_stack_mod]
    
            # By default whatever is first
            # check_stack_seq = next(iter(mod_dic))
            for check_stack_seq  in mod_dic.keys() :
                print("     Seq:",  check_stack_seq,end=" ")
                check_stack_xml = glob.glob(mod_dic[check_stack_seq])

                # Currently not acquired for all settings
                if "b3000" in check_stack_seq :
                    if len(check_stack_xml ) < 2:
                        print("\033[1mMissing\033[0m")
                        continue

                    # siemens acquires two scans that are named pe1 and pe2 - GE only one scans
                    if  check_stack_scn == "SIEMENS": 
                        if  check_stack_seq == "pe1-dti6b3000" :
                            check_stack_xml = [check_stack_xml[0]]
                        elif  check_stack_seq == "pe2-dti6b3000" :
                            check_stack_xml = [check_stack_xml[1]]
                    # print("===check_stack_xml", check_stack_xml)

                assert(check.check_diffusion('TEST','TEST',check_stack_xml,check_stack_scn,check_stack_mod,"",check_stack_seq))
                print("Passed")


    
    
        
