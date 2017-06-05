#!/usr/bin/env python

##
##  Copyright 2017 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##

import os
import sys
from sibispy import config_file_parser as parser

def test_init_path():
    # setting explicitly
    cfp = parser.config_file_parser()
    assert(not cfp.configure(config_file=path))
    
    assert(cfp.get_config_file() == path)

#
# MAIN
#


path = os.path.join(os.path.dirname(sys.argv[0]), 'data', '.sibis-general-config.yml')

test_init_path()

# Test when variable is set 
os.environ.update(SIBIS_CONFIG=path)
cfg = parser.config_file_parser()
assert(not cfg.configure())
os.environ.pop('SIBIS_CONFIG')
assert(cfg.get_config_file() == path)
assert(cfg.get_value('logdir'))
assert(not cfg.get_value('blub','blubber'))
assert(cfg.get_value('xnat','server'))
assert(cfg.get_category('xnat'))
