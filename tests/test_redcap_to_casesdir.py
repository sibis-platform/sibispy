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
from sibispy import redcap_to_casesdir as r2c

if sys.argv.__len__() > 1 : 
    config_file = sys.argv[1]
else :
    config_file = os.path.join(os.path.dirname(sys.argv[0]), 'data', '.sibis-general-config.yml')

slog.init_log(False, False,'test_check_dti_gradient', 'test_check_dti_gradient',None)

session = sibispy.Session()
assert(session.configure(config_file))

redcap_project = session.connect_server('data_entry', True) 
assert(redcap_project)

red2cas = r2c.redcap_to_casesdir() 
if not red2cas.configure(session,redcap_project.metadata) :
    sys.exit(1)

assert(red2cas.create_demographic_datadict("/tmp"))
print "Wrote to /tmp/demographic.csv"

