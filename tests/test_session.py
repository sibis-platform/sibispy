#!/usr/bin/env python

##
##  Copyright 2016 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##
import os
import sys
import sibispy
from sibispy import sibislogger as slog

slog.init_log(False, False,'test_session', 'test_session','/tmp')

path = os.path.join(os.path.dirname(sys.argv[0]), 'data', '.sibis-general-config.yml')

def test_session_init_path():
    # setting explicitly
    session = sibispy.Session()
    assert(session.configure(config_path=path))
    assert(session.config_path == path)

test_session_init_path()

# Test when variable is set 
os.environ.update(SIBIS_CONFIG=path)
session = sibispy.Session()
assert(session.configure())
os.environ.pop('SIBIS_CONFIG')
assert(session.config_path == path)

for project in ['xnat', 'data_entry'] :
    if not session.connect_server(project):
        print "Info: Make sure " + project + " is correctly defined in " + path 
        sys.exit(1)







