#!/usr/bin/env python

##
##  Copyright 2016 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##
import os
import sys
import sibispy
path = os.path.join(os.path.dirname(sys.argv[0]), 'data', '.sibis-general-config.yml')

def test_session_init_path():
    # setting explicitly
    session = sibispy.Session()
    assert(session.configure(config_path=path,initiate_slog=True))
    assert(session.config_path == path)

# test_session_init_env():
os.environ.update(SIBIS_CONFIG=path)
session = sibispy.Session()
assert(session.configure(initiate_slog=True))
os.environ.pop('SIBIS_CONFIG')
assert(session.config_path == path)

if not session.connect_server('data_entry') :
    print "Info: Make sure data_entry token is correctly defined in " + path 
    sys.exit(1)







