#!/usr/bin/env python

##
##  Copyright 2016 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##

# if test script is run with argument then it will run script with the sibis config file defined by that argument 
# for example test_session.py ~/.sibis-general-config.yml 
# otherwise will run with data/.sibis-general-config.yml


import os
import sys
import pytest
import sibispy
from sibispy import sibis_email 
from sibispy import sibislogger as slog

from .utils import get_session



#
# MAIN
#

# if sys.argv.__len__() > 1 : 
#     config_file = sys.argv[1]
# else :
#     config_file = os.path.join(os.path.dirname(sys.argv[0]), 'data', '.sibis-general-config.yml')

# slog.init_log(False, False,'test_sibis_email', 'test_sibis_email')

@pytest.fixture
def session(config_file):
    return get_session(config_file)

@pytest.fixture
def email_adr(session):
    return session.get_email()

# session = sibispy.Session()
# session.configure(config_file)
# email_adr = session.get_email()

#
# Test sibis_email
#
def test_sibis_email(email_adr):
    smail = sibis_email.sibis_email('localhost.localdomain',email_adr)
    assert(smail.send('General-Test', 'test@email.com', [ email_adr ], 'TEST MESSAGE' ))

    smail.add_user_message('testA', 'user-test 1', 't', 'estA', email_adr)
    smail.add_user_message('testA', 'user-test 2')
    smail.add_user_message('testB', 'user-test 3', 'te', 'stB', email_adr)
    smail.add_admin_message('admin-test 1')
    smail.send_all('test_sibis_email','TEST USER INTRO', 'TEST_PROLOG', 'TEST ADMIN INTRO')


#
# Test subclass 
#
def test_sibis_email_subclass(session):
    xnat_util = session.connect_server('xnat')
    xmail = sibis_email.xnat_email(session)

    # If we cannot find admin user everything will be sent to first user in the list 
    uList = list(xnat_util.client.users) 
    if 'admin' in uList : 
        uName = 'admin'
    else :
        uName = uList[0]
        
    xmail.add_user_message(uName, 'xnat-user-test 1')
    xmail.add_user_message(uName, 'xnat-user-test 2')
    # xmail.dump_all()
    xmail.send_all()
