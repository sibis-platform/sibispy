#!/usr/bin/env python

##
##  Copyright 2016 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##

# if test script is run with argument then it will run script with the sibis config file defined by that argument
# for example test_session.py ~/.sibis-general-config.yml
# otherwise will run with data/.sibis-general-config.yml

from __future__ import absolute_import
from __future__ import print_function
from builtins import str
import os
import pytest
from sibispy import session as sess
from . import utils


@pytest.fixture
def session(config_file):
    """
    Return a sibispy.Session configured by the provided config_file fixture.
    """
    return utils.get_session(config_file)


####################
## HOOKS
####################

def pytest_generate_tests(metafunc):
  config_file = metafunc.config.option.config_file
  if config_file is not None:
    if 'github_label' in metafunc.fixturenames:
      session = utils.get_session(config_file)
      test_config = utils.get_test_config('test_post_to_github', session)
      github_labels = test_config['github_labels']
      metafunc.parametrize('github_label', github_labels)


####################
## TESTS
####################


DIR_VARS = [
    "/fs/ncanda-share/log/back-nightly/",
    "/fs/ncanda-share/log/check_new_sessions/",
    "/fs/ncanda-share/log/check_unuploaded_files/",
    "/fs/ncanda-share/log/export_mr_sessions_pipeline/",
    "/fs/ncanda-share/log/front-hourly/",
    "/fs/ncanda-share/log/make_all_inventories/",
    "/fs/ncanda-share/log/nextcloud/",
    "/fs/ncanda-share/log/qc/",
    "/fs/ncanda-share/log/status_reports/",
]

@pytest.mark.parametrize("dir_var", DIR_VARS)
def test_environment_var_dir_exists(dir_var):
  assert os.path.isdir(dir_var), "expected dir `{0}` to exist".format(dir_var)
