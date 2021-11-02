from __future__ import absolute_import, print_function

import pytest
import tempfile
import os
from . import utils
from .. import post_issues_to_github as gh

from sibispy import session as sess


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
## FIXTURES
####################


@pytest.fixture
def session(config_file):
  return utils.get_session(config_file)

@pytest.fixture
def test_config(session):
  return utils.get_test_config(session)

@pytest.fixture
def github_repo(config_file):
  return gh.connect_to_github(config_file, True)

ISSUES = [
  '{"experiment_site_id": "sibislogger_test_1", "error": "Unit Testing 1", "msg": "Please ignore message"}',
  '{"experiment_site_id": "sibislogger_test_2", "error": "Unit Testing 2", "msg": "Please ignore message"}'
]


@pytest.fixture
def github_issues():
  issue_file, file_name = tempfile.mkstemp(suffix="issue_file", text=True)
  with open(file_name, 'w') as f:
    for issue in ISSUES:
      f.write(issue+'\n')
  yield (issue_file, file_name)
  os.remove(file_name)




####################
## TESTS
####################


def test_ping_github():
  result = gh.ping_github()
  assert 0 == result, "expected 0, got {0}".format(result)

def test_connect_to_github(github_repo):
  assert github_repo is not None, "expected repo to not be none"


def test_get_github_label(github_repo, github_label):
  found = gh.get_github_label(github_repo, github_label)
  
  assert [None] != found, "expected label to already exist"
  assert github_label == found[0].name, "expected label `{}` to match, got `{}`".format(github_label, found)

def test_post_issues_to_github(github_issues, config_file):
  parser = gh.get_argument_parser()
  assert parser != None, "argument parser is None"
  issue_filename = github_issues[1]

  open_args = parser.parse_args('--title  "Unit Test (testing)"  --body  {}  --config  {}'.format(issue_filename, config_file).split('  '))
  assert gh.main(open_args) != 1, "Expected issue opening to succeed"

  close_args = parser.parse_args('--title  "Unit Test (testing)"  --body  {}  --close  --config  {}'.format(issue_filename, config_file).split('  '))
  assert gh.main(close_args) != 1, "Expected issue closing to succeed"


