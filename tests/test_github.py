from __future__ import absolute_import, print_function

import pytest
from .. import post_issues_to_github as gh

from sibispy import session as sess


####################
## HOOKS
####################

def pytest_generate_tests(metafunc):
  config_file = metafunc.config.option.config_file
  if config_file is not None:
    if 'github_label' in metafunc.fixturenames:
      session = get_session(config_file)
      test_config = get_test_config(session)
      github_labels = test_config['github_labels']
      metafunc.parametrize('github_label', github_labels)


####################
## HELPERS
####################

def get_session(config_file):
  session = sess.Session()
  assert session.configure(config_file), "Configuration File `{}` is missing or not readable.".format(config_file)
  return session

def get_test_config(session):
  parser, error = session.get_config_test_parser()
  assert error is None, "Error: getting test config: "+error
  return parser.get_category('test_post_to_github')

####################
## FIXTURES
####################


@pytest.fixture
def session(config_file):
  return get_session(config_file)

@pytest.fixture
def test_config(session):
  return get_test_config(session)

@pytest.fixture
def github_repo(config_file):
  return gh.connect_to_github(config_file, True)


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
