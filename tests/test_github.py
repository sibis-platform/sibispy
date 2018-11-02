from __future__ import absolute_import, print_function

import pytest
from .. import post_issues_to_github as gh

def test_ping_github():
  result = gh.ping_github()
  assert 0 == result, "expected 0, got {0}".format(result)


def test_connect_to_github(config_file):
  repo = gh.connect_to_github(config_file, True)
  assert repo is not None, "expected repo to not be none"