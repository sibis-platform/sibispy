from __future__ import absolute_import, print_function

import pytest
from .. import post_issues_to_github as gh

@pytest.fixture
def github_repo(config_file):
  return gh.connect_to_github(config_file, True)

def test_ping_github():
  result = gh.ping_github()
  assert 0 == result, "expected 0, got {0}".format(result)

def test_connect_to_github(github_repo):
  assert github_repo is not None, "expected repo to not be none"


@pytest.mark.parametrize("github_label", [
  "check_exceptions",
  "check_new_sessions",
  "check_object_names",
  "check_phantom_scans",
  "check_subject_ids",
  "csv2redcap",
  "export_measures",
  "get_results_selenium",
  "harvester",
  "import-laptops-csv2redcap",
  "import_mr_sessions",
  "phantom_qa",
  "redcap_update_summary_scores",
  "update_bulk_forms",
  "update_summary_forms",
  "update_summary_scores",
  "update_visit_data"
])
def test_get_github_label(github_repo, github_label):
  found = gh.get_github_label(github_repo, github_label)
  
  assert [None] != found, "expected label to already exist"
  assert github_label == found[0].name, "expected label `{}` to match, got `{}`".format(github_label, found)
