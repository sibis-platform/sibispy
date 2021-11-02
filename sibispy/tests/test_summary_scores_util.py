#!/usr/bin/env python

import os
import sys
import pytest
import sibispy
from sibispy.summary_scores_util import SummaryScoresCollector
from sibispy import sibislogger as slog

from .utils import get_session, get_test_config

@pytest.fixture
def session(config_file):
    return get_session(config_file)

@pytest.fixture
def xnat_test_data(session):
  return get_test_config('test_summary_scores_util', session)


def test_collect_summary_scores(xnat_test_data):
  collector = SummaryScoresCollector(xnat_test_data['scoring_script_dir'])