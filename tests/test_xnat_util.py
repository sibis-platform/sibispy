import os
import pytest
import sys
import yaml

from ..xnat_util import XnatUtil
from ..xnat.jsonutil import JsonTable

import xnat

@pytest.fixture
def general_config_file():
  config_file = os.path.join(os.path.dirname(__file__), 'data', '.sibis-general-config.yml')
  return config_file

@pytest.fixture
def general_config(config_file):
  cfg = None
  with open(config_file, 'r') as f:
    cfg = yaml.load(f)
  return cfg

@pytest.fixture
def xnat_config(general_config):
  return general_config["xnat"]

@pytest.fixture
def xnat_util(xnat_config):
  util = XnatUtil(xnat_config["server"], xnat_config["user"], xnat_config["password"])
  assert util != None, "'XnatUtil' should not be None"
  return util

def test_search_all_mr_sessions(xnat_util):
  
  with xnat_util.connect() as client:
    assert client != None, "`xnat` should not be none"

    #extract all session ids with session insert date, session project and sesion scan damrSessionDatate
    sess = xnat_util.search( 'xnat:mrSessionData', ['xnat:mrSessionData/SESSION_ID','xnat:mrSessionData/INSERT_DATE','xnat:mrSessionData/PROJECT','xnat:mrSessionData/DATE']).all()
    
    assert isinstance(sess, JsonTable), "`sess` should be an instance of JsonTable"

    expected = set(['session_id', 'insert_date', 'project', 'date'])
    assert set(sess.headers()) == expected, "unexpected headers. got {}".format(sess.headers())

def test_get_json(xnat_util):
  with xnat_util.connect() as client:
    assert client != None, "`xnat` should not be none"

    content = xnat_util._get_json('/data/experiments')

    assert content != None, "content should not be None"
    assert content['ResultSet']['Result'][0] != None, "should be at least one result"


def test_monkey_patch(xnat_util):
  with xnat_util.connect() as client:
    all_experiments = xnat_util.select.experiments

    assert isinstance(all_experiments, xnat.core.XNATListing), "unexpected instance type! got: {}".format(type(all_experiments))
    
    experiment = all_experiments.get(0)
    assert experiment != None, "Should have at least one experiment"

    import pdb; pdb.set_trace()

    pass
#     assert xnat != None, "`xnat` should not be none"

#     xnat_eid = 'CENTRAL_E07089'
# #/%s/files
#     content = xnat_util._get_json('/data/experiments')

#     import pdb; pdb.set_trace()

#     assert content != None, "content should not be None"

