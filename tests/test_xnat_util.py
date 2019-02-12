from __future__ import division
from builtins import str
from past.utils import old_div
import os
import pytest
import sys
import yaml
import uuid

from pytest_shutil.workspace import Workspace
from ..xnat_util import XnatUtil, XNATSessionElementUtil, XNATResourceUtil, XNATExperimentUtil
from ..xnat.jsonutil import JsonTable
from .utils import get_session, get_test_config

import xnat


@pytest.yield_fixture
def workdir():
  workdir = Workspace()
  yield workdir
  workdir.teardown()

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

@pytest.fixture
def session(config_file):
  return get_session(config_file)

@pytest.fixture
def xnat_test_data(session):
  return get_test_config('test_xnat_util', session)

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
    assert content[0] != None, "should be at least one result"


def test_monkey_patch(xnat_util):
  with xnat_util.connect() as client:
    all_experiments = xnat_util.select.experiments

    assert isinstance(all_experiments, xnat.core.XNATListing), "unexpected instance type! got: {}".format(type(all_experiments))
    
    experiment = all_experiments.get(0)
    assert experiment != None, "Should have at least one experiment"



def test_raw_text(xnat_util, xnat_test_data):
  test_data = xnat_test_data['raw_text']
  with xnat_util.connect() as client:
    for td in test_data:
      for scan in td['scans']:
        element = xnat_util.select.experiments[td['experiment']].scans[scan]
        result = xnat_util.raw_text(element)
        assert isinstance(result, str), "Expected a unicode string. got: {}".format(result)

def test_download_file(xnat_util, xnat_test_data, tmpdir):
  test_data = xnat_test_data['download_file']
  with xnat_util.connect() as client:
    for td in test_data:
      target = tmpdir.mkdir("{experiment}_{resource}".format(**td)).join(td['file'])
      fileData = xnat_util.download_file(td['experiment'], td['resource'], td['file'], target.__str__(), format='text')
      assert target.check(), "File should exist!"


def test_get_attribute(xnat_util, xnat_test_data):
  test_data = xnat_test_data['get_elem_attribute']
  with xnat_util.connect() as client:
    experiment = client.experiments[test_data['experiment_id']]

    assert experiment != None, "Experiment should have existed."

    elem = XNATSessionElementUtil(experiment)
    val = elem.get(test_data['column'])

    assert val != None and val != "", "Value should not be None or empty."


def test_get_array_experiments(xnat_util, xnat_test_data):
  test_data = xnat_test_data['get_array_experiments']
  with xnat_util.connect() as client:
    for test in test_data:
      phantom_id = test['phantom_id'] 
      edate = test['edate']
    
      phantom_scans = xnat_util.array.experiments(experiment_type='xnat:mrSessionData', constraints={ 'xnat:mrSessionData/subject_id':phantom_id, 'date': edate})
      assert phantom_scans != None


def test_get_attribute_list(xnat_util, xnat_test_data):
  test_data = xnat_test_data['get_elem_attribute_list']
  with xnat_util.connect() as client:
    experiment = client.experiments[test_data['experiment_id']]

    assert experiment != None, "Experiment should have existed."

    elem = XNATSessionElementUtil(experiment)
    vals = elem.mget(test_data['columns'])

    assert len(vals) == len(test_data['columns']), "The number of columns requested should match number of values returned."

    for i, v in enumerate(vals):
      assert v != None and v != "", "Value for {} should not None or empty".format(test_data['columns'][i])


def test_download_upload_file(xnat_util, xnat_test_data, workdir):
  test_data = xnat_test_data['upload_file']
  with xnat_util.connect() as client:
    exp = client.experiments[test_data['experiment']]
    resource = exp.resources[test_data['resource']]

    assert resource != None, "Resource should exist"

    local_file = old_div(workdir.workspace, test_data['file_name'])
    download_file = old_div(workdir.workspace, 'downloaded_') + test_data['file_name']

    random_data = str(uuid.uuid1())

    with open(local_file, 'w+') as out:
      out.write(random_data)

    resource_util = XNATResourceUtil(resource)
    assert resource_util != None, "Patched resource should exist"
    resource_util.detailed_upload(local_file, test_data['file_name'], overwrite=True, tags='test_alpha,test_centauri', content='Test File', format='text')

    updated_file = resource.files[test_data['file_name']]
    assert updated_file != None, "File should exist"
    updated_file.download(download_file)

    assert download_file.exists(), "File should have downloaded"

    downloaded = None
    with open(download_file, 'r') as f:
      downloaded = f.readline()

    assert downloaded == random_data, "File doesn't match"


def test_xnat_experimentutil_summarize(xnat_util, xnat_test_data):
  exp_id = xnat_test_data['xnat_experimentutil_summarize']['experiment']
  with xnat_util.connect() as client:
    exp = client.experiments[exp_id]

    util = XNATExperimentUtil(exp)

    scan_types = util.summarize('scans/scan', 'type')

    assert scan_types != [], "should not be empty"
  



#     assert xnat != None, "`xnat` should not be none"

#     xnat_eid = 'CENTRAL_E07089'
# #/%s/files
#     content = xnat_util._get_json('/data/experiments')

#     import pdb; pdb.set_trace()

#     assert content != None, "content should not be None"

