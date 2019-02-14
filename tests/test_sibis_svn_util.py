##
##  Copyright 2018 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##

"""
Unit tests for sibispy.svn module.
"""

from __future__ import absolute_import, print_function, division

from builtins import str
import pytest
from pytest_shutil.workspace import Workspace
from path import Path
from datetime import datetime
from ..svn_util import SibisSvnClient, UpdateActionTypes, SibisSvnException, UpdateAction
import tempfile

# pytest_plugins = ['pytest_svn']

def ensure_path(svn_repo, repo_name, rel_file):
  path = svn_repo.workspace / repo_name
  split_path = rel_file.rsplit('/',1)
  if len(split_path) == 2:
    parent_path = path / split_path[0]
    if not parent_path.exists():
      parent_path.makedirs_p()
      svn_repo.run("svn add {}".format(split_path[0]), cd=path)
  else:
    path.makedirs_p()
  return path

def del_file(svn_repo, repo_name, rel_file):
  path = ensure_path(svn_repo, repo_name, rel_file)
  txt_file = path / Path(rel_file)
  msg = "Deleted File {} @ {}".format(txt_file, datetime.now())
  svn_repo.run("svn delete {}".format(txt_file), cd=path)
  return msg


def mod_file(svn_repo, repo_name, rel_file):
  path = ensure_path(svn_repo, repo_name, rel_file)
  txt_file = path / Path(rel_file)
  file_exists = txt_file.exists()
  msg = "Modified File {} @ {}".format(txt_file, datetime.now())
  txt_file.write_text(msg+'\n', append=True)

  if (not file_exists):
    svn_repo.run("svn add {}".format(txt_file), cd=path)
  return msg

mock_repo_commits = [
  ({ UpdateAction('A', 'file1', 'file'), UpdateAction('A', 'file2', 'file'), UpdateAction('A', 'file3', 'file') }, set()),
  ({ UpdateAction('D', 'file1', 'none'), UpdateAction('U', 'file2', 'file'), UpdateAction('A', 'file4', 'file') }, set()),
  ({ UpdateAction('A', 'file1', 'file'), UpdateAction('U', 'file2', 'file'), UpdateAction('U', 'file4', 'file') }, set()),
  ({ UpdateAction('A', 'alpha/file5', 'file'), UpdateAction('A', 'alpha/file6', 'file'), UpdateAction('A', 'alpha/file7', 'file') }, set([UpdateAction('A', 'alpha', 'dir')])),
  ({ UpdateAction('D', 'alpha/file5', 'none'), UpdateAction('U', 'alpha/file6', 'file'), UpdateAction('U', 'alpha/file7', 'file') }, set()),
  ({ UpdateAction('A', 'alpha/file5', 'file'), UpdateAction('U', 'alpha/file6', 'file'), UpdateAction('D', 'alpha/file7', 'none') }, set()),
  ({ UpdateAction('A', 'beta/file5', 'file'), UpdateAction('A', 'beta/file6', 'file'), UpdateAction('A', 'beta/file7', 'file') }, set([UpdateAction('A', 'beta', 'dir')])),
  ({ UpdateAction('D', 'beta/file5', 'none'), UpdateAction('U', 'beta/file6', 'file'), UpdateAction('U', 'beta/file7', 'file') }, set()),
  ({ UpdateAction('A', 'beta/file5', 'file'), UpdateAction('U', 'beta/file6', 'file'), UpdateAction('D', 'beta/file7', 'none') }, set()),
  ({ UpdateAction('U', 'beta/file6', 'file'), }, set())
]

repo_actions = {
  'A': mod_file,
  'U': mod_file,
  'D': del_file
}

@pytest.yield_fixture
def svn_workdir(svn_repo):
  workdir = Workspace()
  workdir.run("svn co {}".format(svn_repo.uri))
  yield (workdir, svn_repo)
  workdir.teardown()


@pytest.fixture
def mock_repo(svn_workdir):
  workdir, repo = svn_workdir
  repo_name = str(repo.workspace.name)
  co_dir = workdir.workspace / repo_name

  for commit, _ in mock_repo_commits:
    commit_msg = []
    for action, filename, _ in commit:
      commit_msg.append(repo_actions[action](workdir, repo_name, filename))
    workdir.run("svn ci -m \"{}\"".format("\\n".join(commit_msg)), cd=co_dir)
  return (workdir, co_dir)


def test_sibis_svn_update(mock_repo):
  mock_ws, co_dir = mock_repo
  mock_ws.run("svn up -r0", cd=co_dir)

  client = SibisSvnClient(co_dir)
  assert client, "Client should not be None"

  for rev, (commit, diff) in enumerate(mock_repo_commits, 1): 
    changes = client.update(revision=rev)
    assert changes, "Changes should not be None"
    set_diff = changes.actions.difference(commit)
    assert changes.revision == rev, "Expected revision {}, got: {}".format(rev, changes.revision)
    assert changes.target == '.', "Expected target of . for rev {}, got: {}".format(rev, changes.target)
    assert diff == set_diff, "Change actions for rev {} expected {}, difference {}".format(rev, repr(commit), repr(set_diff))

def test_sibis_svn_update_conflict(mock_repo):
  mock_ws, co_dir = mock_repo
  mock_ws.run("svn up -r1", cd=co_dir)

  client = SibisSvnClient(co_dir)
  assert client, "Client should not be None"
  
  txt_file2 = co_dir / 'file2'
  txt_file2.write_text(r'local conflict\n', append=True)
  changes = client.update(revision=2)

  assert changes.revision == 2, "Expected to be a revision 2, got: {}".format(changes.revision)
  assert UpdateAction(UpdateActionTypes.conflicted, 'file2', 'file') in changes.actions, "Expected to find a conflict"
  

def test_sibis_svn_update_merge(mock_repo):
  mock_ws, co_dir = mock_repo
  mock_ws.run("svn up -r9", cd=co_dir)

  client = SibisSvnClient(co_dir)
  assert client, "Client should not be None"
  
  txt_file2 = co_dir / 'beta' / 'file6'
  ext_contents = txt_file2.bytes()
  txt_file2.write_text(r'local conflict\n'+str(ext_contents))
  changes = client.update(revision=10)

  assert changes.revision == 10, "Expected to be a revision 10, got: {}".format(changes.revision)
  assert UpdateAction(UpdateActionTypes.conflicted, 'beta/file6', 'file') in changes.actions, "Expected to find a conflict"


def test_sibis_svn_update_no_change(mock_repo):
  mock_ws, co_dir = mock_repo

  client = SibisSvnClient(co_dir)
  assert client, "Client should not be None"

  changes = client.update()
  assert changes.revision == len(mock_repo_commits), "updated revision is incorrect"
  assert changes.actions == set([]), "should be an empty set"


  changes = client.update()
  assert changes.revision == len(mock_repo_commits), "updated revision is incorrect"
  assert changes.actions == set([]), "should be an empty set"

def test_sibis_svn_info(mock_repo):
  mock_ws, co_dir = mock_repo

  client = SibisSvnClient(co_dir)
  assert client, "Client should not be None"

  resp = client.info()
  assert resp != None, "Response from `svn info` should not be None"
  assert len(list(resp)) > 0, "Response from `svn info` should have keys"


def test_sibis_svn_log(mock_repo):
  mock_ws, co_dir = mock_repo

  client = SibisSvnClient(co_dir)
  assert client, "Client should not be None"

  resp = client.log()
  assert resp != None, "Response from `svn log` should not be None"
  print('\n'.join(resp))
  # assert len(list(resp)) > 0, "Response from `svn info` should have keys"

  resp = client.log(rel_filepath='./alpha')
  assert resp != None, "Response from `svn log ./alpha` should not be None"
  print(repr(resp))
  # assert len(list(resp)) > 0, "Response from `svn info` should have keys"


def test_sibis_svn_diff_path(mock_repo):
  mock_ws, co_dir = mock_repo

  client = SibisSvnClient(co_dir)
  assert client, "Client should not be None"

  resp = client.diff_path(revision=0)

  assert resp, "Response should not be None"

  assert len(resp.files_changed()) == 8, "There should have been 8 files changed."

  resp = client.diff_path(revision=7)
  assert len(resp.files_changed()) == 2, "There should have been 2 files changed."

  with pytest.raises(SibisSvnException, message="Expecting a revision error"):
    resp = client.diff_path(revision=10)
 

def test_sibis_info_update_diff(mock_repo):
  mock_ws, co_dir = mock_repo
  mock_ws.run("svn up -r2", cd=co_dir)

  client = SibisSvnClient(co_dir)
  assert client, "Client should not be None"

  info = client.info()
  start_rev = info['entry_revision']
  assert int(start_rev) == 2, "Should be commit version 2, got: "+start_rev

  changes = client.update()
  assert changes.revision == 9, "Should be at revision 9, got: "+str(changes.revision)

  diff = client.diff_path(start_rev)
  changed_files = diff.files_changed(True)
  delta = {x.path for x in changes.actions }.difference(set(changed_files)) 
  assert delta == set(['alpha', 'beta']), "Expected only the directories to be different: got: {}".format(repr(delta))

