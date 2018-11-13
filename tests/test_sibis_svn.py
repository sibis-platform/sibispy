##
##  Copyright 2018 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##

"""
Unit tests for sibispy.svn module.
"""

from __future__ import absolute_import, print_function, division

import pytest
from pytest_shutil.workspace import Workspace
from path import Path
from datetime import datetime
from ..svn import SibisSvnClient, UpdateActionTypes
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
  ({ ('A', 'file1'), ('A', 'file2'), ('A', 'file3') }, set()),
  ({ ('D', 'file1'), ('U', 'file2'), ('A', 'file4') }, set()),
  ({ ('A', 'file1'), ('U', 'file2'), ('U', 'file4') }, set()),
  ({ ('A', 'alpha/file5'), ('A', 'alpha/file6'), ('A', 'alpha/file7') }, set([('A', 'alpha')])),
  ({ ('D', 'alpha/file5'), ('U', 'alpha/file6'), ('U', 'alpha/file7') }, set()),
  ({ ('A', 'alpha/file5'), ('U', 'alpha/file6'), ('D', 'alpha/file7') }, set()),
  ({ ('A', 'beta/file5'), ('A', 'beta/file6'), ('A', 'beta/file7') }, set([('A', 'beta')])),
  ({ ('D', 'beta/file5'), ('U', 'beta/file6'), ('U', 'beta/file7') }, set()),
  ({ ('A', 'beta/file5'), ('U', 'beta/file6'), ('D', 'beta/file7') }, set())
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
    for action, filename in commit:
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
  txt_file2.write_text('local conflict\n', append=True)
  changes = client.update(revision=2)

  assert changes.revision == 2, "Expected to be a revision 2, got: {}".format(changes.revision)
  assert (UpdateActionTypes.conflicted, 'file2') in changes.actions, "Expected to find a conflict"
  

def test_sibis_svn_update_merge(mock_repo):
  mock_ws, co_dir = mock_repo
  mock_ws.run("svn up -r1", cd=co_dir)

  client = SibisSvnClient(co_dir)
  assert client, "Client should not be None"
  
  txt_file2 = co_dir / 'file2'
  ext_contents = txt_file2.bytes()
  txt_file2.write_text('local conflict\n'+ext_contents)
  changes = client.update(revision=2)

  assert changes.revision == 2, "Expected to be a revision 2, got: {}".format(changes.revision)
  assert (UpdateActionTypes.merged, 'file2') in changes.actions, "Expected to find a merge"


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



