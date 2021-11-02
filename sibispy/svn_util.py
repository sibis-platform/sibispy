##
##  Copyright 2018 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##
from __future__ import absolute_import, print_function
from builtins import str
from builtins import object
from svn.local import LocalClient
from svn.exception import SvnException
from collections import namedtuple
import os
import xml

class UpdateActionTypes(object):
  """
  SVN 1.8 Update action types.
  """
  added = 'A'
  broken_lock = 'B'
  deleted = 'D'
  updated = 'U'
  conflicted = 'C'
  merged = 'G'
  existed = 'E'


UpdateAction_ = namedtuple('UpdateAction', ['action', 'path', 'kind'])
class UpdateAction(UpdateAction_):
  """
  Action performed to a single file or dir as result of `svn update`
  """
  pass

class SibisSvnUpdate(object):
  """
  Changelog status performed by an SVN update.

  Attributes:
    target           (str): The file or dirctory where svn update was performed.
    revision         (int): The current revision of the working directory
    actions   (set(tuple)): The set of (action, filename) performed by the update.
  """
  def __init__(self, work_dir, cmd_output=[]):
    """
    Constructor for SibisSvnUpdate.

    Parameters:
      cmd_output (list(str)): raw command output from an `svn update`. each element 
                              in the list represents one line of output.
    """
    if len(cmd_output) >= 2:
      primary_content = []
      self.warnings = []
      self.errors = []
      for l in cmd_output:
        if l.startswith('Summary of '):
          break
        if l.startswith('svn: warning: '):
          self.warnings.append(l[14:])
        elif l.startswith('svn: error: '):
          self.errors.append(l[12:])
        else:
          primary_content.append(l)
      
      header = primary_content[0]
      body = primary_content[1:-1]
      footer = primary_content[-1]

      ## get the file or folder updated
      ts = header.index(r"'")+1
      te = header.rindex(r"'")
      self.target = header[ts:te]

      ## get the version
      fs = footer.rindex(r' ')+1
      fe = footer.rindex(r'.')
      self.revision = int(footer[fs:fe])

      self.actions = set()
      for line in body:
        action, path = line.split(None, 1)
        abspath = os.path.join(work_dir, path)
        if os.path.exists(abspath) and os.path.isdir(abspath):
          kind = 'dir'
        elif (os.path.exists(abspath) and os.path.isfile(abspath)):
          kind = 'file'
        else:
          kind = 'none'
        self.actions.add(UpdateAction(action, path, kind))


class SibisSvnDiffPath(object):
  class Kind(object):
    dir = "dir"
    file = "file"
  
  class Props(object):
    none = "none"
    modified = "modified"
  
  class Item(object):
    none = "none"
    added = "added"
    modified = "modified"
    deleted = "deleted"

  def __init__(self, path, kind, item='none', props='none'):
    self.path = path
    self.kind = kind
    self.item = item
    self.props = props


class SibisSvnDiff(object):
  def __init__(self, summary_diff=""):
    root = xml.etree.ElementTree.fromstring(summary_diff)

    self.paths=[]
    for path in root.iter('path'):
      attrib = path.attrib
      self.paths.append(SibisSvnDiffPath(path.text, **attrib))

  def files_changed(self, include_deleted=False):
    files_changed = []
    for pth in self.paths:
      if not include_deleted and pth.item == SibisSvnDiffPath.Item.deleted:
        continue
      if pth.kind == SibisSvnDiffPath.Kind.file:
        files_changed.append(pth.path) 
    return files_changed

class SibisSvnClient(LocalClient):
  """
  Subversion client wrapper around svn module so we can normalize functionality
  when changing svn libraries to something more portable than Tigris' pysvn.

  Assumes that the path provided is an existing svn working directory.
  """
  _DEFAULT_ENV = {
    'LANG': 'C.UTF-8',
    'LANGUAGE': 'C.UTF-8',
    'LC_ALL': 'C.UTF-8'
  }

  def __init__(self, path_, env=_DEFAULT_ENV, *args, **kwargs):
    """
    Constructor for SibisSvnClient class.

    Parameters:
      path_     (str):  path to svn working directory
      env      (dict):  [optional] dictionary of environment vars to add to svn process
      username  (str):  [optional] svn repository username
      password  (str):  [optional] svn repository password
    """
    super(SibisSvnClient, self).__init__(path_, env=env, *args, **kwargs)


  def update(self, rel_filepaths=[], revision=None):
    """
    Update the current working directory to current revision.
    
    NOTE: Output parsing is unreliable, and svn update output is a bit unreliable. If you need a changelog
    it is reccommended to perform something like this instead:

    >>> client = SibisSvnClient('/path/to/workdir')
    >>> info = client.info()
    >>> start_rev = info['entry_revision']
    >>> changes = client.update()
    >>> diff = client.diff_path(start_rev)
    >>> changed_files = diff.files_changed(True)

    Parameters:
      rel_filepaths (list(str)):    list of relative subpaths to update
      revision      (int or str):   The revision number to update to. None updates to HEAD. Default: None

    Returns: SibisSvnUpdate
    """
    cmd = []
    if revision is not None:
      cmd += ['-r', str(revision)]
    cmd += rel_filepaths
    cmd_out = self.run_command(
      'update',
      cmd,
      wd=self.path)
    return SibisSvnUpdate(self.path, cmd_out)
  
  def log(self, *args, **kwargs):
    """
    See svn.common.CommonClient.log_default for parameters.
    """
    return self.log_default(*args, **kwargs)

  def diff_path(self, revision, rel_filepath=None):
    cmd = ['--summarize', '--xml']
    if revision is not None:
      cmd += ['-r', str(revision)]
    
    if rel_filepath is not None:
      work_dir = os.path.join(self.path, rel_filepath)
    else:
      work_dir = self.path
    
    try:
      cmd_out = self.run_command('diff', cmd, wd=work_dir, do_combine=True)
    except SvnException as e:
      raise SibisSvnException(e)


    return SibisSvnDiff(cmd_out)

class SibisSvnException(Exception):
  def __init__(self, *args, **kwargs):
    super(SibisSvnException, self).__init__(*args, **kwargs)
