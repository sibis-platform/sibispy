##
##  Copyright 2018 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##


from __future__ import absolute_import, print_function
from svn.local import LocalClient


class UpdateActionTypes():
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


class SibisSvnUpdate():
  """
  Changelog status performed by an SVN update.

  Attributes:
    target    (str):      The file or dirctory where svn update was performed.
    revision  (int):      The current revision of the working directory
    actions   (set(set)): The set of (action, filename) performed by the update.
  """

  def __init__(self, cmd_output=[]):
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
        self.actions.add((action, path))
      

class SibisSvnClient(LocalClient):
  """
  Subversion client wrapper around svn module so we can normalize functionality
  when changing svn libraries to something more portable than Tigris' pysvn.

  Assumes that the path provided is an existing svn working directory.
  """
  
  def __init__(self, path_, *args, **kwargs):
    """
    Constructor for SibisSvnClient class.

    Parameters:
      path_     (str):  path to svn working directory
      username  (str):  [optional] svn repository username
      password  (str):  [optional] svn repository password
    """
    super(SibisSvnClient, self).__init__(path_, *args, **kwargs)

  def update(self, rel_filepaths=[], revision=None):
    """
    Update the current working directory to current revision.

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
    return SibisSvnUpdate(cmd_out)