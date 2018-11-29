
from __future__ import absolute_import, print_function

from builtins import object
from .xnat.search import Search
import xnat



# jimklo: #stupidpythontricks monkey patch XNATBaseListing so you can access __get_item__ via __call__.
def call_getitem(self, item=None):
  if item == None:
    return self.listing

  return self.__getitem__(item)
xnat.core.XNATBaseListing.__call__ = call_getitem


class XnatUtil(object):
  def __init__(self, server, user=None, password=None):
    self._server = server
    self._user = user
    self._password = password
    self._xnat = None
  
  def __del__(self):
    if self._xnat != None:
      self._xnat.disconnect()
      self._xnat = None

  def connect(self, verify=True, debug=False, loglevel=None):
    self._xnat = xnat.connect(self._server, user=self._user, password=self._password, verify=verify, debug=debug, loglevel=loglevel)
    return self._xnat

  def _get_json(self, uri):
    """ Specific Interface._exec method to retrieve data.
    It forces the data format to csv and then puts it back to a
    json-like format.

    Parameters
    ----------
    uri: string
        URI of the resource to be accessed. e.g. /REST/projects

    Returns
    -------
    List of dicts containing the results
    """
    return self._xnat.get_json(uri)
  
  def download_file(self, experiment_id, resource_id, file_id, target_file, format=None, verbose=True, timeout=None):
    filesRef = self._xnat.experiments[experiment_id].resources[resource_id].files
    fileData = filesRef[file_id]
    fileData.download(target_file, format=format, verbose=verbose, timeout=timeout)
    return fileData



  def put(self):
    self._xnat.put()
  
  def search(self, row, columns=[]):
    return Search(row, columns, self._xnat)

  @property
  def select(self):
    return self._xnat




