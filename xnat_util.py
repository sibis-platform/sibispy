
from __future__ import absolute_import, print_function

from builtins import map
from builtins import object
from .xnat.array import ArrayData
from .xnat.search import Search
from .xnat.jsonutil import JsonTable
from .xnat.uriutil import uri_parent, uri_grandparent
from .xnat.errors import catch_error, is_xnat_error

import difflib
import xnat
import py
import six
import os
import yaml

# jimklo: #stupidpythontricks monkey patch XNATBaseListing so you can access __get_item__ via __call__.
def __call_getitem(self, item=None):
  if item == None:
    return self.listing

  return self.__getitem__(item)
xnat.core.XNATBaseListing.__call__ = __call_getitem


class XNATResourceUtil(object):
  def __init__(self, resource):
    self._res = resource
    if not self._res : 
        raise XnatUtilRuntimeError("resource is NULL")
    self.xnat_session = resource.xnat_session
    self.files = resource.files

  def detailed_upload(self, data, remotepath, overwrite=False, extract=False, tags=None, content=None, format=None, query={}, **kwargs):
    uri = '{}/files/{}'.format(self._res.uri, remotepath.lstrip('/'))
    if extract:
      query['extract'] = 'true'

    if tags:
      query['tags'] = tags
    
    if content:
      query['content'] = content
    
    if format:
      query['format'] = format

    if isinstance(data, six.string_types) and '\0' not in data and os.path.isfile(data):  
      query['inbody'] = 'true'

    query['overwrite'] =  'true' if overwrite else 'false'

    if query['overwrite'] == 'true':
      response = self.xnat_session.delete(uri)
  
    self.xnat_session.upload(uri, data, query=query, method='post', **kwargs)
    self.files.clearcache()

class XNATExperimentUtil(object):
  def __init__(self, experiment):
    self.exp = experiment

  def trigger_pipelines(self):
    self.exp.xnat_session.put(self.exp.uri, query={'triggerPipelines': 'true'})

  # if resource does not exist - creates it 
  def resources_insure(self,resource_name):
    try : 
       return self.exp.resources[resource_name]
    except: 
       # if fails then create directory 
       resources_dir=self.exp.resources
       resources_dir.xnat_session.put(resources_dir.uri+ '/'+ resource_name)
       self.exp.resources.clearcache()
       return self.exp.resources[resource_name] 
    

  def summarize(self, path=None, field=None):
    raw = self.exp.fulldata
    
    def _find_key(field, field_list):
      keys = difflib.get_close_matches(field, field_list)
      if keys == []:
        keys = difflib.get_close_matches(field.upper(), field_list)

      if keys == []:
        raise KeyError("key '{}' not found in {}".format(field, field_list))
      return keys[0]

    if path == None:
      key = _find_key(field, list(raw['data_fields']))
      return [ raw['data_fields'][key] ]

    field_obj = None
    for item in raw['children']:
      if item['field'] == path:
        field_obj = item
        break

    res = list()
    if field_obj != None:
      for item in field_obj['items']:
        key = _find_key(field, list(item['data_fields']))
        res += [item['data_fields'][key]]
    
    return res
      

class XNATSessionElementUtil(object):
  def __init__(self, element):
    self.element = element
    self._xnat = element.xnat_session
  
  def get(self, path):
    """ Get an attribute value.

        .. note::
            The value is always returned in a Python string. It must
            be explicitly casted or transformed if needed.

        Parameters
        ----------
        path: string
            The xpath of the attribute relative to the element.

        Returns
        -------
        A string containing the value.
    """
    query = {
      'ID': self.element.id,
      'columns': path
    }

    get_uri = uri_parent(self.element.uri)
    resp = self.element.xnat_session.get_json(get_uri, query=query)
    jdata = JsonTable(resp['ResultSet']['Result']).where(ID=self.element.id)

    # unfortunately the return headers do not always have the
    # expected name

    header = difflib.get_close_matches(path.split('/')[-1],
                                      jdata.headers()
                                      )
    if header == []:
        header = difflib.get_close_matches(path, jdata.headers())[0]
    else:
        header = header[0]

    replaceSlashS = lambda x : x.replace(r'\s', r' ')
    if type(jdata.get(header)) == list:
        return list(map(replaceSlashS, jdata.get(header)))
    else:
        return jdata.get(header).replace(r'\s', r' ')

  def mget(self, paths):
      """ Set multiple attributes at once.

          It is more efficient to use this method instead of
          multiple times the `get()` method when getting more than
          one attribute because only a single HTTP call is issued to
          the server.

          Parameters
          ----------
          paths: list
              List of attributes' paths.

          Returns
          -------
          list: ordered list of values (in the order of the
          requested paths)
      """
      query = {
        'ID': self.element.id,
        'columns': 'ID,'+','.join(paths)
      }
      get_uri = uri_parent(self.element.uri)

      resp = self.element.xnat_session.get_json(get_uri, query=query)
      jdata = JsonTable(resp['ResultSet']['Result']).where(ID=self.element.id)

      results = []

      # unfortunately the return headers do not always have the
      # expected name

      for path in paths:
          header = difflib.get_close_matches(path.split('/')[-1],
                                              jdata.headers())

          if header == []:
              header = difflib.get_close_matches(path, jdata.headers())[0]
          else:
              header = header[0]
          results.append(jdata.get(header).replace(r'\s', r' '))

      return results
  
  def _raw(self, format="xml"):
    element_url = None
    if hasattr(self.element, 'uri'):
      element_url = self.element.uri
    elif hasattr(self.element, 'fulluri'):
      element_url = self.element.fulluri
    else:
      raise XnatUtilAttributeError("`element` is missing `uri` or `fulluri` property.")

    resp = self._xnat.get(element_url, format=format)
    if resp.status_code == 200:
      return resp
    else:
      raise XnatUtilRuntimeError("Error occurred when trying to GET uri: {} reason: {}".format(resp.url, resp.reason))
  
  @property
  def xml(self):
    return self._raw('xml').text



default_config = os.path.join(os.path.expanduser("~"), ".sibis-general-config.yml")

def get_xnat_util(config=default_config):
  util = None
  if config != None:
    with open(config, 'r') as cfg:
      cfg_obj = yaml.load_all(cfg)
      xnat_cfg = cfg_obj['xnat']
      util = XnatUtil(xnat_cfg['server'], xnat_cfg['user'], xnat_cfg['password'])
  return util
    

class XnatUtil(object):
  def __init__(self, server, user=None, password=None):
    self._server = server
    self._user = user
    self._password = password
    self._xnat = None
    self._debug = False
  
  def __del__(self):
    if self._xnat != None:
      try:
        self._xnat.disconnect()
      except:
        pass
      self._xnat = None

  def connect(self, verify=True, debug=False, loglevel=None):
    self._debug = debug
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
    full_result = self._xnat.get_json(uri)
    if 'ResultSet' in full_result and 'Result' in full_result['ResultSet']:
      return full_result['ResultSet']['Result']
    return None

  def _exec(self, uri, query={}, method='GET', body=None, headers=None, format=None, **kwargs):
        """ A wrapper around a simple httplib2.request call that:
                - avoids repeating the server url in the request
                - deals with custom caching mechanisms :: Depricated
                - manages a user session with cookies
                - catches and broadcast specific XNAT errors

            Parameters
            ----------
            uri: string
                URI of the resource to be accessed. e.g. /REST/projects
            method: GET | PUT | POST | DELETE | HEAD
                HTTP method.
            body: string | dict
                HTTP message body
            headers: dict
                Additional headers for the HTTP request.
            force_preemptive_auth: boolean
                .. note:: Depricated as of 1.0.0.0
                Indicates whether the request should include an Authorization header with basic auth credentials.
            **kwargs: dictionary
                Additional parameters to pass directly to the Requests HTTP call.

            HTTP:GET
            ----------
                When calling with GET as method, the body parameter can be a key:value dictionary containing
                request parameters or a string of parameters. They will be url encoded and appended to the url.

            HTTP:POST
            ----------
                When calling with POST as method, the body parameter can be a key:value dictionary containing
                request parameters they will be url encoded and appended to the url.

        """

        if headers is None:
            headers = {}

        if method is 'GET' and isinstance(body, dict):
          body.update(query)
          query = {}

        fulluri = self._xnat._format_uri(uri, format, query)

        if self._debug:
          print(fulluri)

        response = None

        if method is 'PUT':
            response = self._xnat.interface.put(fulluri, headers=headers, data=body, **kwargs)
        elif method is 'GET':
            response = self._xnat.interface.get(fulluri, headers=headers, params=body, **kwargs)
        elif method is 'POST':
            response = self._xnat.interface.post(fulluri, headers=headers, data=body, **kwargs)
        elif method is 'DELETE':
            response = self._xnat.interface.delete(fulluri, headers=headers, data=body, **kwargs)
        elif method is 'HEAD':
            response = self._xnat.interface.head(fulluri, headers=headers, data=body, **kwargs)
        else:
            print('unsupported HTTP method')
            return

        if (response is not None and not response.ok) or is_xnat_error(response.content):
            if self._debug:
                print(list(response.keys()))
                print(response.get("status"))

            catch_error(response.content, '''XnatUtil._exec failure:
    URI: {response.url}
    status code: {response.status_code}
    headers: {response.headers}
    content: {response.content}
'''.format(response=response))

        return response.content
  
  def download_file(self, experiment_id, resource_id, file_id, target_file, format=None, verbose=False, timeout=None):
    filesRef = self._xnat.experiments[experiment_id].resources[resource_id].files
    fileData = filesRef[file_id]
    target_dir = os.path.dirname(target_file)
    if not os.path.isdir(target_dir):
      os.makedirs(target_dir)
    fileData.download(target_file, format=format, verbose=verbose, timeout=timeout)
    return fileData

  def raw(self, element, format="xml"):
    element_url = None
    if hasattr(element, 'uri'):
      element_url = element.uri
    elif hasattr(element, 'fulluri'):
      element_url = element.fulluri
    else:
      raise XnatUtilAttributeError("`element` is missing `uri` or `fulluri` property.")

    resp = self._xnat.get(element_url, format=format)
    if resp.status_code == 200:
      return resp
    else:
      raise XnatUtilRuntimeError("Error occurred when trying to GET uri: {} reason: {}".format(resp.url, resp.reason))

  def raw_text(self, element, format="xml"):
    return self.raw(element, format).text

  def get_custom_variables(self, experiment, field_names, default_value=None ):
    '''Get one or more custom variables from xnatpy representation of experiment'''
    exp_fields = experiment.fields
    values = []
    for field_name in field_names:
        actual_field_name = field_name.lower()
        if actual_field_name in exp_fields:
            values.append( exp_fields[actual_field_name] )
        else:
            values.append( default_value )
    return values

  def get_attributes(self, element, attribute_list=[]):
    pass

  def put(self, *args, **kwargs):
    self._xnat.put(*args, **kwargs)


  def search(self, row, columns=[]):
    return Search(row, columns, self._xnat)

  @property
  def client(self):
    return self._xnat

  @property
  def select(self):
    return self._xnat

  @property
  def array(self):
    return ArrayData(self._xnat)

class XnatUtilAttributeError(AttributeError):
  pass

class XnatUtilRuntimeError(RuntimeError):
  pass

class XnatUtilFeatureNotImplementedYet(RuntimeError):
  pass
