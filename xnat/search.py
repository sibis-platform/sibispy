###############################################################################
## Lifted from pyxnat and ported to Py3
###############################################################################

# This software is OSI Certified Open Source Software. OSI Certified is a 
# certification mark of the Open Source Initiative.

# Copyright (c) 2010-2011, Yannick Schwartz All rights reserved.

# Redistribution and use in source and binary forms, with or without modification, 
# are permitted provided that the following conditions are met:

#     1. Redistributions of source code must retain the above copyright notice, 
#        this list of conditions and the following disclaimer.

#     2. Redistributions in binary form must reproduce the above copyright notice, 
#        this list of conditions and the following disclaimer in the documentation 
#        and/or other materials provided with the distribution.

#     3. Neither the name of Yannick Schwartz. nor the names of other pyxnat 
#        contributors may be used to endorse or promote products derived from this 
#        software without specific prior written permission.

# This software is provided by the copyright holders and contributors "as is" and
# any express or implied warranties, including, but not limited to, the implied
# warranties of merchantability and fitness for a particular purpose are disclaimed.
# In no event shall the copyright owner or contributors be liable for any direct,
# indirect, incidental, special, exemplary, or consequential damages (including, but
# not limited to, procurement of substitute goods or services; loss of use, data, or
# profits; or business interruption) however caused and on any theory of liability,
# whether in contract, strict liability, or tort (including negligence or otherwise)
# arising in any way out of the use of this software, even if advised of the
# possibility of such damage.




from builtins import next
from builtins import str
from builtins import zip
from past.builtins import basestring
from builtins import object
import csv
import difflib
from lxml import etree
from io import StringIO
from .errors import is_xnat_error, catch_error, DataError, ProgrammingError
from .jsonutil import JsonTable

search_nsmap = {'xdat':'http://nrg.wustl.edu/security',
                'xsi':'http://www.w3.org/2001/XMLSchema-instance'}

special_ops = {'*':'%', }


class Search(object):
  """ Define constraints to make a complex search on the database.

      This :class:`Search` is available at different places throughout
      the API:

          >>> interface.search(DATA_SELECTION).where(QUERY)

      Examples
      --------
          >>> query = [('xnat:subjectData/SUBJECT_ID', 'LIKE', '%'),
                        ('xnat:projectData/ID', '=', 'my_project'),
                        [('xnat:subjectData/AGE', '>', '14'),
                          'AND'
                        ],
                        'OR'
                      ]
  """
  def __init__(self, row, columns, interface):
      """ Configure the result table.

          Parameters
          ----------
          row: string
              The returned table will have one line for every matching
              occurence of this type.
              e.g. xnat:subjectData
              --> table with one line per matching subject
          columns: list
              The returned table will have all the given columns.
      """
      self._row = row
      self._columns = columns
      self._intf = interface

  def where(self, constraints=None, template=None, query=None):
      """ Triggers the search.

          Parameters
          ----------
          contraints: list
              A query is an unordered list that contains
                  - 1 or more constraints
                  - 0 or more sub-queries (lists as this one)
                  - 1 comparison method between the constraints
                      ('AND' or 'OR')
              A constraint is an ordered tuple that contains
                  - 1 valid searchable_type/searchable_field
                  - 1 operator among '=', '<', '>', '<=', '>=', 'LIKE'

          Returns
          -------
          results: JsonTable object
              An table-like object containing the results. It is
              basically a list of dictionaries that has additional
              helper methods.
      """

      if isinstance(constraints, str):
          constraints = rpn_contraints(constraints)
    # jimklo: Removing support as sibispy isn't using this feature and there
    #         really isn't enough documentation to warrant trying to reverse
    #         engineer and port that feature.
    #
    #   elif isinstance(template, (tuple)):
    #       tmp_bundle = self._intf.manage.search.get_template(
    #           template[0], True)

    #       tmp_bundle = tmp_bundle % template[1]
    #       constraints = query_from_xml(tmp_bundle)['constraints']
    #   elif isinstance(query, (str, unicode)):
    #       tmp_bundle = self._intf.manage.search.get(query, 'xml')
    #       constraints = query_from_xml(tmp_bundle)['constraints']
      elif isinstance(constraints, list):
          pass
      else:
          raise ProgrammingError('One of contraints, template and query'
                                  'parameters must be correctly set.')

      bundle = build_search_document(self._row, self._columns, constraints)

      response = self._intf.post('/data/search', data=bundle, format='csv')
      content = response.text

      if is_xnat_error(content):
          catch_error(content)

      results = csv.reader(StringIO(content), delimiter=',', quotechar='"')
      headers = next(results)

      headers_of_interest = []

      for column in self._columns:
          try:
              headers_of_interest.append(
                  difflib.get_close_matches(
                      column.split(self._row + '/')[0].lower() \
                          or column.split(self._row + '/')[1].lower(),
                      headers)[0]
                  )
          except IndexError:
              headers_of_interest.append('unknown')

      if len(self._columns) != len(headers_of_interest):
          raise DataError('unvalid response headers')

      return JsonTable([dict(list(zip(headers, res))) for res in results],
                        headers_of_interest).select(headers_of_interest)

  def all(self):
      return self.where([(self._row + '/ID', 'LIKE', '%'), 'AND'])

# -----------------------------------------------------------------------------

def rpn_contraints(rpn_exp):
    left = []
    right = []
    triple = []

    for i, t in enumerate(rpn_exp.split()):
        if t in ['AND', 'OR']:
            if 'AND' in right or 'OR' in right and left == []:
                try:
                    operator = right.pop(right.index('AND'))
                except:
                    operator = right.pop(right.index('OR'))

                left = [right[0]]
                left.append(right[1:] + [t])
                left.append(operator)

                right = []

            elif right != []:
                right.append(t)

                if left != []:
                    left.append(right)
                else:
                    left = right[:]
                    right = []

            elif right == [] and left != []:
                left = [left]
                left.append(t)
                right = left[:]
                left = []
            else:
                raise ProgrammingError('in expression %s' % rpn_exp)

        else:
            triple.append(t)
            if len(triple) == 3:
                right.append(tuple(triple))
                triple = []

    return left if left != [] else right

def query_from_xml(document):
    query = {}
    root = etree.fromstring(document)
    _nsmap = root.nsmap

    query['description'] = root.get('description', default="")

    query['row'] = root.xpath('xdat:root_element_name',
                              namespaces=root.nsmap)[0].text

    query['columns'] = []

    for node in root.xpath('xdat:search_field',
                           namespaces=_nsmap):

        en = node.xpath('xdat:element_name', namespaces=root.nsmap)[0].text
        fid = node.xpath('xdat:field_ID', namespaces=root.nsmap)[0].text

        query['columns'].append('%s/%s' % (en, fid))

    query['users'] = [
        node.text
        for node in root.xpath('xdat:allowed_user/xdat:login',
                               namespaces=root.nsmap
                               )
        ]

    try:
        search_where = root.xpath('xdat:search_where',
                                  namespaces=root.nsmap)[0]

        query['constraints'] = query_from_criteria_set(search_where)
    except:
        query['constraints'] = [('%s/ID' % query['row'], 'LIKE', '%'), 'AND']

    return query

def query_from_criteria_set(criteria_set):
    query = []
    query.append(criteria_set.get('method'))
    _nsmap = criteria_set.nsmap

    for criteria in criteria_set.xpath('xdat:criteria',
                                       namespaces=_nsmap):

        _f = criteria.xpath('xdat:schema_field', namespaces=_nsmap)[0]
        _o = criteria.xpath('xdat:comparison_type', namespaces=_nsmap)[0]
        _v = criteria.xpath('xdat:value', namespaces=_nsmap)[0]

        constraint = (_f.text, _o.text, _v.text)
        query.insert(0, constraint)

    for child_set in criteria_set.xpath('xdat:child_set',
                                        namespaces=_nsmap):

        query.insert(0, query_from_criteria_set(child_set))

    return query

def build_search_document(root_element_name, columns, criteria_set,
                          brief_description='', long_description='',
                          allowed_users=[]):
    root_node = \
        etree.Element(etree.QName(search_nsmap['xdat'], 'bundle'),
                      nsmap=search_nsmap
                      )

    root_node.set('ID', "@%s" % root_element_name)
    root_node.set('brief-description', brief_description)
    root_node.set('description', long_description)
    root_node.set('allow-diff-columns', "0")
    root_node.set('secure', "false")

    root_element_name_node = \
        etree.Element(etree.QName(search_nsmap['xdat'], 'root_element_name'),
                      nsmap=search_nsmap
                      )

    root_element_name_node.text = root_element_name

    root_node.append(root_element_name_node)

    for i, column in enumerate(columns):
        element_name, field_ID = column.split('/', 1)

        search_field_node = \
            etree.Element(etree.QName(search_nsmap['xdat'], 'search_field'),
                          nsmap=search_nsmap
                          )

        element_name_node = \
            etree.Element(etree.QName(search_nsmap['xdat'], 'element_name'),
                          nsmap=search_nsmap
                          )

        element_name_node.text = element_name

        field_ID_node = \
            etree.Element(etree.QName(search_nsmap['xdat'], 'field_ID'),
                          nsmap=search_nsmap
                          )

        field_ID_node.text = field_ID

        sequence_node = \
            etree.Element(etree.QName(search_nsmap['xdat'], 'sequence'),
                          nsmap=search_nsmap
                          )

        sequence_node.text = str(i)

        type_node = \
            etree.Element(etree.QName(search_nsmap['xdat'], 'type'),
                          nsmap=search_nsmap
                          )

        type_node.text = 'string'

        header_node = \
            etree.Element(etree.QName(search_nsmap['xdat'], 'header'),
                          nsmap=search_nsmap
                          )

        header_node.text = column

        search_field_node.extend([element_name_node,
                                  field_ID_node,
                                  sequence_node,
                                  type_node, header_node
                                  ])

        root_node.append(search_field_node)

    search_where_node = \
        etree.Element(etree.QName(search_nsmap['xdat'], 'search_where'),
                      nsmap=search_nsmap
                      )

    root_node.append(build_criteria_set(search_where_node, criteria_set))

    if allowed_users != []:

        allowed_users_node = \
            etree.Element(etree.QName(search_nsmap['xdat'], 'allowed_user'),
                          nsmap=search_nsmap
                          )

        for allowed_user in allowed_users:
            login_node = \
                etree.Element(etree.QName(search_nsmap['xdat'], 'login'),
                              nsmap=search_nsmap
                              )
            login_node.text = allowed_user
            allowed_users_node.append(login_node)

        root_node.append(allowed_users_node)

    return etree.tostring(root_node.getroottree())

def build_criteria_set(container_node, criteria_set):

    for criteria in criteria_set:
        if isinstance(criteria, basestring):
            container_node.set('method', criteria)

        if isinstance(criteria, (list)):
            sub_container_node = \
                etree.Element(etree.QName(search_nsmap['xdat'], 'child_set'),
                              nsmap=search_nsmap
                              )

            container_node.append(
                build_criteria_set(sub_container_node, criteria))

        if isinstance(criteria, (tuple)):
            if len(criteria) != 3:
                raise ProgrammingError('%s should be a 3-element tuple' %
                                        str(criteria)
                                        )

            constraint_node = \
                etree.Element(etree.QName(search_nsmap['xdat'], 'criteria'),
                              nsmap=search_nsmap
                              )

            constraint_node.set('override_value_formatting', '0')

            schema_field_node = \
                etree.Element(etree.QName(search_nsmap['xdat'],
                                          'schema_field'
                                          ),
                              nsmap=search_nsmap
                              )

            schema_field_node.text = criteria[0]

            comparison_type_node = \
                etree.Element(etree.QName(search_nsmap['xdat'],
                                          'comparison_type'
                                          ),
                              nsmap=search_nsmap
                              )

            comparison_type_node.text = special_ops.get(criteria[1],
                                                        criteria[1]
                                                        )

            value_node = \
                etree.Element(etree.QName(search_nsmap['xdat'], 'value'),
                              nsmap=search_nsmap
                              )

            value_node.text = criteria[2].replace('*', special_ops['*'])

            constraint_node.extend([
                    schema_field_node, comparison_type_node, value_node])

            container_node.append(constraint_node)

    return container_node