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

from builtins import object
from .jsonutil import JsonTable
from .search import Search

class ArrayData(object):

    def __init__(self, interface):
        self._intf = interface

    def _get_array(self, query_cols, project_id=None,
                   subject_id=None, subject_label=None,
                   experiment_id=None, experiment_label=None,
                   experiment_type='xnat:imageSessionData',
                   columns=None, constraints=None
                   ):

        if constraints is None:
            constraints = {}

        queryParams = {
            'xsiType': experiment_type
        }
        uri = '%s/experiments' % self._intf.uri

        if project_id is not None:
            queryParams['project'] = project_id

        if subject_id is not None:
            queryParams['%s/subject_id' % experiment_type] = subject_id

        #Subject Label is only held in the xnat:subjectData so look for it there.
        #this should join against whatever the experiment type is. 
        if subject_label is not None:
            queryParams['xnat:subjectData/label'] = subject_label

        if experiment_id is not None:
            queryParams['ID'] = experiment_id

        if experiment_label is not None:
            queryParams['label'] = experiment_label

        if len(query_cols) > 0:
            queryParams['columns'] = ','.join(query_cols)
        else:
            queryParams['columns'] = ''

        if constraints != {}:
            queryParams['columns'] += ',' + ','.join(list(constraints.keys()))

        if columns is not None:
            queryParams['columns'] += ',' + ','.join(columns)

        c = {}
        [c.setdefault(key.lower(), value) for key, value in list(constraints.items())]


        full_results = self._intf.get_json(uri, query=queryParams)

        return JsonTable(full_results["ResultSet"]["Result"]).where(**c)

    def experiments(self, project_id=None, subject_id=None, subject_label=None,
              experiment_id=None, experiment_label=None,
              experiment_type='xnat:mrSessionData',
              columns=None,
              constraints=None
              ):

        """ Returns a list of all visible experiment IDs of the specified 
            type, filtered by optional constraints.

            Parameters
            ----------
            project_id: string
                Name pattern to filter by project ID.
            subject_id: string
                Name pattern to filter by subject ID.
            subject_label: string
                Name pattern to filter by subject ID.
            experiment_id: string
                Name pattern to filter by experiment ID.
            experiment_label: string
                Name pattern to filter by experiment ID.
            experiment_type: string
                xsi path type; e.g. 'xnat:mrSessionData'
            columns: list
                Values to return.
            constraints: dict
                Dictionary of xsi_type (key--) and parameter (--value)
                pairs by which to filter.
            """

        query_cols = ['ID','project','%s/subject_id' % experiment_type]

        return self._get_array(query_cols, project_id,
                               subject_id, subject_label,
                               experiment_id, experiment_label,
                               experiment_type, columns, constraints
                               )

    def scans(self, project_id=None, subject_id=None, subject_label=None,
              experiment_id=None, experiment_label=None,
              experiment_type='xnat:mrSessionData',
              scan_type='xnat:mrScanData',
              columns=None,
              constraints=None
              ):

        """ Returns a list of all visible scan IDs of the specified type,
            filtered by optional constraints.

            Parameters
            ----------
            project_id: string
                Name pattern to filter by project ID.
            subject_id: string
                Name pattern to filter by subject ID.
            subject_label: string
                Name pattern to filter by subject ID.
            experiment_id: string
                Name pattern to filter by experiment ID.
            experiment_label: string
                Name pattern to filter by experiment ID.
            experiment_type: string
                xsi path type; e.g. 'xnat:mrSessionData'
            scan_type: string
                xsi path type; e.g. 'xnat:mrScanData', etc.
            columns: list
                Values to return.
            constraints: dict
                Dictionary of xsi_type (key--) and parameter (--value)
                pairs by which to filter.
            """

        query_cols = ['ID','project','%s/subject_id' % experiment_type, '%s/ID' % scan_type]

        return self._get_array(query_cols, project_id,
                               subject_id, subject_label,
                               experiment_id, experiment_label,
                               experiment_type, columns, constraints
                               )

    def search_experiments(self,
                           project_id=None,
                           subject_id=None,
                           subject_label=None,
                           experiment_type='xnat:mrSessionData',
                           columns=None,
                           constraints=None
                           ):

        """ Returns a list of all visible experiment IDs of the 
            specified type, filtered by optional constraints. This
            function is a shortcut using the search engine.

            Parameters
            ----------
            project_id: string
                Name pattern to filter by project ID.
            subject_id: string
                Name pattern to filter by subject ID.
            subject_label: string
                Name pattern to filter by subject ID.
            experiment_type: string
                xsi path type must be a leaf session type. 
                defaults to 'xnat:mrSessionData'
            columns: List[string]
                list of xsi paths for names of columns to return.
            constraints: list[(tupple)]
                List of tupples for comparison in the form (key, comparison, value)
                valid comparisons are: =, <, <=,>,>=, LIKE
            """

        if columns is None:
            columns = []

        where_clause = []

        if project_id is not None:
            where_clause.append(('%s/project' % experiment_type, "=", project_id))
        if subject_id is not None:
            where_clause.append(('xnat:subjectData/ID', "=", subject_id))
        if subject_label is not None:
            where_clause.append(('xnat:subjectData/LABEL', "=", subject_label))

        if constraints is not None:
            where_clause.extend(constraints)

        if where_clause != []:
            where_clause.append('AND')

        if where_clause != []:
            table = Search(experiment_type, columns=columns, interface=self._intf).where(where_clause)
            return table
        else:
            table = Search(experiment_type, columns=columns, interface=self._intf)
            return table.all()

