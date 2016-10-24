#!/usr/bin/env python

import redcap
import pandas
from redcap import RedcapError
import logger


#Override existing function in redcap.project to add sibis.logging when failure.

def import_records(rc_project, form_status, overwrite):
    #
    # rc_project: is the redcap.Project() object
    # form_status: array of dicts, csv/xml string, ``pandas.DataFrame``
    # overwrite : ('normal'), 'overwrite'
    #            ``'overwrite'`` will erase values previously stored in the
    #            database if not specified in the to_import dictionaries.
    #
    import_response = {}
    try:
        import_response = rc_project.import_records(form_status, overwrite=overwrite)
    except redcap.RedcapError as e:
        error = 'Failed to import data into Redcap Project'
        logger.logging('22_sri_import_records@sibis',
                  error,
                  data_to_import=form_status,
                  redcap_error=str(e))
    return import_response