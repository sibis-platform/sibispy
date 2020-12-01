#!/usr/bin/env python

##
##  See COPYING file distributed along with the ncanda-data-integration package
##  for the copyright and license terms
##

from __future__ import print_function
from builtins import str
from builtins import range
from builtins import object
import os
import re
import sys
import time
import hashlib
import urllib3
import pandas
import numpy as np
import redcap
import sibispy
from sibispy import sibislogger as slog

class redcap_compute_summary_scores(object):
    def __init__(self):
        self.__session = None
        self.__rc_summary = None
        self.__form_event_mapping = None
        self.__demographics = None

    def configure(self, session) :
        self.__session = session
        operationsDir = self.__session.get_operations_dir()
        if not operationsDir :
            slog.info("redcap_compute_summary_scores.configure", "ERROR: could not retrieve operations dir!") 
            return False


        scoring_script_dir = os.path.join(operationsDir,'redcap_summary_scoring')
        if not os.path.exists(scoring_script_dir):
            slog.info("redcap_compute_summary_scores.configure", "Warning: " + str(scoring_script_dir) + " does not exist - nothing to do !") 
            return False

        # add  scoring directory to search path used by python
        sys.path.append(operationsDir)

        global scoring 
        try:
            from redcap_summary_scoring import scoring
        except:
            import redcap_summary_scoring as scoring

        # Get system config for this project
        (cfgParser, err_msg) = self.__session.get_config_sys_parser()
        if err_msg:
            slog.info('recap_compute_summary_scores.configure',str(err_msg))
            return False
        # Get the API to connect to, defaulting to 'data_entry'
        target_api = (cfgParser
                      .get_category('redcap_compute_summary_scores')
                      .get('target_api', 'data_entry'))

        if target_api not in self.__session.api:
            self.__session.api.update({target_api: None})

        # If connection to redcap server fail, try multiple times
        try:
            self.__rc_summary =  self.__session.connect_server(target_api)
        except Exception as e:
            slog.info("redcap_compute_summary_scores.configure." + hashlib.sha1(str(e).encode()).hexdigest()[0:6],
                      "ERROR: Could not connect to redcap!:", err_msg = str(e))
            return False
 
        if not self.__rc_summary:
            return False
            
        self.__form_event_mapping = self.__rc_summary.export_fem(format='df')

        # Get record IDs and exclusions
        baseline_events = cfgParser.get_category('redcap_compute_summary_scores')['baseline_events'].split(",")
        demographics_fields =  cfgParser.get_category('redcap_compute_summary_scores')['demographics_fields'].split(",")
        self.__demographics = self.__rc_summary.export_records(fields=demographics_fields, event_name='unique', format='df')
        try:
            drop_fields = cfgParser.get_category('redcap_compute_summary_scores')['drop_fields'].split(",")
            self.__demographics = self.__demographics.drop(columns=drop_fields, errors='ignore')
        except:
            pass
        finally:
            self.__demographics = (self.__demographics
                                   .dropna(axis=1, how='all')
                                   .dropna(axis=0, how='all'))
        
        self.__demographics = pandas.concat([self.__demographics.xs(event, level=1) for event in baseline_events])

        return True

    def get_list_of_instruments(self):
        return scoring.instrument_list

    # Find all matching fields from a list of field names that match a list of given regular expression patterns
    def __get_matching_fields__(self,field_list, pattern_list):
        matches = set()
        for field_pattern in pattern_list:
            pattern_matches = [field for field in field_list if re.match(field_pattern, field)]
            if len(pattern_matches) > 0:
                matches.update(pattern_matches)
            else:
                # If no matches, assume this is a "complete" field and simply add the pattern itself
                matches.update([field_pattern])
        return matches

    def compute_summary_scores(self, instrument, subject_id=None, update_all=False, verbose=False, log=slog):
        scored_records = pandas.DataFrame()
        if instrument not in self.get_list_of_instruments(): 
            slog.info("compute_scored_records", "ERROR: instrument '" + instrument + "' does not exist!") 
            return (scored_records,True)
            
        # Get fields in the summary project for this instrument
        instrument_complete = '%s_complete' % instrument
        if subject_id:
            record_ids = self.__rc_summary.export_records(fields=[instrument_complete], records=[subject_id],event_name='unique', format='df')
        else : 
            record_ids = self.__rc_summary.export_records(fields=[instrument_complete],event_name='unique', format='df')

        # ensure record_ids are strings
        # import pdb; pdb.set_trace()
        ridx = record_ids.index
        if ridx.get_level_values(0).dtype != np.dtype(np.object):
            record_ids.index = ridx.set_levels([ridx.levels[0].astype('str')] + [ridx.levels[1]])

        # Get events for which this instrument is present, and drop all records from other events
        form_key = self.__session.get_redcap_form_key() 
        instrument_events_list = self.__form_event_mapping[self.__form_event_mapping[form_key] == scoring.output_form[instrument]]['unique_event_name'].tolist()

        record_ids = record_ids[record_ids.index.map(lambda x: x[1] in instrument_events_list)]
        if not len(record_ids):
            if verbose : 
                print("No records to score") 
            return (scored_records,False)

        # Unless instructed otherwise, drop all records that already exist
        if not update_all:
            try :  
                records_complete = record_ids[instrument_complete]
            except Exception as e:
                slog.info("compute_scored_records-" + hashlib.sha1(str(e).encode()).hexdigest()[0:6],
                          "ERROR: %s missing in instrument %s" % (instrument_complete,instrument),
                          err_msg = str(e))
                return (scored_records,True)

            record_ids = record_ids[records_complete.map(lambda x: True if str(x) == 'nan' else x < 1)]

        if not len(record_ids):
            return (scored_records,False)

        if verbose:
            print(len(record_ids), 'records to score')

        # Now get the imported records referenced by each record in the summary table
        import_fields = []
        for import_instrument in list(scoring.fields_list[instrument].keys()):
            import_fields += self.__get_matching_fields__(self.__rc_summary.field_names, scoring.fields_list[instrument][import_instrument])

        # Retrieve data from record in chunks of 50 records
        # We cannot always get everything in one request (too large), but don't want each record by itself either, for speed.
        # Have to do this separately for each event, because of the way REDCap separates study ID and event name in the request
        imported = []
        for event_name in set(record_ids.index.map(lambda key: key[1]).tolist()):
            records_this_event = record_ids.xs( event_name, level=1).index.tolist()
            for idx in range(0, len(records_this_event), 50):
                i = 0
                while i < 5:
                    try:
                        imported.append(
                            self.__rc_summary.export_records(
                                fields=import_fields,
                                records=records_this_event[idx:idx + 50],
                                events=[event_name], 
                                event_name='unique', 
                                format='df'
                            )
                        )
                    except urllib3.exceptions.MaxRetryError:
                        i += 1
                        time.sleep(12)
                        continue

                    break
        
        if False: 
            print("DEBUGGING:redcap_compute_summary_scores.py:Start ....")
            (scoresDF,errFlag)  = scoring.compute_scores(
                instrument, pandas.concat(imported), self.__demographics, log=slog)
            print(".... end") 
        else : 
            try: 
                (scoresDF, errFlag)  = scoring.compute_scores(
                    instrument, pandas.concat(imported), self.__demographics, log=slog)
            except slog.sibisExecutionError as err:
                err.add(subject_id = subject_id, update_all= update_all)
                err.slog_post()
                return (pandas.DataFrame(), False) 

            except Exception as e:
                error = "ERROR: scoring failed!"
                slog.info("compute_summary_scores-" + instrument + "-" + hashlib.sha1(str(e).encode()).hexdigest()[0:6],
                          error,
                          err_msg=str(e),
                          subject_id=subject_id,
                          update_all=update_all,
                          pyFile="redcap_summary_scoring/" + instrument + "/__init__.py",
                          err_obj=e )
                return (pandas.DataFrame(), False)

        return (scoresDF, errFlag)

    def upload_summary_scores_to_redcap(self, instrument, scored_records):
        return self.__session.redcap_import_record(instrument, None, None, None, scored_records)
