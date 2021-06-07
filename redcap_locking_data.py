##
##  Copyright 2016 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##
"""
Create the SIBIS Locking Object
===============================
The SIBIS Locking Object provides functionality to lock, unlock, and report on the locking status of a visit 
"""
from ast import literal_eval
from builtins import str
from builtins import object
import pandas as pd
import sibispy
from sibispy import sibislogger as slog
import re
from typing import List


class redcap_locking_data(object):
    def __init__(self):
        self.__session__ = None
        self.__event_dict = None

    def configure(self, sessionObj):
        self.__session__ = sessionObj
        self.__event_dict = self.get_event_names_for_ids()

    def unlock_form(self,project_name, arm_name, event_descrip, name_of_form, subject_id = None):
        """
        Unlock a given form be removing records from table

        :param project_name: str
        :param arm_name: str
        :param event_descrip: str
        :param name_of_form: str
        :param subject_id: str
        :return: None
        """
        locked_forms = self.__session__.get_mysql_table_records('redcap_locking_data', project_name, arm_name, event_descrip, name_of_form=name_of_form, subject_id=subject_id)
        locked_list = ', '.join([str(i) for i in locked_forms.ld_id.values.tolist()])
        if locked_list:
            return self.__session__.delete_mysql_table_records('redcap_locking_data', locked_list)
 
        return 0

    def lock_form(self,project_name, arm_name, event_descrip, name_of_form, outfile = None, subject_id = None):
        """
        Lock all records for a given form for a project and event

        :param project_name: str
        :param arm: str
        :param event_descrip: str
        :param name_of_form: str
        :return:
        """
        
        # first make sure that all those forms are unlocked 
        self.unlock_form(project_name, arm_name, event_descrip, name_of_form, subject_id)
        # Then get records to lock 
        project_records = self.__session__.get_mysql_project_records(project_name, arm_name, event_descrip, subject_id = subject_id)
        # Lock them 
        # lock all the records for this form by appending entries to locking table
        # Kilian: Problem this table is created regardless if the form really exists in redcap or not
        return self.__session__.add_mysql_table_records('redcap_locking_data',project_name, arm_name, event_descrip, name_of_form, project_records, outfile) 

    def report_locked_forms_from_enriched_lock_table(
        self,
        subject_id: str,
        xnat_id: str,
        project_name: str,
        forms: List[str],
        redcap_event_name: str,
        enriched_table: pd.DataFrame,
    ) -> pd.DataFrame:
        # 1. Subset to project_id of interest (so that redcap_event_name is
        #    guaranteed unique)
        project_id = self.__session__.get_mysql_project_id(project_name)
        project_idx = enriched_table['project_id'] == project_id
        lock_table = enriched_table.loc[project_idx]

        # 2. Get export_arm and export_event via redcap_event_name
        event_idx = lock_table['redcap_event_name'] == redcap_event_name
        arm_name = lock_table.loc[event_idx, 'export_arm'].values[0]
        event_name = lock_table.loc[event_idx, 'export_event'].values[0]
        # event_id = lock_table.loc[event_idx, 'event_id'].values[0]
        index_cols = dict(subject=xnat_id, arm=arm_name, visit=event_name)

        # 3. Subset to just the subject and event of interest
        select_idx = ((lock_table['record'] == subject_id) 
                      & (lock_table['export_event'] == event_name))
        lock_table = lock_table.loc[select_idx]

        # 4. Write out to a single-row DataFrame
        columns = ['subject', 'arm', 'visit'] + list(forms)
        data = pd.DataFrame(data=index_cols, index=[0], columns=columns)
        for _, row in lock_table.iterrows():
            name_of_form = row.get('form_name')
            if name_of_form in forms:
                timestamp = row.get('timestamp')
                data.at[0, name_of_form] = timestamp

        return data

    def report_locked_forms(self,subject_id, xnat_id, forms, project_name, arm_name, event_descrip, my_sql_table=pd.DataFrame()):
        """
        Generate a report for a single subject reporting all of the forms that
        are locked in the database using the timestamp the record was locked

        This is called in export_redcap_to_pipeline.export()

        :param site_id: str (e.g., X-12345-G-6)
        :param xnat_id: str (e.g., NCANDA_S12345)
        :param forms: list
        :param project_name: str (e.g., ncanda_subject_visit_log)
        :param arm_name: str (e.g., Standard)
        :param event_descrip: str (e.g., Baseline)
        :return: `pandas.DataFrame`
        """

        columns = ['subject', 'arm', 'visit'] + list(forms)
        data = dict(subject=xnat_id, arm=arm_name.lower(), visit=event_descrip.lower())
        dataframe = pd.DataFrame(data=data, index=[0], columns=columns)

        if my_sql_table.empty:
            locked_forms = self.__session__.get_mysql_table_records('redcap_locking_data',project_name, arm_name, event_descrip, subject_id=subject_id)
        else: 
            locked_forms = self.__session__.get_mysql_table_records_from_dataframe(my_sql_table,project_name, arm_name, event_descrip, subject_id=subject_id)
           
        for _, row in locked_forms.iterrows():
            # form_name is not version dependent as it did not change in redcap table only api ! 
            name_of_form = row.get('form_name')
            if name_of_form in forms: 
                timestamp = row.get('timestamp')
                dataframe.at[0, name_of_form] = timestamp

        return dataframe

    def report_locked_forms_all(self,project_name):
        """
        Generate a report for a single subject reporting all of the forms that
        are locked in the database using the timestamp the record was locked

        :return: `pandas.DataFrame`
        """

        lock_data = self.__session__.get_mysql_table_records(
            'redcap_locking_data', project_name, arm_name=None,
            event_descrip=None, subject_id=None)
        enriched_lock_data = lock_data.merge(self.__event_dict, how='left')

        return enriched_lock_data

    def get_event_names_for_ids(self) -> pd.DataFrame:
        # 0. Check if this work has already been done (only needs doing once)
        if self.__event_dict is not None:
            return self.__event_dict

        # 1. Match event_id to constructed redcap_event_name
        db_conn = self.__session__.api['redcap_mysql_db']
        events = pd.read_sql_table('redcap_events_metadata', db_conn,
                                   columns=['event_id', 'arm_id', 'descrip'])
        arms = pd.read_sql_table('redcap_events_arms', db_conn,
                                 columns=['arm_id', 'arm_num'])
        event_arm = events.merge(arms, how='outer')
        event_arm['redcap_event_name'] = event_arm.apply(
            lambda x: "{}_arm_{}".format(
                re.sub('-', '', re.sub(r'[ ]', '_', x['descrip']).lower()),
                x['arm_num']
            ), axis=1
        )

        event_dict = event_arm[['event_id', 'redcap_event_name']]

        # 2. Match Redcap event name to the export event name
        config, error = self.__session__.get_config_sys_parser()
        lookup = (config.get_category('redcap_to_casesdir')
                        .get('event_dictionary', {}))

        records = []
        for rc_event, literal_tuple in lookup.items():
            arm, event = literal_eval("({})".format(literal_tuple))
            records.append({'redcap_event_name': rc_event,
                            'export_arm': arm,
                            'export_event': event})

        lookup_df = pd.DataFrame.from_records(records)
        event_dict_export = event_dict.merge(lookup_df, how='outer')

        # 3. Save into an object and return for good measure
        self.__event_dict = event_dict_export
        return event_dict_export
