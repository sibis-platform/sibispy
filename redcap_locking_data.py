##
##  Copyright 2016 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##
"""
Create the SIBIS Locking Object
===============================
The SIBIS Locking Object provides functionality to lock, unlock, and report on the locking status of a visit 
"""
from builtins import str
from builtins import object
import pandas as pd 
import sibispy 
from sibispy import sibislogger as slog

class redcap_locking_data(object):
    def __init__(self):
        self.__session__ = None

    def configure(self, sessionObj):
        self.__session__ = sessionObj

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

        return self.__session__.get_mysql_table_records('redcap_locking_data',project_name, arm_name=None, event_descrip=None, subject_id=None)

    
