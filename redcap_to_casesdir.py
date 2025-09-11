from __future__ import print_function
from builtins import zip
from builtins import str
from builtins import range
from builtins import object
import os
import re
import ast
import glob
import hashlib
from pathlib import Path
from typing import Union

import pandas
import numpy as np
import datetime
import subprocess
from ast import literal_eval as make_tuple

import sibispy
from sibispy import sibislogger as slog
from sibispy import utils as sutils
from sibispy import config_file_parser as cfg_parser
from sibispy.cluster_util import SlurmScheduler


class redcap_to_casesdir(object):
    def __init__(self):
        self.__import_forms = dict()
        self.__export_forms = dict()
        self.__export_rename = dict()
        # Make lookup dicts for mapping radio/dropdown codes to labels
        self.__code_to_label_dict = dict()
        self.__metadata_dict = dict()
        self.__event_dict = dict()
        self.__demographic_event_skips = list()
        self.__forms_dir =  None
        self.__sibis_defs = None
        self.__scanner_dict = None


    def configure(self, sessionObj, redcap_metadata):
        # Make sure it was set up correctly
        if not sessionObj.get_ordered_config_load() :
            slog.info('recap_to_cases_dir.configure',"ERROR: session has to be configured with ordered_config_load set to True")
            return False

        # reading script specific settings
        (cfgParser,err_msg) = sessionObj.get_config_sys_parser()
        if err_msg:
            slog.info('recap_to_cases_dir.configure',str(err_msg))
            return False

        self.__sibis_defs = cfgParser.get_category('redcap_to_casesdir')

        self.__scanner_dict = self.__sibis_defs['scanner_dict']
        for TYPE in list(self.__scanner_dict.keys()) :
            self.__scanner_dict[TYPE] = self.__scanner_dict[TYPE].split(",")


        # Reading in events
        self.__event_dict = self.__transform_dict_string_into_tuple__('event_dictionary')
        if not  self.__event_dict:
            return False

        # Reading in which events to skip demographics generation for (i.e. midyears)
        self.__demographic_event_skips = self.__sibis_defs['skip_demographics_for']

        # reading in all forms and variables that should be exported to cases_dir
        self.__forms_dir  = os.path.join(sessionObj.get_operations_dir(),'redcap_to_casesdir')
        if not os.path.exists(self.__forms_dir) :
            slog.info('redcap_to_casesdir.configure','ERROR: ' + str(self.__forms_dir)  + " does not exist!")
            return False

        exports_files = glob.glob(os.path.join(self.__forms_dir, '*.txt'))

        for f in exports_files:
            file = open(f, 'r')
            contents = [line.strip() for line in file.readlines()]
            file.close()

            export_name = re.sub(r'\.txt$', '', os.path.basename(f))
            import_form = re.sub(r'\n', '', contents[0])
            self.__import_forms[export_name] = import_form
            self.__export_forms[export_name] = [re.sub(r'\[.*\]', '', field) for field in contents[1:]] + ['%s_complete' % import_form]
            self.__export_rename[export_name] = dict()

            for field in contents[1:]:
                match = re.match(r'^(.+)\[(.+)\]$', field)
                if match:
                    self.__export_rename[export_name][match.group(1)] = match.group(2)

        return self.__organize_metadata__(redcap_metadata)

    def __transform_dict_string_into_tuple__(self,dict_name):
        dict_str = self.__sibis_defs[dict_name]
        dict_keys = list(dict_str.keys())
        if not len(dict_keys):
            slog.info('redcap_to_casesdir.configure',"ERROR: Cannot find '" + dict_name + "'' in config file!")
            return None

        dict_tup = dict()
        for key in dict_keys:
            # turn string into tuple
            dict_tup[key] = make_tuple("(" + dict_str[key] +")")

        return dict_tup

    # Organize REDCap metadata (data dictionary)
    def __organize_metadata__(self,redcap_metadata):
        # turn metadata into easily digested dict
        for field in redcap_metadata:
            field_tuple = (field['field_type'],
                           field['text_validation_type_or_show_slider_number'],
                           field['field_label'],
                           field['text_validation_min'],
                           field['text_validation_max'],
                           field['select_choices_or_calculations'])

            self.__metadata_dict[field['field_name']] = field_tuple

        meta_data_dict = self.__transform_dict_string_into_tuple__('general_datadict')
        if not  meta_data_dict :
            return False

        self.__metadata_dict.update(meta_data_dict)

        if not self.__check_all_forms__():
            return False

        self.__make_code_label_dict__(redcap_metadata)
        return True

    # Filter confidential fields from all forms
    def __check_all_forms__(self):
        # Filter each form
        text_list = list()
        non_redcap_list = list()
        empty_line_list = list()
        
        for export_name in list(self.__export_forms.keys()):
            (form_text_list, form_non_redcap_list)  = self.__check_form__(export_name)
            if form_text_list :
                text_list += form_text_list
            if form_non_redcap_list:
                if form_non_redcap_list[0][1] == '' :
                    empty_line_list += [form_non_redcap_list[0][0]]
                else :
                    non_redcap_list += form_non_redcap_list

        if text_list:
            slog.info('redcap_to_casesdir.__check_all_forms__.' + hashlib.sha1(str(text_list).encode()).hexdigest()[0:6],
                      "ERROR: The txt file(s) in '" + str(self.__forms_dir) + "' list non-numeric redcap variable names!",
                      form_variable_list = str(text_list),
                      info = "Remove it from form file or modify definition in REDCap")

        if non_redcap_list :
            slog.info('redcap_to_casesdir.__check_all_forms__.' +  hashlib.sha1(str(text_list).encode()).hexdigest()[0:6],
                      "ERROR: The txt file(s) in '" + str(self.__forms_dir) + "' list variables that do not exist in redcap!",
                      form_variable_list = str(non_redcap_list),
                      info = "Remove it from form or modify definition REDCap")
            
        if empty_line_list :
            slog.info('redcap_to_casesdir.__check_all_forms__.' +  hashlib.sha1(str(text_list).encode()).hexdigest()[0:6],
                      "ERROR: The txt file(s) in '" + str(self.__forms_dir) + "' contain more than one empty line!",
                      empty_line_list = str(empty_line_list),
                      info = "Remove all empty lines form the text file. Make sure the last variable in the text file ends with a Carriage return!")

        if non_redcap_list or text_list or empty_line_list:
            return False

        return True

    # Filter potentially confidential fields out of given list, based on project
    #  metadata
    def __check_form__(self, export_name):
        text_list = list()
        non_redcap_list = list()

        for field_name in self.__export_forms[export_name]:
            try:
                (field_type, field_validation, field_label, text_val_min,
                 text_val_max, choices) = self.__metadata_dict[re.sub(r'___.*', '', field_name)]
                if (field_type != 'text' and field_type != 'notes') or (field_validation in ['number', 'integer', 'time']):
                    pass
                else:
                    text_list.append([export_name,field_name, field_type, field_validation])
            except:
                if '_complete' not in field_name:
                    non_redcap_list.append([export_name,field_name])

        return (text_list,non_redcap_list)

    def __make_code_label_dict__(self,redcap_metadata):
        # First turn metadata into easily digested dict
        for field in redcap_metadata:
            if field['field_type'] in ['radio', 'dropdown']:
                field_dict = {'': ''}
                choices = field['select_choices_or_calculations']
                for choice in choices.split('|'):
                    code_label = [c.strip() for c in choice.split(',')]
                    field_dict[code_label[0]] = ', '.join(code_label[1:])
                self.__code_to_label_dict[field['field_name']] = field_dict

    # used to be get_export_form_names
    def get_export_names_of_forms(self):
        return list(self.__export_forms.keys())

    def create_datadict(self, export_name, datadict_dir):
         if export_name not in self.__export_forms.keys() : 
             slog.info('redcap_to_casesdir.create_datadict',"ERROR: could not create data dictionary for form " + export_name)
             return None 

         export_form_entry_list = self.__export_forms[export_name]
         size_entry_list = len(export_form_entry_list)
         export_form_list = [export_name] * size_entry_list
         return self.__create_datadicts_general__(datadict_dir, export_name, export_form_list,export_form_entry_list)

    # defining entry_list only makes sense if export_forms_list only consists of one
    # entry !
    def create_all_datadicts(self, datadict_dir):
        for export_name in self.get_export_names_of_forms():
            self.create_datadict(export_name,datadict_dir)
        self.create_demographic_datadict(datadict_dir)

    # Create custom form for demographics
    def create_demographic_datadict(self, datadict_dir):
        meta_data_dict = self.__transform_dict_string_into_tuple__('demographic_datadict')
        if not meta_data_dict:
            return False
        self.__metadata_dict.update(meta_data_dict)

        dict_str = self.__sibis_defs['demographic_datadict']
        export_entry_list = list(dict_str.keys())
 
        export_form_list = ['demographics'] * len(export_entry_list)

        return self.__create_datadicts_general__(datadict_dir, 'demographics', export_form_list,export_entry_list)

    # for each entry in the form list you have to define a variable
    def __create_datadicts_general__(self,datadict_dir, datadict_base_file,export_forms_list, variable_list):
        redcap_datadict_columns = ["Variable / Field Name", "Form Name",
                                   "Section Header", "Field Type", "Field Label",
                                   "Choices, Calculations, OR Slider Labels",
                                   "Field Note",
                                   "Text Validation Type OR Show Slider Number",
                                   "Text Validation Min", "Text Validation Max",
                                   "Identifier?",
                                   "Branching Logic (Show field only if...)",
                                   "Required Field?", "Custom Alignment",
                                   "Question Number (surveys only)",
                                   "Matrix Group Name", "Matrix Ranking?"]

        # Insert standard set of data elements into each datadict.
        for i in range(3):
            elements = ['subject', 'arm', 'visit']
            export_forms_list.insert(i, export_forms_list[0])
            variable_list.insert(i, elements[i])

        if not os.path.exists(datadict_dir):
            os.makedirs(datadict_dir)

        ddict = pandas.DataFrame(index=variable_list,columns=redcap_datadict_columns)

        for name_of_form, var in zip(export_forms_list, variable_list):
            field_name = re.sub(r'___.*', '', var)
            ddict.loc[var, "Variable / Field Name"] = field_name
            ddict.loc[var, "Form Name"] = name_of_form

            # Check if var is in data dict ('FORM_complete' fields are NOT)
            if field_name in list(self.__metadata_dict.keys()):
                ddict.loc[var, "Field Type"] = self.__metadata_dict[field_name][0]
                # need to transfer to utf-8 code otherwise can create problems when
                # writing dictionary to file it just is a text field so it should not matter
                #  .encode('utf-8')
                # Not needed in Python 3 anymore 
                ddict.loc[var, "Field Label"] = self.__metadata_dict[field_name][2]
                ddict.loc[var, "Text Validation Type OR Show Slider Number"] = self.__metadata_dict[field_name][1]
                ddict.loc[var, "Text Validation Min"] = self.__metadata_dict[field_name][3]
                ddict.loc[var, "Text Validation Max"] = self.__metadata_dict[field_name][4]
                #.encode('utf-8')
                ddict.loc[var, "Choices, Calculations, OR Slider Labels"] = self.__metadata_dict[field_name][5]

        # Finally, write the data dictionary to a CSV file
        dicFileName = os.path.join(datadict_dir,datadict_base_file + '_datadict.csv')
        try:
            sutils.safe_dataframe_to_csv(ddict,dicFileName)
            return dicFileName
        except Exception as err_msg:
            slog.info('redcap_to_casesdir.__create_datadicts_general__',"ERROR: could not export dictionary" + dicFileName,
                      err_msg = str(err_msg))
            return None


    def __ndar_months__(self, dob_ymd: str, ref_ymd: str,
                        fmt: str = sutils.date_format_ymd):
        """
        Age in whole months, per NDAR spec:
          • DOB floored to 1st of its month.
          • +1 month if ref-date is ≥ 16 days into its month.
          • If either date missing → ''  (keeps blanks blank).
        """
        if pandas.isnull(dob_ymd) or pandas.isnull(ref_ymd):
            return ''

        dob = datetime.datetime.strptime(dob_ymd, fmt).replace(day=1)
        ref = datetime.datetime.strptime(ref_ymd, fmt)

        months = (ref.year - dob.year) * 12 + (ref.month - dob.month)
        if ref.day >= 16:
            months += 1
        return months
    
    def __age_to_months__(self, dob, ref_date, fallback_years):
        """
        • Try NDAR months (blank → '').
        • If NDAR returns '' OR 0, fall back to years×12, but
          blank out 0 as well.
        • Always return either '' or an *int*.
        """
        months = self.__ndar_months__(dob, ref_date)
        if isinstance(months, int) and months > 0:
            return months

        # fall back to years ⇒ months
        if pandas.notnull(fallback_years):
            m = int(round(float(fallback_years) * 12))
            return m if m > 0 else ''
        return ''
    
    def __visit_months__(self, row):
        """Compute NDAR months from dob + visit_date for this DataFrame row."""
        return self.__ndar_months__(row['dob'], row['visit_date'])

    # Truncate age to 2 digits for increased identity protection
    def __truncate_age__(self, age_in):
        matched = re.match(r'([0-9]*\.[0-9]*)', str(age_in))
        if matched:
            return round(float(matched.group(1)), 2)
        else:
            return age_in

    def __get_scanner_mfg_and_model__(self, mri_scanner, expid):
        if mri_scanner == 'nan' :
            return ["",""]

        mri_scanner= mri_scanner.upper()
        for TYPE in list(self.__scanner_dict.keys()) :
            if TYPE in mri_scanner :
                return self.__scanner_dict[TYPE]

        slog.info(expid, "Error: Do not know scanner type", script='redcap_to_casesdir.py', mri_scanner = mri_scanner)
        return ["",""]


    # NCANDA SPECIFIC - Generalize later
    # Create "demographics" file "by hand" - this includes some text fields
    def export_subject_demographics(
        self,
        subject,
        subject_code,
        arm_code,
        visit_code,
        site,
        visit_age,
        subject_data,
        visit_data,
        exceeds_criteria_baseline,
        siblings_enrolled_yn_corrected,
        siblings_id_first_corrected,
        measures_dir,
        conditional=False,
        verbose=False,
    ):
            target_path = os.path.join(measures_dir, 'demographics.csv')
            # this is done so that midyear visit does not ovewrite the main visit  demographic file 
            if conditional and os.path.exists(target_path):
                if verbose :
                    print("Skipping updating demographics based on midyear visit: " + target_path)
                return 0
            
            # Latino and race coding arrives here as floating point numbers; make
            # int strings from that (cannot use "int()" because it would fail for
            # missing data
            hispanic_code = re.sub(r'(.0)|(nan)', '', str(subject_data['hispanic']))
            race_code = re.sub(r'(.0)|(nan)', '', str(subject_data['race']))


            # scanner manufacturer map
            scanner_mfg, scanner_model = self.__get_scanner_mfg_and_model__(str(visit_data['mri_scanner']), subject + "-" + visit_code)

            # Definig enroll_exception_drinking_2
            if exceeds_criteria_baseline < 0 :
                exceeds_criteria_baseline=int(subject_data['enroll_exception___drinking'])

            if siblings_enrolled_yn_corrected < 0:
                siblings_enrolled_yn_corrected=subject_data['siblings_enrolled___true']

            # No sibling than by default it is the subject itself 
            if siblings_enrolled_yn_corrected == 0 :
                siblings_id_first_corrected = subject_code
            elif siblings_id_first_corrected == None :
                # if there is a sibling and if not a special case, then use default 
                siblings_id_first_corrected=subject_data['siblings_id1']
                # unless not defined either -> then it must be the first subject 
                if type(siblings_id_first_corrected) is not str or siblings_id_first_corrected == "" :
                    siblings_id_first_corrected = subject_code

            # if you add a line pe
            if race_code == '6':
                # if other race is specified, mark race label with manually curated
                # race code
                race_label=subject_data['race_other_code']
            else :
                race_label=self.__code_to_label_dict['race'][race_code]
                
            if pandas.isnull(subject_data['family_id']):
                family_id = ""
            else:
                family_id = str(int(subject_data['family_id']))

            if conditional:
                visit_age = ''
            else:
                visit_age = self.__truncate_age__(visit_age)
                
            # define ndar values
            if pandas.isnull(subject_data['ndar_guid_id']):
                if pandas.isnull(subject_data['ndar_guid_pseudo_id']):
                    ndar_guid_id = ""
                else:
                    ndar_guid_id = subject_data['ndar_guid_pseudo_id']
            else:
                ndar_guid_id = subject_data['ndar_guid_id']
            
            ndar_guid_anomaly = re.sub(r'\.0$|nan', '', str(subject_data['ndar_guid_anomaly']))
            ndar_guid_anomaly_visit = re.sub(r'\.0$|nan', '', str(subject_data['ndar_guid_anomaly_visit']))
            ndar_guid_aud_dx_followup = re.sub(r'\.0$|nan', '', str(subject_data['ndar_guid_aud_dx_followup']))
            ndar_guid_aud_dx_initial = re.sub(r'\.0$|nan', '', str(subject_data['ndar_guid_aud_dx_initial']))
            
            # convert visit date from YYYY-MM-DD to MM/YYYY
            if pandas.isnull(visit_data['visit_date']):
                visit_date = ""
            else:
                visit_date = datetime.datetime.strptime(visit_data['visit_date'], "%Y-%m-%d").strftime("%m/%Y")

            visit_months = self.__ndar_months__(subject_data['dob'], visit_data['visit_date'])
            
            demographics = [
                ['subject', subject_code],
                ['arm', arm_code],
                ['visit', visit_code],
                ['site', site],
                ['sex', subject[8]],
                ['visit_age', visit_months],
                ['mri_structural_age', visit_months],
                ['mri_diffusion_age', visit_months],
                ['mri_restingstate_age', visit_months],
                ['exceeds_bl_drinking',
                 'NY'[int(subject_data['enroll_exception___drinking'])]],
                ['exceeds_bl_drinking_2',exceeds_criteria_baseline],                
                ['siblings_enrolled_yn',
                 'NY'[int(subject_data['siblings_enrolled___true'])]],
                ['siblings_id_first', subject_data['siblings_id1']],
                ['siblings_enrolled_yn_2',
                 'NY'[int(siblings_enrolled_yn_corrected)]],
                # ['siblings_id_first_2', siblings_id_first_corrected],
                ['family_id', family_id],
                ['hispanic', self.__code_to_label_dict['hispanic'][hispanic_code][0:1]],
                ['race', race_code],
                ['race_label', race_label],
                ['participant_id', subject],
                ['scanner', scanner_mfg],
                ['scanner_model', scanner_model],
                ['visit_date', visit_date],
                ['ndar_guid_id', ndar_guid_id],
                ['ndar_guid_anomaly', ndar_guid_anomaly],
                ['ndar_guid_anomaly_visit', ndar_guid_anomaly_visit],
                ['ndar_guid_aud_dx_followup', ndar_guid_aud_dx_followup],
                ['ndar_guid_aud_dx_initial', ndar_guid_aud_dx_initial],
            ]

            series = pandas.Series()
            for (key, value) in demographics:
                series.at[key] = value

            # write/update export_measures_log
            self.update_export_log(measures_dir)

            return sutils.safe_dataframe_to_csv(pandas.DataFrame(series).T,
                                                    target_path,
                                                    verbose=verbose)

    def update_export_log(self, measures_dir):
        """
        Update the export_measures.log file with the current datetime whenever
        a demographics form is updated for a subject.
        """
        # create the export_meaures.log file if it doesn't exit
        measures_path = Path(measures_dir)
        log_path = measures_path / "export_measures.log"
        try:
            with open(log_path, 'w') as file:
                current_datetime = str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                file.write(f"{current_datetime}\n")
        except IOError:
            print(f"Warning: Unable to write log file {log_path}")

        return

    def export_subject_form(self, export_name, subject, subject_code, arm_code, visit_code, all_records, measures_dir,verbose = False):
        # ------------------------------------------------------------------
        # 1.  Build initial record
        # ------------------------------------------------------------------
        complete = f"{self.__import_forms[export_name]}_complete"
        fields   = [f for f in self.__export_forms[export_name] if f != complete]
        fields   = [f for f in fields if f not in ('subject', 'arm', 'visit')]
        record   = all_records[fields].reindex(fields, axis=1)

        if len(record) > 1:
            slog.info(f"{subject}-{visit_code}",
                    f"ERROR: multiple records for form '{export_name}'!")
            return None
        if record.empty:
            if verbose:
                slog.info(f"{subject}-{visit_code}",
                        f"Info: visit data had no '{export_name}' records.")
            return None

        # add index cols
        record.insert(0, 'subject', subject_code)
        record.insert(1, 'arm',     arm_code)
        record.insert(2, 'visit',   visit_code)
        record = record.copy()                # avoid fragmentation warning

        # ------------------------------------------------------------------
        # 2.  Age conversion + coded-label collectionzd
        # ------------------------------------------------------------------
        label_cols = {}
        age_cols   = [c for c in record.columns if c.endswith('_age')]
        record[age_cols] = record[age_cols].astype('object')

        # === compute ONE month count for this visit (uses all_records) ===
        idx    = all_records.index[0]
        months = self.__ndar_months__(
                    all_records.at[idx, 'dob'],
                    all_records.at[idx, 'visit_date'])

        for col in record.columns:

            # -------- age columns → same month value for all ------------
            if col.endswith('_age'):
                # is there a value in the incoming record?
                raw_value = record[col].iloc[0]
                if pandas.isnull(raw_value) or str(raw_value).strip() == '':
                    # keep it blank
                    record.iat[0, record.columns.get_loc(col)] = ''
                else:
                    record.iat[0, record.columns.get_loc(col)] = months or ''

            # -------- coded radio/dropdown → extra <col>_label ----------
            if (col in self.__code_to_label_dict and not col.endswith('_why')):
                code  = str(record[col].iloc[0])
                label = self.__code_to_label_dict[col].get(code, '')
                label_cols[f"{col}_label"] = label

        # attach any <col>_label columns we collected
        if label_cols:
            record = pandas.concat(
                [record, pandas.DataFrame(label_cols, index=record.index)],
                axis=1
            )

        # ------------------------------------------------------------------
        # 3.  Final type cast for *_age → Int64 (no decimals)
        # ------------------------------------------------------------------
        for col in record.columns:
            if col.endswith('_age'):
                record[col] = pandas.to_numeric(record[col],
                                                errors='coerce'
                                                ).astype('Int64')

        # ------------------------------------------------------------------
        # 4.  Rename columns *once* after everything is settled
        # ------------------------------------------------------------------
        rename_map = {
            old: self.__export_rename[export_name].get(old, old)
            for old in record.columns
        }
        record.rename(columns=rename_map, inplace=True)

        # ------------------------------------------------------------------
        # 5.  Export
        # ------------------------------------------------------------------
        out_path = os.path.join(measures_dir, f"{export_name}.csv")
        return sutils.safe_dataframe_to_csv(record, out_path, verbose=verbose)

    # First get data for all fields across all forms in this event - this
    # speeds up transfers over getting each form separately
    def get_subject_specific_form_data(self,subject,event,forms_this_event, redcap_project,select_exports=None):
        # define fields and forms to export
        all_fields = ['study_id']
        forbidden_export_fields = ['subject', 'visit', 'arm']
        export_list = []
        for export_name in list(self.__export_forms.keys()):
            if export_name in forbidden_export_fields:
                continue
            if (self.__import_forms[export_name] in forms_this_event):
                if (not select_exports or export_name in select_exports):
                    all_fields += [re.sub(r'___.*', '', field_name) for field_name in self.__export_forms[export_name]]
                    export_list.append(export_name)

        # Remove the fields we are forbidden to export from REDCap
        all_fields = np.setdiff1d(all_fields, forbidden_export_fields).tolist()
        
        # Needed for age recomputation
        always_needed = ['dob', 'visit_date']
        all_fields += always_needed

        # Get data
        # all_records = redcap_project.export_records(fields=all_fields,records=[subject], events=[event],format_type='df')
        all_records = sutils.try_redcap_export_records(redcap_project, fields=all_fields,records=[subject], events=[event],format_type='df')

        # Add in baseline date of birth so 
        # baseline_dob = redcap_project.export_records(
        #     fields=['dob'], records=[subject],
        #     events=['baseline_visit_arm_1'], format_type='df'
        # ).iloc[0]['dob']

        baseline_dob = sutils.try_redcap_export_records(redcap_project,
            fields=['dob'], records=[subject],
            events=['baseline_visit_arm_1'], format_type='df'
        ).iloc[0]['dob']

        all_records['dob'] = baseline_dob

        # return results
        return (all_records,export_list)

    # Export selected REDCap data to cases dir
    def export_subject_all_forms(self,redcap_project, site, subject, event, subject_data, visit_age, visit_data, arm_code, visit_code, subject_code, subject_datadir,forms_this_event, exceeds_criteria_baseline, siblings_enrolled_yn_corrected,siblings_id_first_corrected, select_exports=None, force_demo_flag=False, verbose=False):

        measures_dir = os.path.join(subject_datadir, 'measures')
        if not os.path.exists(measures_dir):
            os.makedirs(measures_dir)

        # Export demographics (if selected)
        if force_demo_flag :
            conditional = False
        else :
            conditional = event in self.__demographic_event_skips
            
        if not select_exports or 'demographics' in select_exports:
            self.export_subject_demographics(
                subject=subject,
                subject_code=subject_code,
                arm_code=arm_code,
                visit_code=visit_code,
                site=site,
                visit_age=visit_age,
                subject_data=subject_data,
                visit_data=visit_data,
                exceeds_criteria_baseline=exceeds_criteria_baseline,
                siblings_enrolled_yn_corrected=siblings_enrolled_yn_corrected,
                siblings_id_first_corrected=siblings_id_first_corrected,
                measures_dir=measures_dir,
                conditional=conditional,
                verbose=verbose)

        (all_records,export_list) = self.get_subject_specific_form_data(subject,event,forms_this_event, redcap_project, select_exports)
        # Now go form by form and export data
        for export_name in export_list:
            self.export_subject_form(export_name, subject, subject_code, arm_code, visit_code, all_records, measures_dir, verbose)



    # What Arm and Visit of the study is this event?
    def translate_subject_and_event( self, subject_code, event_label):
        if event_label in list(self.__event_dict.keys()):
            (arm_code,visit_code) = self.__event_dict[event_label]
        else:
            slog.info(str(subject_code),"ERROR: Cannot determine study Arm and Visit from event %s" % event_label )
            return (None,None,None)

        pipeline_workdir_rel = os.path.join( subject_code, arm_code, visit_code )
        return (arm_code,visit_code,pipeline_workdir_rel)

    def days_between_dates( self, date_from_str, date_to_str, date_format_ymd=sutils.date_format_ymd):
        return (datetime.datetime.strptime( date_to_str, date_format_ymd ) - datetime.datetime.strptime( date_from_str, date_format_ymd ) ).days

    def get_event_dictionary(self):
        return self.__event_dict

    def schedule_cluster_job(self, job_script: str, job_title: str, submit_log: Union[str, Path] = None,
                             job_log: str = '/dev/null', verbose: bool = False) -> bool:

        slurm_config = self.__sibis_defs['cluster_config']        
        slurm = SlurmScheduler(slurm_config)
        try:
            return slurm.schedule_job(job_script, job_title, submit_log, job_log, verbose)[0]
        
        except Exception as err_msg:
            sbatch_cmd=slurm_config['base_cmd'].split(' ')[-1]        
            slog.info(job_title + "-" +hashlib.sha1(str(job_script).encode('utf-8')).hexdigest()[0:6],"ERROR: Failed to schedule job via slurm !",
                      job_script = str(job_script),
                      err_msg = str(err_msg),
                      slurm_config=str(slurm_config),
                      info="Make sure '" + sbatch_cmd +"' on '"+ slurm_config['connection']['host'] +"' exists! Debug by running test/test_redcap_to_casesdir.py")
            return False

    def schedule_old_cluster_job(self,job_script, job_title,submit_log=None, job_log=None, verbose=False):
        qsub_cmd= '/opt/sge/bin/lx-amd64/qsub'
        if not os.path.exists(qsub_cmd):
            slog.info(job_title + "-" +hashlib.sha1(str(job_script).encode('utf-8')).hexdigest()[0:6],"ERROR: Failed to schedule job as '" + qsub_cmd + "' cannot be found!", job_script = str(job_script))
            return False

        sge_env = os.environ.copy()
        sge_env['SGE_ROOT'] = '/opt/sge'
        sge_param = self.__sibis_defs['old_cluster_parameters'].split(',')
        if job_log :
            sge_param += ['-o', job_log]
        else :
            sge_param += ['-o','/dev/null']

        qsub_args= [ qsub_cmd ] + sge_param + ['-N', '%s' % (job_title) ]
        #stderr=subprocess.STDOUT
        qsub_process = subprocess.Popen( qsub_args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr= subprocess.PIPE, env=sge_env)
        (stdoutdata, stderrdata) = qsub_process.communicate(str(job_script).encode('utf-8'))

        cmd_str='echo "%s" | %s\n' % (job_script," ".join(qsub_args))
        if stderrdata :
            slog.info(job_title + "-" + hashlib.sha1(str(stderrdata).encode('utf-8')).hexdigest()[0:6],"ERROR: Failed to schedule job !", cmd = cmd_str, err_msg = str(stderrdata))
            return False

        if verbose:
            print(cmd_str)
            if stdoutdata:
                print(stdoutdata.decode('utf-8'))

        if submit_log:
            with open(submit_log, "a") as myfile:
               myfile.write(cmd_str)
               myfile.write(stdoutdata.decode('utf-8'))

        return True
