import os
import re
import ast
import glob
import hashlib
import pandas 

import sibispy
from sibispy import sibislogger as slog
from sibispy import config_file_parser as cfg_parser
from ast import literal_eval as make_tuple

class redcap_to_casesdir(object):
    def __init__(self):
        self.__import_forms = dict()
        self.__export_forms = dict()
        self.__export_rename = dict()
        # Make lookup dicts for mapping radio/dropdown codes to labels
        self.__code_to_label_dict = dict()
        self.__metadata_dict = dict()
        self.__forms_dir =  None 
        self.__sibis_defs = None

    def configure(self, sessionObj, redcap_metadata):
        # Make sure it was set up correctly 
        if not sessionObj.get_ordered_config_load() :
            slog.info('recap_to_cases_dir.configure',"ERROR: session has to be configured with ordered_config_load set to True")
            return False

        # reading in all forms and variables that should be exported to cases_dir 
        (cfgParser,err_msg) = sessionObj.get_config_sys_parser()
        if err_msg:
            slog.info('recap_to_cases_dir.configure',str(err_msg))
            return False

        self.__sibis_defs= cfgParser.get_category('redcap_to_casesdir')

        self.__forms_dir  = os.path.join(sessionObj.get_operations_dir(),'redcap_to_casesdir')
        if not os.path.exists(self.__forms_dir) :
            slog.info('redcap_to_casesdir.configure','ERROR: ' + str(self.__forms_dir)  + " does not exist!")
            return False

        exports_files = glob.glob(os.path.join(self.__forms_dir, '*.txt'))
 
        for f in exports_files:
            file = open(f, 'r')
            contents = [line.strip() for line in file.readlines()]
            file.close()

            export_name = re.sub('\.txt$', '', os.path.basename(f))
            import_form_name = re.sub('\n', '', contents[0])
            self.__import_forms[export_name] = import_form_name
            self.__export_forms[export_name] = [re.sub('\[.*\]', '', field) for field in contents[1:]] + ['%s_complete' % import_form_name]
            self.__export_rename[export_name] = dict()

            for field in contents[1:]:
                match = re.match('^(.+)\[(.+)\]$', field)
                if match:
                    self.__export_rename[export_name][match.group(1)] = match.group(2)

        return self.__organize_metadata__(redcap_metadata)

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

        if not self.__check_all_forms__():
            return False

        self.__make_code_label_dict__(redcap_metadata)
        return True

    # Filter confidential fields from all forms
    def __check_all_forms__(self):
        # Filter each form
        text_list = list()
        non_redcap_list = list()
        for export_name in self.__export_forms.keys():
            (form_text_list, form_non_redcap_list)  = self.__check_form__(export_name)
            if form_text_list :
                text_list += form_text_list
            if form_non_redcap_list:
                non_redcap_list += form_non_redcap_list

        if text_list: 
            slog.info('redcap_to_casesdir.__filter_all_forms__.' + hashlib.sha1(str(text_list)).hexdigest()[0:6], "ERROR: The txt file(s) in '" + str(self.__forms_dir) + "' list non-numeric redcap variable names!",
                      form_variable_list = str(text_list),
                      info = "Remove it from form file or modify definition in REDCap")

        if non_redcap_list : 
            slog.info('redcap_to_casesdir.__filter_all_forms__.' +  hashlib.sha1(str(text_list)).hexdigest()[0:6], "ERROR: The txt file(s) in '" + str(self.__forms_dir) + "' list variables that do not exist in redcap!",
                      form_variable_list = str(non_redcap_list),
                      info = "Remove it from form or modify definition REDCap")
            
        if non_redcap_list or text_list:
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
                 text_val_max, choices) = self.__metadata_dict[re.sub('___.*', '', field_name)]
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

    def get_export_form_names(self):
        return self.__export_forms.keys()

    def create_datadict(self, export_name, datadict_dir):
         export_form_entry_list = self.__export_forms[export_name]
         size_entry_list = len(export_form_entry_list)
         export_form_list = [export_name] * size_entry_list
         return self.__create_datadicts_general__(datadict_dir, export_name, export_form_list,export_form_entry_list)

    # defining entry_list only makes sense if export_forms_list only consists of one
    # entry !
    def create_all_datadicts(self, datadict_dir):
        for export_name in self.get_export_form_names():
            self.create_datadict(export_name,datadict_dir)
        self.create_demographic_datadict(datadict_dir)

    def __reading_datadict_definitions__(self,dictionary_type):
        dict_data = self.__sibis_defs[dictionary_type]
        export_entry_list = dict_data.keys(); 
        if not len(export_entry_list):
            slog.info('redcap_to_casesdir.__reading_datadict_definitions',"ERROR: Cannot find " + dictionary_type + " in config file!")
            return None

        meta_data_dict= dict()
        for variable in export_entry_list:
            # turn string into tuple 
            meta_data_dict[variable] = make_tuple("(" + dict_data[variable] +")")
        
        self.__metadata_dict.update(meta_data_dict)

        return export_entry_list

    # Create custom form for demographics 
    def create_demographic_datadict(self, datadict_dir):
        export_entry_list = self.__reading_datadict_definitions__('demographic_datadict')
        if not export_entry_list: 
            return False

        # First two entries are extracted from SubjectID
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


        if not self.__reading_datadict_definitions__('general_datadict'):
            return None

        if not os.path.exists(datadict_dir):
            os.makedirs(datadict_dir)

        ddict = pandas.DataFrame(index=variable_list,columns=redcap_datadict_columns)

        for form_name, var in zip(export_forms_list, variable_list):
            field_name = re.sub('___.*', '', var)
            ddict["Variable / Field Name"][var] = field_name
            ddict["Form Name"][var] = form_name

            # Check if var is in data dict ('FORM_complete' fields are NOT)
            if field_name in self.__metadata_dict.keys():
                ddict["Field Type"][var] = self.__metadata_dict[field_name][0]
                # need to transfer to utf-8 code otherwise can create problems when
                # writing dictionary to file it just is a text field so it should
                #  not matter
                ddict["Field Label"][var] = self.__metadata_dict[field_name][2].encode('utf-8')
                ddict["Text Validation Type OR Show Slider Number"][var] = self.__metadata_dict[field_name][1]
                ddict["Text Validation Min"][var] = self.__metadata_dict[field_name][3]
                ddict["Text Validation Max"][var] = self.__metadata_dict[field_name][4]
                # need to transfer to utf-8 code otherwise can create problems when
                # writing dictionary to file it just is a choice field so it
                # should not matter
                ddict["Choices, Calculations, OR Slider Labels"][var] = self.__metadata_dict[field_name][5].encode('utf-8')

        # Finally, write the data dictionary to a CSV file
        dicFileName = os.path.join(datadict_dir,datadict_base_file + '_datadict.csv')
        try:
            ddict.to_csv(dicFileName, index=False)
            return dicFileName
        except Exception, err_msg:
            slog.info('redcap_to_casesdir.__create_datadicts_general__',"ERROR: could not export dictionary" + dicFileName, 
                      err_msg = str(err_msg))
            return None
