##
##  Copyright 2016 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##
"""
Create the SIBIS Session Object
===============================
The SIBIS Session Object provides a single point of reference to access
multiple systems. For example, XNAT, REDDCap, and Github.
"""
import ast
import os
import time
import requests
from sibispy import sibislogger as slog
from sibispy import config_file_parser as cfg_parser

# --------------------------------------------
# this class was created to capture output from xnat 
# if one cannot connect to server
from cStringIO import StringIO
import sys

class Capturing(list):
    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self
    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        del self._stringio    # free up some memory
        sys.stdout = self._stdout

# --------------------------------------------
# CLASS DEFINITION

class Session(object):
    """
    SIBIS Session Object
    ====================
    Main object that provides logging, data connections, and
    issues management.

    config_file: yml file specifying configuration
                 Or set path as SIBIS_CONFIG environment variable
                 (default: see cfg_parser.default_config_file)
    connect: connects to all servers
             (default: None)

    assumes 
      from sibispy import sibislogger as slog
      slog.init_log() 
    is called before 

    """

    def __init__(self):
        self.__config_data = cfg_parser.config_file_parser()
        self.api = {'xnat': None, 'import_laptops' : None, 'import_webcnp' : None, 'data_entry': None, 'redcap_mysql_db' : None} 
        # redcap projects are import_laptops, import_webcnp, and data_entry
        self.__active_redcap_project__ = None
   
    def configure(self, config_file=None, ):
        """
        Configures the session object by first checking for an
        environment variable, then in the home directory.
        """

        err_msg = self.__config_data.configure(config_file)
        if err_msg:
            slog.info('session.configure',str(err_msg),
                      sibis_config_file=self.__config_data.get_config_file())
            return False

        return True

    def connect_server(self,api_type, timeFlag=False):
        """
        Connect to servers, setting each property.
        """
        if api_type not in self.api :
            slog.info('session.connect_server','api type ' + api_type + ' not defined !',
                      api_types = str(self.api.keys()))
            return None

        if timeFlag : 
            slog.startTimer2() 

        if api_type == 'xnat' :
            connectionPtr = self.__connect_xnat__()
        elif api_type == 'redcap_mysql_db' : 
            connectionPtr = self.__connect_redcap_mysql__()
        else :
            connectionPtr = self.__connect_redcap_project__(api_type)
            
        if timeFlag : 
            slog.takeTimer2('connect_' + api_type) 

        return connectionPtr

    def __connect_xnat__(self):
        import pyxnat
        cfg = self.__config_data.get_category('xnat')
        try : 
            xnat = pyxnat.Interface(server=cfg.get('server'),
                                    user=cfg.get('user'),
                                    password=cfg.get('password'),
                                    cachedir=cfg.get('cachedir'))
        except Exception as err_msg: 
            slog.info('session.__connect_xnat__', str(err_msg), server=cfg.get('server'))
            return None

        self.api['xnat'] = xnat
        return xnat

    def __connect_redcap_project__(self,api_type):
        import redcap
        cfg = self.__config_data.get_category('redcap')   
        if not cfg : 
            slog.info('session.__connect_redcap_project__','Error: config file does not contain section redcap',
                      config_file = self.__config_data.get_config_file())
                      
            return None 

        try:
            data_entry = redcap.Project(cfg.get('server'),
                                        cfg.get(api_type + '_token'),
                                        verify_ssl=cfg.get('verify_ssl'))
            self.api[api_type] = data_entry

        except KeyError, err:
            slog.info('session.__connect_redcap_project__',str(err),
                      server=cfg.get('server'),
                      api_type = api_type)
            return None 

        except requests.RequestException, err:
            slog.info('session.__connect_redcap_project__',str(err),
                      server=cfg.get('server'),
                      api_type = api_type)
            return None

        self.__active_redcap_project__ = api_type

        return data_entry

    def __connect_redcap_mysql__(self):
        from sqlalchemy import create_engine

        cfg = self.__config_data.get_category('redcap-mysql') 
        if not cfg:
            slog.info('session.__connect_redcap_mysql__','Error: config file does not contain section recap-mysql',
                      config_file = self.__config_data.get_config_file())
            return None

        user = cfg.get('user')
        passwd = cfg.get('passwd')
        db = cfg.get('db')
        hostname = cfg.get('hostname')
        connection_string = "mysql+pymysql://{0}:{1}@{2}/{3}".format(user,
                                                                 passwd,
                                                                 hostname,
                                                                 db)

        try:
            engine = create_engine(connection_string, pool_recycle=3600)
        except Exception, err_msg:
            slog.info('session.__connect_redcap_mysql__',str(err_msg),
                      database = db,
                      hostname = hostname)
            return None

        self.api['redcap_mysql_db'] = engine 
            
        return engine



    def get_email(self):
        return self.__config_data.get_value('email')

    def get_log_dir(self):
        return os.path.join(self.__config_data.get_value('analysis_dir'),'log')


    def get_operations_dir(self):
        return os.path.join(self.__config_data.get_value('analysis_dir'),"operations")

    def get_cases_dir(self):
        return os.path.join(self.__config_data.get_value('analysis_dir'),'cases')

    def get_summaries_dir(self):
        return os.path.join(self.__config_data.get_value('analysis_dir'),'summaries')

    def get_xnat_dir(self):
        return os.path.join(self.__config_data.get_value('import_dir'),'XNAT')


    def get_xnat_server_address(self):
        return self.__config_data.get_value('xnat','server')

    # if time_label is set then will take the time of the operation 
    def xnat_export_general(self,form, fields, conditions, time_label = None): 
        if not self.api['xnat'] : 
            slog.info('xnat_export_general','Error: XNAT api not defined')  
            return None

        if time_label:
            slog.startTimer2() 
        try:
            #  python if one cannot connect to server then 
            with Capturing() as xnat_output: 
                xnat_data = self.api['xnat'].select(form, fields).where(conditions).items()
        
        except Exception, err_msg:
            if xnat_output : 
                slog.info("session.xnat_export_general","ERROR: querying XNAT failed most likely due disconnect to server ({})".format(time.asctime()),
                          xnat_api_output = str(xnat_output),
                          form = str(form),
                          fields = str(fields),
                          conditions = str(conditions),
                          err_msg = str(err_msg))
            else :                
                slog.info("session.xnat_export_general","ERROR: querying XNAT failed at {}".format(time.asctime()),
                      form = str(form),
                      fields = str(fields),
                      conditions = str(conditions),
                      err_msg = str(err_msg))
            return None

        if time_label:
            slog.takeTimer2("xnat_export_" + time_label) 
        
        return xnat_data

    def __get_active_redcap_api__(self):
        project = self.__active_redcap_project__
        if not project :
            slog.info('__get_active_redcap_api__','Error: an active redcap project is currently not defined ! Most likely redcap api was not initialized correctly')  
            return None

        if not self.api[project]: 
            slog.info('__get_active_redcap_api__','Error: ' + str(project) + ' api not defined')  
            return None
            
        return self.api[project]

   
    # if time_label is set then will take the time of the operation 
    def redcap_export_records(self, time_label, **selectStmt):
        red_api = self.__get_active_redcap_api__()
        if not red_api: 
            return None

        if time_label:
            slog.startTimer2() 
        try:
            redcap_data = red_api.export_records(**selectStmt)
        except Exception, err_msg:
            slog.info("session.redcap_export_records","ERROR: exporting data from REDCap failed at {}".format(time.asctime()),
                      err_msg = str(err_msg),
                      **selectStmt)
            return None

        if time_label:
            slog.takeTimer2("redcap_export_" + time_label) 
        
        return redcap_data

    def redcap_import_record(self, error_label, subject_label, event, time_label, record):
        red_api = self.__get_active_redcap_api__()
        if not red_api: 
            return None

        if time_label:
            slog.startTimer2() 
        try:
            import_response = red_api.import_records([record], overwrite='overwrite')

        except requests.exceptions.RequestException as e:
            error = 'Failed to import into REDCap'
            err_list = ast.literal_eval(str(e))['error'].split('","')
            # probably needs more work here - but right now good enough
            if len(err_list) > 3 and "This field is located on a form that is locked. You must first unlock this form for this record." in err_list[3]:
                red_var = err_list[1]
                event = err_list[0].split('(')[1][:-1]
                red_value = self.redcap_export_records(False,fields=[red_var],records=[subject_label],events=[event])[0][red_var]
                if not record.has_key("mri_xnat_sid") or not record.has_key("mri_xnat_eids") :
                    slog.info(error_label, error,
                              redcap_value="'"+str(red_value)+"'",
                              redcap_variable=red_var,
                              redcap_event=event,
                              new_value="'"+str(err_list[2])+"'",
                              requestError=str(e))
                else :
                    slog.info(error_label, error,
                              redcap_value="'"+str(red_value)+"'",
                              redcap_variable=red_var,
                              redcap_event=event,
                              new_value="'"+str(err_list[2])+"'",
                              xnat_sid=record["mri_xnat_sid"],
                              xnat_eid=record["mri_xnat_eids"], 
                              requestError=str(e))

            elif not record.has_key("mri_xnat_sid") or not record.has_key("mri_xnat_eids") :
                slog.info(error_label, error,
                          requestError=str(e))
            else : 
                slog.info(error_label, error,
                          xnat_sid=record["mri_xnat_sid"], 
                          xnat_eid=record["mri_xnat_eids"],
                          requestError=str(e))
            return None

        if time_label:
            slog.takeTimer2("redcap_import_" + time_label, str(import_response)) 
        
        return import_response
