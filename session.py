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
from __future__ import print_function, absolute_import
import ast
import os
import time
import datetime
import requests
import hashlib
import pandas as pd
from pandas.io.sql import execute
import warnings
from sibispy.svn_util import SibisSvnClient


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
        self.__config_usr_data = cfg_parser.config_file_parser()
        self.__config_srv_data = None 
        self.api = {'xnat': None, 'xnat_http': None, 'import_laptops' : None, 'import_webcnp' : None, 'data_entry': None, 'redcap_mysql_db' : None, 'browser_penncnp': None, 'svn_laptop': None} 
        # redcap projects are import_laptops, import_webcnp, and data_entry
        self.__active_redcap_project__ = None
        self.__ordered_config_load = False
   
    def configure(self, config_file=None, ordered_config_load_flag = False):
        """
        Configures the session object by first checking for an
        environment variable, then in the home directory.
        """
        self.__ordered_config_load = ordered_config_load_flag
        err_msg = self.__config_usr_data.configure(config_file,ordered_load = self.__ordered_config_load )
        if err_msg:
            slog.info('session.configure',str(err_msg),
                      sibis_config_file=config_file)
            return False

        (sys_file_parser,err_msg) = self.get_config_sys_parser()
        if err_msg :
            slog.info('session.configure',str(err_msg))
            return False
            
        self.__config_srv_data = sys_file_parser.get_category('session')

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
        elif api_type == 'xnat_http' :
            connectionPtr = self.__connect_xnat_http__()
        elif api_type == 'browser_penncnp' : 
            connectionPtr = self.__connect_penncnp__()
        elif api_type == 'svn_laptop' : 
            connectionPtr = self.__connect_svn_laptop__()
        elif api_type == 'redcap_mysql_db' : 
            connectionPtr = self.__connect_redcap_mysql__()
        else :
            connectionPtr = self.__connect_redcap_project__(api_type)
            
        if timeFlag : 
            slog.takeTimer2('connect_' + api_type) 

        return connectionPtr

    # Access xnat directly through http calls 
    def __connect_xnat_http__(self):
        import requests
        cfg = self.__config_usr_data.get_category('xnat')
        jsessionid = ''.join([self.get_xnat_data_address(), '/JSESSIONID'])

        http_session = requests.session()
        http_session.auth = (cfg['user'], cfg['password'])
        try : 
            http_session.get(jsessionid)

        except Exception as err_msg: 
            slog.info('session.__connect_xnat_http__', str(err_msg), server=cfg.get('server'))
            return None

        self.api['xnat_http'] = http_session
        return http_session


    # Access xnat through pyxnat 
    def __connect_xnat__(self):
        import pyxnat
        cfg = self.__config_usr_data.get_category('xnat')
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

    def __list_running_process__(self,cmd): 
        from subprocess import Popen, PIPE
        check_cmd  = "ps -ef | grep '" + cmd + "' | awk '{print $2}'"

        try : 
            p = Popen(check_cmd, shell = True, stdout = PIPE,  stderr = PIPE)
            return p.communicate()
            
        except Exception as err_msg:
            return (None,str(err_msg))
            
        
    def __connect_penncnp__(self):
        # Check that config file is correctly defined 
        if "penncnp" not in self.__config_srv_data.keys():
            slog.info("session.__connnect_penncnp__","ERROR: penncnp server info not defined!")
            return None

        penncnp_srv_data = self.__config_srv_data["penncnp"]

        if "penncnp" not in self.__config_usr_data.keys():
            slog.info("session.__connnect_penncnp__","ERROR: penncnp user info not defined!")
            return None

        penncnp_usr_data = self.__config_usr_data.get_category('penncnp')

        # Check if display is open     
        display = ":" + str(penncnp_srv_data["display"])
        vfb_cmd =  'vfb +extension RANDR ' + display
        check_cmd = "[X]" + vfb_cmd

        (pip_list, err_msg) = self.__list_running_process__(check_cmd)
        if err_msg : 
            slog.info("session.__connect_penncnp__","Checking if command %s is already running failed with the following error message: %s" % (check_cmd,strcheck_err))
            return None

        if pip_list: 
            slog.info("session.__connect_penncnp__","Error: sessions with display " + display + " are already running ! Please execute 'kill -9 " + str(pip_list) + "' before proceeding!")
            return None 

        # Open screen
        import subprocess
        display_cmd = "X" + vfb_cmd + " &> /dev/null & "
        try:
            err_msg = subprocess.check_output(display_cmd,shell=True)
        except Exception as err_msg:
            pass
            
        if err_msg: 
            slog.info("session.__connect_penncnp__","The following command failed %s with the following output %s" % (display_cmd,str(err_msg)))
            return None

        (pip, err_msg) = self.__list_running_process__(check_cmd)
        if err_msg : 
            slog.info("session.__connect_penncnp__","Checking if command %s is already running failed with the following error message: %s" % (check_cmd,strcheck_err))
            return None

        if not pip: 
            slog.info("session.__connect_penncnp__","Error: sessions with display " + display + " did not start up!")
            return None 

        os.environ["DISPLAY"]=display

        # Set up Browser
        # Configure Firefox profile for automated file download
        from selenium import webdriver
        
        fp = webdriver.FirefoxProfile()
        fp.set_preference("browser.download.folderList",2)
        fp.set_preference("browser.download.manager.showWhenStarting",False)
        fp.set_preference("browser.download.dir", os.getcwd())
        fp.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/vnd.ms-excel")
        browser = webdriver.Firefox( firefox_profile=fp, service_log_path="/tmp/geckodriver.log")       
 
        # Log into website
        browser.get(penncnp_srv_data["server"])
        browser.find_element_by_name("adminid").send_keys(penncnp_usr_data['user'] )
        browser.find_element_by_name("pwd").send_keys(penncnp_usr_data['password'] )
        browser.find_element_by_name("Login").click()

        # Exit configuration
        self.api['browser_penncnp'] = {"browser": browser, "pip" : int(pip), "display": display} 
        return browser


    def __connect_svn_laptop__(self):
        # Check that config file is correctly defined 
        if "svn_laptop" not in self.__config_usr_data.keys():
            slog.info("session.__connnect_svn_laptop__","ERROR: svn laptop user info not defined!")
            return None
        usr_data = self.__config_usr_data.get_category('svn_laptop')

        svnDir = self.get_laptop_svn_dir()
        client = SibisSvnClient(svnDir, username=usr_data['user'], password=usr_data['password'])
        self.api['svn_laptop'] = {"client": client, "user" : usr_data['user'], "password": usr_data['password']}         

        return client

    def __connect_redcap_project__(self,api_type):
        import redcap
        cfg = self.__config_usr_data.get_category('redcap')   
        if not cfg : 
            slog.info('session.__connect_redcap_project__','Error: config file does not contain section redcap',
                      config_file = self.__config_usr_data.get_config_file())
                      
            return None 

        try:
            data_entry = redcap.Project(cfg.get('server'),
                                        cfg.get(api_type + '_token'),
                                        verify_ssl=cfg.get('verify_ssl'))
            self.api[api_type] = data_entry

        except KeyError as err:
            slog.info('session.__connect_redcap_project__',str(err),
                      server=cfg.get('server'),
                      api_type = api_type)
            return None 

        except requests.RequestException as err:
            slog.info('session.__connect_redcap_project__',str(err),
                      server=cfg.get('server'),
                      api_type = api_type)
            return None

        self.__active_redcap_project__ = api_type

        return data_entry

    def __connect_redcap_mysql__(self):
        from sqlalchemy import create_engine

        cfg = self.__config_usr_data.get_category('redcap-mysql') 
        if not cfg:
            slog.info('session.__connect_redcap_mysql__','Error: config file does not contain section redcap-mysql',
                      config_file = self.__config_usr_data.get_config_file())
            return None

        # by default should be set to 3306 
        port = cfg.get('port')
        if not port: 
            slog.info('session.__connect_redcap_mysql__',"Error: config file does not define 'port' in section 'redcap-mysql'",
                      config_file = self.__config_usr_data.get_config_file())
            return None

        user = cfg.get('user')
        passwd = cfg.get('passwd')
        db = cfg.get('db')
        hostname = cfg.get('hostname')

        connection_string = "mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(user,
                                                                 passwd,
                                                                 hostname,
                                                                 port,
                                                                 db)

        try:
            engine = create_engine(connection_string, pool_recycle=3600)
        except Exception as err_msg:
            slog.info('session.__connect_redcap_mysql__',str(err_msg),
                      database = db,
                      hostname = hostname)
            return None

        self.api['redcap_mysql_db'] = engine 
            
        return engine

    def __get_analysis_dir(self) :
        analysis_dir = self.__config_usr_data.get_value('analysis_dir')
        if analysis_dir == None :
            slog.info("session.__get_analysis_dir-" + hashlib.sha1(str(self.__config_usr_data.get_config_file())).hexdigest()[0:6],"ERROR: 'analysis_dir' is not defined in config file !",
                      config_file = self.__config_usr_data.get_config_file())
            
        return  analysis_dir
            
            
    def get_ordered_config_load(self) : 
        return self.__ordered_config_load

    def get_project_name(self):
        return self.__config_usr_data.get_value('project_name')

    def get_email(self):
        return self.__config_usr_data.get_value('email')

    def get_beta_dir(self):
        aDir = self.__get_analysis_dir()
        if aDir :
            return os.path.join(aDir,'beta')
        return None

    def get_log_dir(self):
        aDir = self.__get_analysis_dir()
        if aDir :
            return os.path.join(aDir,'log')
        return None

    def get_operations_dir(self):
        aDir = self.__get_analysis_dir()
        if aDir :
            return os.path.join(aDir,'operations')
        return None

    def get_config_parser_for_file(self, key, filename):
        oDir = self.get_operations_dir()
        if not oDir :
            return (None,"ERROR: could not retrieve operations directory") 

        sys_file = os.path.join(oDir,filename)
        if not os.path.exists(sys_file) : 
            return (None,"ERROR:", sys_file," does not exist!") 

        # Get project specific settings for test file 
        sys_file_parser = cfg_parser.config_file_parser()
        err_msg = sys_file_parser.configure(sys_file,ordered_load=self.__ordered_config_load)
        if err_msg:
            return (None, "{0} ({1} : {2})".format(str(err_msg), key, str(sys_file)))

        return (sys_file_parser, None)

    def get_config_test_parser(self):
        return self.get_config_parser_for_file('config_test_file', 'sibis_test_config.yml')

    def get_config_sys_parser(self):
        return self.get_config_parser_for_file('config_sys_file', 'sibis_sys_config.yml')

    def get_cases_dir(self):
        aDir = self.__get_analysis_dir()
        if aDir :
            return os.path.join(aDir,'cases')
        return None

    def get_summaries_dir(self):
        aDir = self.__get_analysis_dir()
        if aDir :
            return os.path.join(aDir,'summaries')
        return None

    def get_dvd_dir(self):
        aDir = self.__get_analysis_dir()
        if aDir :
            return os.path.join(aDir,'burn2dvd')
        return None

    def get_datadict_dir(self):
        aDir = self.__get_analysis_dir()
        if aDir :
            return os.path.join(aDir,'datadict')
        return None


    def __get_laptop_dir(self):
        return os.path.join(self.__config_usr_data.get_value('import_dir'),'laptops')

    # Kilian: Ommit ncanda from svn dir name 
    def get_laptop_svn_dir(self):
        return os.path.join(self.__get_laptop_dir(),'ncanda')

    def get_laptop_imported_dir(self):
        return os.path.join(self.__get_laptop_dir(),'imported')

    def get_xnat_dir(self):
        return os.path.join(self.__config_usr_data.get_value('import_dir'),'XNAT')

    # Important for redcap front end - not sibis programs
    def get_redcap_uploads_dir(self):
        return os.path.join(self.__config_usr_data.get_value('import_dir'),'redcap')

    def get_xnat_server_address(self):
        return self.__config_usr_data.get_value('xnat','server')

    def get_xnat_data_address(self):
        return self.get_xnat_server_address()  + '/data'

    def xnat_http_get_all_experiments(self): 
        xnat_http_api = self.__get_xnat_http_api__()
        if not xnat_http_api: 
            return None

        server = self.get_xnat_data_address()
        collections = dict(projects=lambda: server + '/projects',
                       subjects=lambda x: server + '/{0}/subjects'.format(x),
                       experiments=lambda: server + '/experiments')

        return xnat_http_api.get(collections.get('experiments')() + '?format=csv')

    def xnat_http_get_experiment_xml(self,experiment_id): 
        xnat_http_api = self.__get_xnat_http_api__()
        if not xnat_http_api: 
            return None

        server = self.get_xnat_data_address()
        entities = dict(project=lambda x: server + '/projects/{0}'.format(x),
                    subject=lambda x: server + '/subjects/{0}'.format(x),
                    experiment=lambda x:
                    server + '/experiments/{0}'.format(x))

        return xnat_http_api.get(entities.get('experiment')(experiment_id) +  '?format=csv')

    # makes a difference where later saved file on disk how the function is called 
    def xnat_get_experiment(self,eid,project=None,subject_label=None): 
        xnat_api = self.__get_xnat_api__()
        if not xnat_api:
            error_msg = "XNAT API is not defined! Cannot retrieve experiment!",
            slog.info(eid,error_msg,
                      function = "session.xnat_get_experiment")
            return None
 
        # makes a difference where later saved file on disk how the function is called 
        if project and subject_label: 
            select_object =  self.xnat_get_subject(project,subject_label)
            if not select_object  :
                slog.info(subject_label,"ERROR: session.xnat_get_experiment: subject " + subject_label + " not found !",project = project)
                return None
        else :
            select_object =  xnat_api.select

        try : 
            xnat_experiment = select_object.experiment(eid)

        except Exception as err_msg:
            slog.info(eid + "-" + hashlib.sha1(str(err_msg)).hexdigest()[0:6],"ERROR: problem with xnat api !",
                      err_msg = str(err_msg),
                      function = "session.xnat_get_experiment")
            return None

        # not sure how this would ever happen
        if not xnat_experiment:
            slog.info(eid,"ERROR: session.xnat_get_experiment: experiment not created - problem with xnat api!")
            return None

        if not xnat_experiment.exists() :
            slog.info(eid,"ERROR: session.xnat_get_experiment: experiment does not exist !") 
            return None


        return xnat_experiment

    # replaces xnat_api.select.project(prj).subject( subject_label ).attrs.get(attribute)
    def xnat_get_subject(self,project,subject_label):
        xnat_api = self.__get_xnat_api__()
        if not xnat_api:
            error_msg = "XNAT API is not defined! Cannot retrieve subject !"
            slog.info(subject_label,error_msg,
                      function = "session.xnat_get_subject",
                      project = project)
            return None
 
        try : 
            xnat_project = xnat_api.select.project(project)

        except Exception as err_msg:
            slog.info(subject_label + "-" + hashlib.sha1(str(err_msg)).hexdigest()[0:6],"ERROR: project could not be found!",
                      err_msg = str(err_msg),
                      function = "session.xnat_get_subject",
                      project = project)
            return None
            
        if not xnat_project:
            slog.info(subject_label,"ERROR: session.xnat_get_subject: project " + project + " not found !")
            return None

        try : 
            xnat_subject = xnat_project.subject( subject_label )

        except Exception as err_msg:
            slog.info(subject_label + "-" + hashlib.sha1(str(err_msg)).hexdigest()[0:6],"ERROR: subject could not be found!",
                      err_msg = str(err_msg),
                      project = project,
                      function = "session.xnat_get_subject",
                      subject = subject_label)
            return None

        return xnat_subject


    def xnat_get_subject_attribute(self,project,subject_label,attribute):
        xnat_subject =  self.xnat_get_subject(project,subject_label)
        if not xnat_subject: 
            issue_url = slog.info(subject_label,"ERROR: session.xnat_get_subject_attribute: subject " + subject_label + " not found !",project = project)
            return [None,issue_url]

        try : 
            if attribute == "label" :
                return [xnat_subject.label(),None]
            else :
                return [xnat_subject.attrs.get(attribute),None]

        except Exception as err_msg:
            issue_url = slog.info("session.xnat_get_subject_attribute" + hashlib.sha1(str(err_msg)).hexdigest()[0:6],"ERROR: attribute could not be found!",
                      err_msg = str(err_msg),
                      project = project,
                      subject = subject_label,
                      function = "session.xnat_get_subject_attribute",
                      info = "Did subject change site ? Make sure that site of subject ID matches project !",  
                      attribute = attribute)
            
            return [None,issue_url]

    # if time_label is set then will take the time of the operation 
    def xnat_export_general(self,form, fields, conditions, time_label = None): 
        xnat_api = self.__get_xnat_api__()
        if not xnat_api: 
            return None

        if time_label:
            slog.startTimer2() 
        try:
            #  python if one cannot connect to server then 
            with Capturing() as xnat_output: 
                xnat_data = xnat_api.select(form, fields).where(conditions).items()
        
        except Exception as err_msg:
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

    def __get_xnat_api__(self): 
        if not self.api['xnat'] : 
            slog.info('__get_xnat_api__','Error: XNAT api not defined')  
            return None

        return self.api['xnat']


    def __get_xnat_http_api__(self): 
        if not self.api['xnat_http'] : 
            slog.info('__get_xnat_http_api__','Error: XNAT_HTTP api not defined')  
            return None

        return self.api['xnat_http']


    def initialize_penncnp_wait(self) : 
        from selenium.webdriver.support.ui import WebDriverWait
        return WebDriverWait(self.api['browser_penncnp']["browser"],self.__config_srv_data["penncnp"]["wait"])
        
    def get_penncnp_export_report(self,wait) :
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.common.by import By
        try: 
            report = wait.until(EC.element_to_be_clickable((By.NAME,'Export Report')))
        except Exception as e:
                slog.info('session.get_penncnp_export', "ERROR: Timeout, could not find Export Report",
                  info = "Try increasing wait time at WebDriverWait", 
                  msg=str(e))
                return None

        return report

    def disconnect_penncnp(self):
        # Note, if python script is manually killed before reaching this function then the subprocesses (e.g. X display) are also automatically killed  
        if not self.api['browser_penncnp']:
            return 
  
        self.api['browser_penncnp']['browser'].quit()

        if "DISPLAY" in os.environ.keys() and  os.environ['DISPLAY'] == self.api['browser_penncnp']['display'] :
            del os.environ['DISPLAY']
 
        import subprocess
        kill_cmd = "kill -9 " + str(self.api['browser_penncnp']['pip']) 
        try:
            err_msg = subprocess.check_output(kill_cmd,shell=True)
        except Exception as err_msg:
            pass
            
        
        if err_msg: 
            slog.info("session.__connect_penncnp__","The following command failed %s with the following output %s" % (kill_cmd,str(err_msg)))
            return None

    def get_redcap_server_address(self):
        return self.__config_usr_data.get_value('redcap','server')


    #
    # REDCAP API CALLS
    #
    def __get_active_redcap_api__(self):
        project = self.__active_redcap_project__
        if not project :
            slog.info('__get_active_redcap_api__','Error: an active redcap project is currently not defined ! Most likely redcap api was not initialized correctly')  
            return None

        if not self.api[project]: 
            slog.info('__get_active_redcap_api__','Error: ' + str(project) + ' api not defined')  
            return None
            
        return self.api[project]

   
    def get_redcap_version(self):
        api = self.__get_active_redcap_api__()
        if not api :
            return None
        return api.redcap_version

    def get_redcap_form_key(self):
        rc_version =  self.get_redcap_version()
        if not rc_version : 
            return '' 

        if rc_version.major < 8 : 
            return 'form_name'  

        return 'form'


    # if time_label is set then will take the time of the operation
    def redcap_export_records(self, time_label, **selectStmt):
        return self.redcap_export_records_from_api(time_label, None, **selectStmt)

    def redcap_export_records_from_api(self, time_label, api_type, **selectStmt):
        if api_type == None :
            red_api =  self.__get_active_redcap_api__()
        else :
            if api_type in self.api :
                red_api = self.api[api_type]
            else :
                return None
            
        if not red_api: 
            return None

        if time_label:
            slog.startTimer2() 
        try:
            with warnings.catch_warnings(record=True) as w:
                redcap_data = red_api.export_records(**selectStmt)
            if len(w):
                w_str = str(w[-1])
                if "Specify dtype option on import or set low_memory=False"  not in w_str : 
                    slog.info("session.redcap_export_records","Waring: exporting data from REDCap caused warning at {}".format(time.asctime()),
                              warning_msg = w_msg,
                              **selectStmt)

        except Exception as err_msg:
            slog.info("session.redcap_export_records","ERROR: exporting data from REDCap failed at {}".format(time.asctime()),
                      err_msg = str(err_msg),
                      **selectStmt)
            return None

        if time_label:
            slog.takeTimer2("redcap_export_" + time_label) 
        
        return redcap_data


    def redcap_import_record_to_api(self, records, api_type, error_label, time_label = None): 
        if len(records) == 0 : 
            return None

        if api_type == None :
            api_type = self.__active_redcap_project__

        if api_type in self.api :
            red_api = self.api[api_type]
        else :
            return None
            
        if not red_api: 
            return None

        if time_label:
            slog.startTimer2() 
        try:
            import_response = red_api.import_records(records, overwrite='overwrite')

        except requests.exceptions.RequestException as e:
            error = 'session:redcap_import_record:Failed to import into REDCap' 
            err_list = ast.literal_eval(str(e))['error'].split('","')
            error_label  += '-' + hashlib.sha1(str(e)).hexdigest()[0:6] 

            slog.info(error_label, error,
                      requestError=str(e), 
                      red_api = api_type)
            return None

        if time_label:
            slog.takeTimer2("redcap_import_" + time_label, str(import_response)) 
        
        return import_response

    def redcap_import_record(self, error_label, subject_label, event, time_label, records, record_id=None):
        if len(records) == 0 : 
            return None

        red_api = self.__get_active_redcap_api__()
        if not red_api: 
            return None

        if time_label:
            slog.startTimer2() 
        try:
            import_response = red_api.import_records(records, overwrite='overwrite')

        except requests.exceptions.RequestException as e:
            error = 'session:redcap_import_record:Failed to import into REDCap' 
            err_list = ast.literal_eval(str(e))['error'].split('","')
            error_label  += '-' + hashlib.sha1(str(e)).hexdigest()[0:6] 

            if len(records) > 1 :
                slog.info(error_label, error,
                          requestError=str(e), 
                          red_api = self.__active_redcap_project__)

            else :
                if isinstance(records,list) :
                    record = records[0]
                elif isinstance(records, pd.DataFrame): 
                    record_ser = records.iloc[0]
                    subject_label = record_ser.name[0]
                    error_label = "_".join(record_ser.name) + "_" + error_label
                    record = record_ser.to_dict()
                else :  
                    slog.info(error_label, "ERROR: session:redcap_import_record: type is not yet implemented",
                              import_record_id=str(record_id),  
                              requestError=str(e),
                              red_api = self.__active_redcap_project__)
                    
                if len(err_list) > 3 and "This field is located on a form that is locked. You must first unlock this form for this record." in err_list[3]:
                    red_var = err_list[1]
                    try:
                        event = err_list[0].split('(')[1][:-1]
                    except IndexError:  # Try to obtain event from record if unextractable from error
                        event = record.get('redcap_event_name')
                    if subject_label: 
                        red_value_temp = self.redcap_export_records(False,fields=[red_var],records=[subject_label],events=[event])
                        if red_value_temp : 
                            red_value = red_value_temp[0][red_var]
                            if "mri_xnat_sid" not in record or "mri_xnat_eids" not in record :
                                slog.info(error_label, error,
                                  redcap_value="'"+str(red_value)+"'",
                                  redcap_variable=red_var,
                                  redcap_event=event,
                                  new_value="'"+str(err_list[2])+"'",
                                  import_record_id=str(record_id), 
                                  requestError=str(e),
                                  red_api = self.__active_redcap_project__)
                            else :
                                slog.info(error_label, error,
                                  redcap_value="'"+str(red_value)+"'",
                                  redcap_variable=red_var,
                                  redcap_event=event,
                                  new_value="'"+str(err_list[2])+"'",
                                  xnat_sid=record["mri_xnat_sid"],
                                  xnat_eid=record["mri_xnat_eids"],
                                  requestError=str(e),
                                  red_api = self.__active_redcap_project__)
                        else : 
                            slog.info(error_label, error,
                                  redcap_variable=red_var,
                                  redcap_event=event,
                                  new_value="'"+str(err_list[2])+"'",
                                  import_record_id=str(record_id), 
                                  requestError=str(e),
                                  red_api = self.__active_redcap_project__)
                    else : 
                        slog.info(error_label, error,
                                  redcap_variable=red_var,
                                  redcap_event=event,
                                  new_value="'"+str(err_list[2])+"'",
                                  import_record_id=str(record_id), 
                                  requestError=str(e),
                                  red_api = self.__active_redcap_project__)

                elif "mri_xnat_sid" not in record or "mri_xnat_eids" not in record :
                    slog.info(error_label, error,
                              import_record_id=str(record_id),  
                              requestError=str(e),
                              red_api = self.__active_redcap_project__)
                else : 
                    slog.info(error_label, error,
                              xnat_sid=record["mri_xnat_sid"], 
                              xnat_eid=record["mri_xnat_eids"],
                              requestError=str(e),
                              red_api = self.__active_redcap_project__)
            return None

        if time_label:
            slog.takeTimer2("redcap_import_" + time_label, str(import_response)) 
        
        return import_response


    def get_mysql_project_id(self, project_name):
        """
        Get the project ID from a project_name
        
        :param project_name: str
        :param engine: sqlalchemy.Engine
        :return: int
        """
        try : 
            projects = pd.read_sql_table('redcap_projects', self.api['redcap_mysql_db'])
        except Exception as err_msg:
            slog.info("session.get_mysql_project_id." + hashlib.sha1(str(err_msg)).hexdigest()[0:6], "ERROR: could not read sql table redcap_projects!", project_name = project_name, err_msg =str(err_msg) )
            return None
            
        project_id = projects[projects.project_name == project_name].project_id
        return int(project_id)


    def get_mysql_arm_id(self,arm_name, project_id):
        """
        Get an arm_id using the arm name and project_id

        :param arm_name: str
        :param project_id: int
        :return: int
        """
        arms = pd.read_sql_table('redcap_events_arms', self.api['redcap_mysql_db'])
        arm_id = arms[(arms.arm_name == arm_name) & (arms.project_id == project_id)].arm_id
        return int(arm_id)


    def get_mysql_event_id(self, event_descrip, arm_id):
        """
        Get an event_id using the event description and arm_id
        
        :param event_descrip: str
        :param arm_id: int
        :return: int
        """
        events = pd.read_sql_table('redcap_events_metadata', self.api['redcap_mysql_db'])
        event_id = events[(events.descrip == event_descrip) & (events.arm_id == arm_id)].event_id
        return int(event_id)

    # 'redcap_locking_data'
    def get_mysql_table_records(self,table_name,project_name, arm_name, event_descrip, name_of_form=None, subject_id=None):
        """
        Get a dataframe of forms for a specific event

        :param project_name: str
        :param arm_name: str
        :param event_descrip: str
        :return: pandas.DataFrame`
        """

        project_id = self.get_mysql_project_id(project_name)
        if not project_id : 
            return pd.DataFrame()  

        arm_id = self.get_mysql_arm_id(arm_name, project_id)
        event_id = self.get_mysql_event_id(event_descrip, arm_id)
        table_records = pd.read_sql_table(table_name, self.api['redcap_mysql_db'])
        table_forms = table_records[(table_records.project_id == project_id) & (table_records.event_id == event_id)]
        if name_of_form :
            table_forms = table_forms[table_forms.form_name == name_of_form]

        if subject_id:
            table_forms = table_forms[table_forms.record == subject_id]

        return table_forms


    def get_mysql_project_records(self, project_name, arm_name, event_descrip, subject_id = None ):
        """
        Get a dataframe of records for a specific event
        
        :param project_name: str
        :param arm_name: str
        :param event_descrip: str
        :param engine: `sqlalchemy.Engine`
        :return: `pandas.DataFrame`
        """
        project_id = self.get_mysql_project_id(project_name)
        if not project_id : 
            return pandas.DataFrame() 

        arm_id = self.get_mysql_arm_id(arm_name, project_id)
        event_id = self.get_mysql_event_id(event_descrip, arm_id)
        sql = "SELECT DISTINCT record " \
              "FROM redcap.redcap_data AS rd " \
              "WHERE rd.project_id = {0} " \
              "AND rd.event_id = {1}".format(project_id, event_id)
        if subject_id :
            sql +=  " AND rd.record = '{0}';".format(subject_id)
        else :
            sql +=";"
        
        return pd.read_sql(sql, self.api['redcap_mysql_db'])
    


    def delete_mysql_table_records(self, table_name, record_list):
        sql = 'DELETE FROM ' + table_name + ' WHERE ' + table_name + '.ld_id IN ({0});'.format(record_list)
        execute(sql, self.api['redcap_mysql_db'])
        return len(record_list)

    def add_mysql_table_records(self, table_name, project_name, arm_name, event_descrip, name_of_form, record_list, outfile=None):
        # get the ids needed to lock the forms
        project_id = self.get_mysql_project_id(project_name)
        if not project_id : 
            return -1

        arm_id = self.get_mysql_arm_id(arm_name, project_id)
        event_id = self.get_mysql_event_id(event_descrip, arm_id)

        len_list = len(record_list)
        user_name =  self.__config_usr_data.get_category('redcap')['user'] 
        
        project_id_series = [project_id] * len_list
        event_id_series = [event_id] * len_list
        form_name_series = [name_of_form] * len_list
        username_series = [user_name] * len_list
        additional_records = dict(project_id=project_id_series,
                               record=record_list.record.tolist(),
                               event_id=event_id_series,
                               form_name=form_name_series,
                               username=username_series,
                               timestamp=datetime.datetime.now())

        dataframe = pd.DataFrame(data=additional_records)
        dataframe.to_sql(table_name, self.api['redcap_mysql_db'], if_exists='append', index=False)

        if outfile : 
            dataframe.record.to_csv(outfile, index=False)

        return len(record_list)

    def svn_client(self):
        svn_laptop = self.api['svn_laptop']
        if not svn_laptop:
            slog.info('session.svn_client',"ERROR: svn api is not defined")
            return None

        client = svn_laptop['client']
        return client

if __name__ == '__main__':
    import argparse 
    default = 'default: %(default)s'
    formatter = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(prog="session.py",
                                     description="Call a specific function in session",
                                     formatter_class=formatter)
    parser.add_argument('-c', '--config',
                        help="SIBIS config file. {}".format(default),
                        default=os.environ.get('SIBIS_CONFIG'))
    parser.add_argument('function_call',help="variable to get value for")
    argv = parser.parse_args()
    slog.init_log(False, False,'session', 'session',None)
    sInstance = Session()
    sInstance.configure(argv.config)
    print(getattr(sInstance,argv.function_call)())
    sys.exit()


