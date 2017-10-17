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
import hashlib
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
        self.api = {'xnat': None, 'import_laptops' : None, 'import_webcnp' : None, 'data_entry': None, 'redcap_mysql_db' : None, 'browser_penncnp': None} 
        # redcap projects are import_laptops, import_webcnp, and data_entry
        self.__active_redcap_project__ = None
   
    def configure(self, config_file=None):
        """
        Configures the session object by first checking for an
        environment variable, then in the home directory.
        """

        err_msg = self.__config_usr_data.configure(config_file)
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
        elif api_type == 'browser_penncnp' : 
            connectionPtr = self.__connect_penncnp__()
        elif api_type == 'redcap_mysql_db' : 
            connectionPtr = self.__connect_redcap_mysql__()
        else :
            connectionPtr = self.__connect_redcap_project__(api_type)
            
        if timeFlag : 
            slog.takeTimer2('connect_' + api_type) 

        return connectionPtr

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
            
        except Exception,err_msg:
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
        except Exception, err_msg:
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
        browser = webdriver.Firefox( firefox_profile=fp )

        # Log into website
        browser.get(penncnp_srv_data["server"])
        browser.find_element_by_name("adminid").send_keys(penncnp_usr_data['user'] )
        browser.find_element_by_name("pwd").send_keys(penncnp_usr_data['password'] )
        browser.find_element_by_name("Login").click()

        # Exit configuration
        self.api['browser_penncnp'] = {"browser": browser, "pip" : int(pip), "display": display} 
        return browser

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

        cfg = self.__config_usr_data.get_category('redcap-mysql') 
        if not cfg:
            slog.info('session.__connect_redcap_mysql__','Error: config file does not contain section recap-mysql',
                      config_file = self.__config_usr_data.get_config_file())
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

    def __get_analysis_dir(self) :
        analysis_dir = self.__config_usr_data.get_value('analysis_dir')
        if analysis_dir == None :
            slog.info("session.__get_analysis_dir-" + hashlib.sha1(str(self.__config_usr_data.get_config_file())).hexdigest()[0:6],"ERROR: 'analysis_dir' is not defined in config file !",
                      config_file = self.__config_usr_data.get_config_file())
            
        return  analysis_dir
            
            
    def get_project_name(self):
        return self.__config_usr_data.get_value('project_name')


    def get_email(self):
        return self.__config_usr_data.get_value('email')

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

    def get_config_sys_parser(self):
        oDir = self.get_operations_dir()
        if not oDir :
            return (None,"ERROR: could not retrieve operations directory") 

        sys_file = os.path.join(oDir,'sibis_sys_config.yml')
        if not os.path.exists(sys_file) : 
            return (None,"ERROR:", sys_file," does not exist!") 

        # Get procject specific settings for test file 
        sys_file_parser = cfg_parser.config_file_parser()
        err_msg = sys_file_parser.configure(sys_file)
        if err_msg:
            return (None, str(err_msg) + " (config_sys_file : " + str(config_sys_file) + ")")

        return (sys_file_parser, None)

        
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


    def get_laptop_dir(self):
        return os.path.join(self.__config_usr_data.get_value('import_dir'),'laptops')


    def get_xnat_dir(self):
        return os.path.join(self.__config_usr_data.get_value('import_dir'),'XNAT')

    def get_xnat_server_address(self):
        return self.__config_usr_data.get_value('xnat','server')

    def xnat_get_experiment(self,eid): 
        xnat_api = self.__get_xnat_api__()
        if not xnat_api:
            error_msg = "XNAT API is not defined! Cannot retrieve experiment!",
            slog.info(eid,error_msg,
                      function = "session.xnat_get_experiment")
            return None
 
        try : 
            xnat_experiment = xnat_api.select.experiment(eid)

        except Exception, err_msg:
            slog.info(eid + "-" + hashlib.sha1(str(err_msg)).hexdigest()[0:6],"ERROR: experiment could not be found!",
                      err_msg = str(err_msg),
                      function = "session.xnat_get_experiment")
            return None
            
        if not xnat_experiment:
            slog.info(eid + "-" + hashlib.sha1("session.xnat_get_subject_attribute").hexdigest()[0:6],"ERROR: experiment not found!", 
            function = "session.xnat_get_experiment")

        return xnat_experiment

    # replaces xnat_api.select.project(prj).subject( subject_label ).attrs.get(attribute)
    def xnat_get_subject_attribute(self,project,subject_label,attribute):
        xnat_api = self.__get_xnat_api__()
        if not xnat_api:
            error_msg = "XNAT API is not defined! Cannot retrieve value for " + attribute
            slog.info(subject_label,error_msg,
                      function = "session.xnat_get_subject_attribute",
                      attribute = attribute,
                      project = project)
            return None
 
        try : 
            xnat_project = xnat_api.select.project(project)

        except Exception, err_msg:
            slog.info(subject_label + "-" + hashlib.sha1(str(err_msg)).hexdigest()[0:6],"ERROR: project could not be found!",
                      err_msg = str(err_msg),
                      function = "session.xnat_get_subject_attribute",
                      project = project)
            return None
            
        if not xnat_project:
            slog.info(subject_label + "-" + hashlib.sha1("session.xnat_get_subject_attribute").hexdigest()[0:6],"ERROR: project not found !",
                      function = "session.xnat_get_subject_attribute",
                      project = project)
            return None

        try : 
            xnat_subject = xnat_project.subject( subject_label )

        except Exception, err_msg:
            slog.info(subject_label + "-" + hashlib.sha1(str(err_msg)).hexdigest()[0:6],"ERROR: subject could not be found!",
                      err_msg = str(err_msg),
                      project = project,
                      function = "session.xnat_get_subject_attribute",
                      subject = subject_label)
            return None

        if not xnat_subject:
            slog.info(subject_label + "-" + hashlib.sha1("session.xnat_get_subject_attribute").hexdigest()[0:6] ,"ERROR: subject not found !",
                      project = project,
                      function = "session.xnat_get_subject_attribute",
                      subject = subject_label)
            return None

        try : 
            if attribute == "label" :
                return xnat_subject.label()
            else :
                return xnat_subject.attrs.get(attribute)

        except Exception, err_msg:
            slog.info("session.xnat_get_subject_attribute" + hashlib.sha1(str(err_msg)).hexdigest()[0:6],"ERROR: attribute could not be found!",
                      err_msg = str(err_msg),
                      project = project,
                      subject = subject_label,
                      function = "session.xnat_get_subject_attribute",
                      attribute = attribute)
            
        return None


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

    def __get_xnat_api__(self): 
        if not self.api['xnat'] : 
            slog.info('__get_xnat_api__','Error: XNAT api not defined')  
            return None

        return self.api['xnat']

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
        except Exception, err_msg:
            pass
            
        
        if err_msg: 
            slog.info("session.__connect_penncnp__","The following command failed %s with the following output %s" % (kill_cmd,str(err_msg)))
            return None

    def get_redcap_server_address(self):
        return self.__config_usr_data.get_value('redcap','server')


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
	print "RUNNING redcap_export_records"
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

    def redcap_import_record(self, error_label, subject_label, event, time_label, records, record_id=None):
        red_api = self.__get_active_redcap_api__()
        if not red_api: 
            return None

        if time_label:
            slog.startTimer2() 
        try:
            import_response = red_api.import_records(records, overwrite='overwrite')

        except requests.exceptions.RequestException as e:
            error = 'Failed to import into REDCap' 
            err_list = ast.literal_eval(str(e))['error'].split('","')
            error_label  += '-' + hashlib.sha1(str(e)).hexdigest()[0:6] 

            if len(records) > 1 :
                slog.info(error_label, error,
                          requestError=str(e))

            else :
                record = records[0]
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
                                  import_record_id=str(record_id), 
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
                              import_record_id=str(record_id),  
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
    print getattr(sInstance,argv.function_call)()
    sys.exit()


