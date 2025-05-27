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
from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import object
from contextlib import contextmanager
import ast
import os
import time
import datetime
import requests
import hashlib
import pandas as pd
from pandas.io.sql import execute
import re
import warnings
from sibispy.svn_util import SibisSvnClient


from sibispy import sibislogger as slog
from sibispy import config_file_parser as cfg_parser

# --------------------------------------------
# this class was created to capture output from xnat
# if one cannot connect to server
from io import StringIO
import sys
from typing import Mapping

try:
    from typing import Literal
except:
    # for python < 3.8
    from typing_extensions import Literal
 
class Capturing(list):
    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self

    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        del self._stringio  # free up some memory
        sys.stdout = self._stdout


class StreamTee(object):
    """
    Allows you to use a single file-like object to write to two file-like objects at the same time.

    > import sys
    > logfile = file("blah.txt", "w+")
    > sys.stdout = StreamTee(sys.stdout, logfile)
    """

    def __init__(self, stream1, stream2):
        self.stream1 = stream1
        self.stream2 = stream2
        self.__missing_method_name = None

    def __getattribute__(self, name):
        return object.__getattribute__(self, name)

    def __getattr__(self, name):
        self.__missing_method_name = name
        return getattr(self, "__methodmissing__")

    def __methodmissing__(self, *args, **kwargs):
        callable2 = getattr(self.stream2, self.__missing_method_name)
        callable2(*args, **kwargs)

        callable1 = getattr(self.stream1, self.__missing_method_name)
        return callable1(*args, **kwargs)


class CapturingTee(list):
    """
    Same thing as Capturing, however it doesn't block stdout from being written to console.
    """

    def __enter__(self):
        self._stdout = sys.stdout
        self._stringio = StringIO()
        sys.stdout = StreamTee(self._stdout, self._stringio)
        return self

    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        del self._stringio
        sys.stdout = self._stdout


class ValueKeyDict(dict):
    """
    Creates a `dict` with custom indexing behavior where when the requested index
    value is `None`, the index key is returned.

    Example:
    > foo = ValueKeyDict({
    >     "alpha": None,
    >     "beta": "charlie"
    > })
    > foo.get("alpha") == None
    True
    > foo.get("beta") == "charlie"
    True
    > foo["alpha"] == "alpha"
    True
    > foo["beta"] == "charlie"
    True
    """

    def __getitem__(self, index):
        val = super().__getitem__(index)
        if val == None:
            return index
        else:
            return val


SIBIS_DIRS = ValueKeyDict(
    {
        "beta": None,
        "log": None,
        "operations": None,
        "cases": None,
        "summaries": None,
        "burn2dvd": None,
        "datadict": None,
        "laptops": None,
        "laptops_svn": "ncanda",
        "laptops_imported": "imported",
        "XNAT": None,
        "redcap": None,
    }
)


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

    def __init__(self, opt_api: Mapping = {}):
        self.__config_usr_data = cfg_parser.config_file_parser()
        self.__config_srv_data = None
        self.api = {
            "xnat": None,
            "xnat_http": None,
            "import_laptops": None,
            "import_webcnp": None,
            "data_entry": None,
            "redcap_mysql_db": None,
            "browser_penncnp": None,
            "svn_laptop": None,
        }
        # redcap projects are import_laptops, import_webcnp, and data_entry

        # Inject additional API options if required (to avoid depending on the
        # hard-coded options above)
        self.api.update(opt_api)

        self.__active_redcap_project__ = None
        self.__ordered_config_load = False

    def configure(self, config_file=None, ordered_config_load_flag=False):
        """
        Configures the session object by first checking for an
        environment variable, then in the home directory.
        """
        self.__ordered_config_load = ordered_config_load_flag
        err_msg = self.__config_usr_data.configure(
            config_file, ordered_load=self.__ordered_config_load
        )
        if err_msg:
            slog.info("session.configure", str(err_msg), sibis_config_file=config_file)
            return False

        (sys_file_parser, err_msg) = self.get_config_sys_parser()
        if err_msg:
            slog.info("session.configure", str(err_msg))
            return False

        if sys_file_parser.has_category("sibis_dirs"):
            sibis_dirs = sys_file_parser.get_category("sibis_dirs")
            SIBIS_DIRS.update(sibis_dirs)

        self.__config_srv_data = sys_file_parser.get_category("session")

        return True

    def connect_server(self, api_type, timeFlag=False, penncnp_HiddenBrowserFlag=True):
        """
        Connect to servers, setting each property.
        """
        if api_type not in self.api:
            slog.info(
                "session.connect_server",
                "api type " + api_type + " not defined !",
                api_types=str(list(self.api.keys())),
            )
            return None

        if timeFlag:
            slog.startTimer2()

        if api_type == "xnat":
            connectionPtr = self.__connect_xnat__()
        elif api_type == "xnat_http":
            connectionPtr = self.__connect_xnat_http__()
        elif api_type == "browser_penncnp":
            connectionPtr = self.__connect_penncnp__(penncnp_HiddenBrowserFlag)
        elif api_type == "svn_laptop":
            connectionPtr = self.__connect_svn_laptop__()
        elif api_type == "redcap_mysql_db":
            connectionPtr = self.__connect_redcap_mysql__()
        else:
            connectionPtr = self.__connect_redcap_project__(api_type)

        if timeFlag:
            slog.takeTimer2("connect_" + api_type)

        return connectionPtr

    # Access xnat directly through http calls
    def __connect_xnat_http__(self):
        import requests

        cfg = self.__config_usr_data.get_category("xnat")
        jsessionid = "".join([self.get_xnat_data_address(), "/JSESSIONID"])

        http_session = requests.session()
        http_session.auth = (cfg["user"], cfg["password"])
        try:
            http_session.get(jsessionid)

        except Exception as err_msg:
            slog.info(
                "session.__connect_xnat_http__", str(err_msg), server=cfg.get("server")
            )
            return None

        self.api["xnat_http"] = http_session
        return http_session

    # Access xnat through XnatUtil
    def __connect_xnat__(self):
        from .xnat_util import XnatUtil

        cfg = self.__config_usr_data.get_category("xnat")
        try:
            util = XnatUtil(
                server=cfg.get("server"),
                user=cfg.get("user"),
                password=cfg.get("password"),
            )

            raw_xnat = util.connect()

        except Exception as err_msg:
            slog.info(
                "session.__connect_xnat__", str(err_msg), server=cfg.get("server")
            )
            return None

        self.api["xnat"] = util
        return util

    def __list_running_process__(self, cmd):
        from subprocess import Popen, PIPE

        check_cmd = "ps -efww | grep '" + cmd + "' | awk '{print $2}'"

        try:
            p = Popen(check_cmd, shell=True, stdout=PIPE, stderr=PIPE)
            return p.communicate()

        except Exception as err_msg:
            return (None, str(err_msg))

    @contextmanager
    def __connect_penncnp__(self, penncnp_HiddenBrowserFlag=True):
        # Check that config file is correctly defined
        if "penncnp" not in list(self.__config_srv_data.keys()):
            slog.info(
                "session.__connnect_penncnp__",
                "ERROR: penncnp server info not defined!",
            )
            yield
            return

        penncnp_srv_data = self.__config_srv_data["penncnp"]

        if "penncnp" not in list(self.__config_usr_data.keys()):
            slog.info(
                "session.__connnect_penncnp__", "ERROR: penncnp user info not defined!"
            )
            yield
            return

        penncnp_usr_data = self.__config_usr_data.get_category("penncnp")

        if penncnp_HiddenBrowserFlag:         
            # Check if display is open
            display = ":" + str(penncnp_srv_data["display"])
            vfb_cmd = "vfb +extension RANDR " + display
            check_cmd = "[X]" + vfb_cmd
            
            (pip_list, err_msg) = self.__list_running_process__(check_cmd)
            if err_msg:
                slog.info(
                    "session.__connect_penncnp__",
                    "Checking if command %s is already running failed with the following error message: %s"
                    % (check_cmd, strcheck_err),
                )
                yield
                return

            if pip_list:
                slog.info(
                    "session.__connect_penncnp__",
                    "Error: sessions with display "
                    + display
                    + " are already running ! Please execute 'kill -9 "
                    + pip_list.decode("utf-8")
                    + "' before proceeding!",
                )
                yield
                return

            # Open screen
            import subprocess

            display_cmd = "X" + vfb_cmd + " &> /dev/null"
            try:
                proc = subprocess.Popen(display_cmd, shell=True)
                proc.poll()
                if proc.returncode is not None:
                    (out, err_msg) = proc.communicate(timeout=30)
            except Exception as err_msg:
                pass

            if err_msg:
                slog.info(
                    "session.__connect_penncnp__",
                    "The following command failed %s with the following output %s"
                    % (display_cmd, str(err_msg)),
                )
                yield
                self.disconnect_penncnp()
                return

            (pip, err_msg) = self.__list_running_process__(check_cmd)
            if err_msg:
                slog.info(
                    "session.__connect_penncnp__",
                    "Checking if command %s is already running failed with the following error message: %s"
                    % (check_cmd, strcheck_err),
                )
                yield
                self.disconnect_penncnp()
                return
        
            # if multiple pips are returned (run outside the container)
            pip=pip.decode("utf-8").split('\n')[0]
            if not pip:
                slog.info(
                    "session.__connect_penncnp__",
                    "Error: sessions with display " + display + " did not start up!",
                )
                yield
                self.disconnect_penncnp()
                return

            os.environ["DISPLAY"] = display
        else:
            # Do not set display
            display=""
            pip=-1

        # Set up Browser
        # Configure Firefox profile for automated file download
        from selenium import webdriver

        fp = webdriver.FirefoxProfile()
        fp.set_preference("browser.download.folderList", 2)
        fp.set_preference("browser.download.manager.showWhenStarting", False)
        fp.set_preference("browser.download.dir", os.getcwd())
        fp.set_preference(
            "browser.helperApps.neverAsk.saveToDisk", "application/vnd.ms-excel"
        )
        browser = webdriver.Firefox(
            firefox_profile=fp, service_log_path="/tmp/geckodriver.log"
        )

        # Log into website
        browser.get(penncnp_srv_data["server"])
        browser.find_element_by_name("adminid").send_keys(penncnp_usr_data["user"])
        browser.find_element_by_name("pwd").send_keys(penncnp_usr_data["password"])
        browser.find_element_by_name("Login").click()

        # Exit configuration
        self.api["browser_penncnp"] = {
            "browser": browser,
            "pip": int(pip),
            "display": display,
        }

        try:
            yield browser
        except:
            raise
        finally:
            self.disconnect_penncnp()

    def __connect_svn_laptop__(self):
        # Check that config file is correctly defined
        if "svn_laptop" not in list(self.__config_usr_data.keys()):
            slog.info(
                "session.__connnect_svn_laptop__",
                "ERROR: svn laptop user info not defined!",
            )
            return None
        usr_data = self.__config_usr_data.get_category("svn_laptop")

        svnDir = self.get_laptop_svn_dir()
        client = SibisSvnClient(
            svnDir, username=usr_data["user"], password=usr_data["password"]
        )
        self.api["svn_laptop"] = {
            "client": client,
            "user": usr_data["user"],
            "password": usr_data["password"],
        }

        return client

    def __connect_redcap_project__(self, api_type):
        import redcap

        cfg = self.__config_usr_data.get_category("redcap")
        if not cfg:
            slog.info(
                "session.__connect_redcap_project__",
                "Error: config file does not contain section redcap",
                config_file=self.__config_usr_data.get_config_file(),
            )

            return None

        try:
            data_entry = redcap.Project(
                cfg.get("server"),
                cfg.get(api_type + "_token"),
                verify_ssl=cfg.get("verify_ssl"),
            )
            self.api[api_type] = data_entry

        except KeyError as err:
            slog.info(
                "session.__connect_redcap_project__",
                str(err),
                server=cfg.get("server"),
                api_type=api_type,
            )
            return None

        except requests.RequestException as err:
            slog.info(
                "session.__connect_redcap_project__",
                str(err),
                server=cfg.get("server"),
                api_type=api_type,
            )
            return None

        self.__active_redcap_project__ = api_type

        return data_entry

    def __connect_redcap_mysql__(self):
        from sqlalchemy import create_engine

        cfg = self.__config_usr_data.get_category("redcap-mysql")
        if not cfg:
            slog.info(
                "session.__connect_redcap_mysql__",
                "Error: config file does not contain section redcap-mysql",
                config_file=self.__config_usr_data.get_config_file(),
            )
            return None

        # by default should be set to 3306
        port = cfg.get("port")
        if not port:
            slog.info(
                "session.__connect_redcap_mysql__",
                "Error: config file does not define 'port' in section 'redcap-mysql'",
                config_file=self.__config_usr_data.get_config_file(),
            )
            return None

        user = cfg.get("user")
        passwd = cfg.get("passwd")
        db = cfg.get("db")
        hostname = cfg.get("hostname")

        connection_string = "mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(
            user, passwd, hostname, port, db
        )

        try:
            engine = create_engine(connection_string, pool_recycle=3600)
        except Exception as err_msg:
            slog.info(
                "session.__connect_redcap_mysql__",
                str(err_msg),
                database=db,
                hostname=hostname,
            )
            return None

        self.api["redcap_mysql_db"] = engine

        return engine

    def __get_analysis_dir(self):
        analysis_dir = self.__config_usr_data.get_value("analysis_dir")
        if analysis_dir == None:
            slog.info(
                "session.__get_analysis_dir-"
                + hashlib.sha1(
                    str(self.__config_usr_data.get_config_file()).encode("utf-8")
                ).hexdigest()[0:6],
                "ERROR: 'analysis_dir' is not defined in config file !",
                config_file=self.__config_usr_data.get_config_file(),
            )

        return analysis_dir

    def get_analysis_dir(self):
        return self.__get_analysis_dir()

    def get_import_dir(self):
        import_dir = self.__config_usr_data.get_value("import_dir")
        if import_dir == None:
            slog.info(
                "session.get_import_dir-"
                + hashlib.sha1(
                    str(self.__config_usr_data.get_config_file()).encode("utf-8")
                ).hexdigest()[0:6],
                "ERROR: 'import_dir' is not defined in config file !",
                config_file=self.__config_usr_data.get_config_file(),
            )
        return import_dir

    def get_ordered_config_load(self):
        return self.__ordered_config_load

    def get_project_name(self):
        return self.__config_usr_data.get_value("project_name")

    def get_email(self):
        return self.__config_usr_data.get_value("email")

    def get_beta_dir(self):
        aDir = self.__get_analysis_dir()
        if aDir:
            return os.path.join(aDir, SIBIS_DIRS["beta"])
        return None

    def get_log_dir(self):
        aDir = self.__get_analysis_dir()
        if aDir:
            return os.path.join(aDir, SIBIS_DIRS["log"])
        return None

    def get_operations_dir(self):
        aDir = self.__get_analysis_dir()
        if aDir:
            return os.path.join(aDir, SIBIS_DIRS["operations"])
        return None

    def get_config_parser_for_file(self, key, filename):
        oDir = self.get_operations_dir()
        if not oDir:
            return (None, "ERROR: could not retrieve operations directory")

        sys_file = os.path.join(oDir, filename)
        if not os.path.exists(sys_file):
            return (None, "ERROR:" + sys_file + " does not exist!")

        # Get project specific settings for test file
        sys_file_parser = cfg_parser.config_file_parser()
        err_msg = sys_file_parser.configure(
            sys_file, ordered_load=self.__ordered_config_load
        )
        if err_msg:
            return (None, "{0} ({1} : {2})".format(str(err_msg), key, str(sys_file)))

        return (sys_file_parser, None)

    def get_config_test_parser(self):
        return self.get_config_parser_for_file(
            "config_test_file", "sibis_test_config.yml"
        )

    def get_config_sys_parser(self):
        return self.get_config_parser_for_file(
            "config_sys_file", "sibis_sys_config.yml"
        )

    def get_cases_dir(self):
        aDir = self.__get_analysis_dir()
        if aDir:
            return os.path.join(aDir, SIBIS_DIRS["cases"])
        return None

    def get_summaries_dir(self):
        aDir = self.__get_analysis_dir()
        if aDir:
            return os.path.join(aDir, SIBIS_DIRS["summaries"])
        return None

    def get_dvd_dir(self):
        aDir = self.__get_analysis_dir()
        if aDir:
            return os.path.join(aDir, SIBIS_DIRS["burn2dvd"])
        return None

    def get_datadict_dir(self):
        aDir = self.__get_analysis_dir()
        if aDir:
            return os.path.join(aDir, SIBIS_DIRS["datadict"])
        return None

    def __get_laptop_dir(self):
        return os.path.join(
            self.__config_usr_data.get_value("import_dir"), SIBIS_DIRS["laptops"]
        )

    # Kilian: Ommit ncanda from svn dir name
    def get_laptop_svn_dir(self):
        return os.path.join(self.__get_laptop_dir(), SIBIS_DIRS["laptops_svn"])

    def get_laptop_imported_dir(self):
        return os.path.join(self.__get_laptop_dir(), SIBIS_DIRS["laptops_imported"])

    def get_xnat_dir(self):
        return os.path.join(
            self.__config_usr_data.get_value("import_dir"), SIBIS_DIRS["XNAT"]
        )

    # Important for redcap front end - not sibis programs
    def get_redcap_uploads_dir(self):
        return os.path.join(
            self.__config_usr_data.get_value("import_dir"), SIBIS_DIRS["redcap"]
        )

    def get_xnat_server_address(self):
        return self.__config_usr_data.get_value("xnat", "server")

    def get_xnat_data_address(self):
        return self.get_xnat_server_address() + "/data"

    def get_xnat_experiments_address(self):
        return self.get_xnat_data_address() + "/experiments"

    def get_xnat_session_address(
        self, experiment_id: str, output_format: Literal["html", "csv", "json"] = "html"
    ):
        return (
            self.get_xnat_experiments_address()
            + "/"
            + experiment_id
            + "?format="
            + output_format
        )

    def xnat_http_get_all_experiments(self):
        xnat_http_api = self.__get_xnat_http_api__()
        if not xnat_http_api:
            return None

        return xnat_http_api.get(self.get_xnat_experiments_address() + "?format=csv")

    def xnat_http_get_experiment_xml(self, experiment_id):
        xnat_http_api = self.__get_xnat_http_api__()
        if not xnat_http_api:
            return None

        return xnat_http_api.get(self.get_xnat_session_address(experiment_id, "csv"))

    def xnat_get_classes(self):
        xnat_api = self.__get_xnat_api__()
        if not xnat_api:
            error_msg = ("XNAT API is not defined! Cannot retrieve classes!",)
            slog.info(eid, error_msg, function="session.xnat_get_classes")
            return None
        return xnat_api.client.classes

    # makes a difference where later saved file on disk how the function is called
    def xnat_get_experiment(self, eid, project=None, subject_label=None):
        xnat_api = self.__get_xnat_api__()
        if not xnat_api:
            error_msg = ("XNAT API is not defined! Cannot retrieve experiment!",)
            slog.info(eid, error_msg, function="session.xnat_get_experiment")
            return None

        # makes a difference where later saved file on disk how the function is called
        if project and subject_label:
            select_object = self.xnat_get_subject(project, subject_label)
            if not select_object:
                slog.info(
                    subject_label,
                    "ERROR: session.xnat_get_experiment: subject "
                    + subject_label
                    + " not found !",
                    project=project,
                )
                return None
        else:
            select_object = xnat_api.select

        try:
            xnat_experiment = select_object.experiments[eid]
        except KeyError as err_msg:
            slog.info(
                eid + "-" + hashlib.sha1(str(err_msg).encode("utf-8")).hexdigest()[0:6],
                "WARNING: eid: {} does not exist!".format(eid),
                err_msg=str(err_msg),
                function="session.xnat_get_experiment",
            )
            return None
        except Exception as err_msg:
            slog.info(
                eid + "-" + hashlib.sha1(str(err_msg).encode("utf-8")).hexdigest()[0:6],
                "ERROR: problem with xnat api !",
                err_msg=str(err_msg),
                function="session.xnat_get_experiment",
            )
            return None

        # not sure how this would ever happen
        if not xnat_experiment:
            slog.info(
                eid,
                "ERROR: session.xnat_get_experiment: experiment not created - problem with xnat api!",
            )
            return None

        # if not xnat_experiment.exists() :
        #     slog.info(eid,"ERROR: session.xnat_get_experiment: experiment does not exist !")
        #     return None

        return xnat_experiment

    # replaces xnat_api.select.project(prj).subject( subject_label ).attrs.get(attribute)
    def xnat_get_subject(self, project, subject_label):
        xnat_api = self.__get_xnat_api__()
        if not xnat_api:
            error_msg = "XNAT API is not defined! Cannot retrieve subject !"
            slog.info(
                subject_label,
                error_msg,
                function="session.xnat_get_subject",
                project=project,
            )
            return None

        try:
            xnat_project = xnat_api.select.projects[project]
        except KeyError as err_msg:
            slog.info(
                subject_label
                + "-"
                + hashlib.sha1(str(err_msg).encode("utf-8")).hexdigest()[0:6],
                "WARNING: project:{} could not be found!".format(project),
                err_msg=str(err_msg),
                function="session.xnat_get_subject",
                project=project,
            )
            return None

        except Exception as err_msg:
            slog.info(
                subject_label
                + "-"
                + hashlib.sha1(str(err_msg).encode("utf-8")).hexdigest()[0:6],
                "ERROR: project could not be found!",
                err_msg=str(err_msg),
                function="session.xnat_get_subject",
                project=project,
            )
            return None

        if not xnat_project:
            slog.info(
                subject_label,
                "ERROR: session.xnat_get_subject: project " + project + " not found !",
            )
            return None

        try:
            xnat_subject = xnat_project.subjects[subject_label]

        except Exception as err_msg:
            slog.info(
                subject_label
                + "-"
                + hashlib.sha1(str(err_msg).encode("utf-8")).hexdigest()[0:6],
                "ERROR: subject could not be found!",
                err_msg=str(err_msg),
                project=project,
                function="session.xnat_get_subject",
                subject=subject_label,
            )
            return None

        return xnat_subject

    def xnat_get_subject_attribute(self, project, subject_label, attribute):
        xnat_subject = self.xnat_get_subject(project, subject_label)
        if not xnat_subject:
            issue_url = slog.info(
                subject_label,
                "ERROR: session.xnat_get_subject_attribute: subject "
                + subject_label
                + " not found !",
                project=project,
            )
            return [None, issue_url]

        try:

            try:
                return [getattr(xnat_subject, attribute), None]
            except:
                return [getattr(xnat_subject, attribute.lower()), None]

        except Exception as err_msg:
            issue_url = slog.info(
                "session.xnat_get_subject_attribute"
                + hashlib.sha1(str(err_msg).encode("utf-8")).hexdigest()[0:6],
                "ERROR: attribute could not be found!",
                err_msg=str(err_msg),
                project=project,
                subject=subject_label,
                function="session.xnat_get_subject_attribute",
                info="Did subject change site ? Make sure that site of subject ID matches project !",
                attribute=attribute,
            )

            return [None, issue_url]

    # if time_label is set then will take the time of the operation
    def xnat_export_general(self, form, fields, conditions, time_label=None):
        xnat_api = self.__get_xnat_api__()
        if not xnat_api:
            return None

        if time_label:
            slog.startTimer2()
        try:
            #  python if one cannot connect to server then
            with Capturing() as xnat_output:
                xnat_data = list(
                    xnat_api.search(form, fields).where(conditions).items()
                )

        except Exception as err_msg:
            if xnat_output:
                slog.info(
                    "session.xnat_export_general",
                    "ERROR: querying XNAT failed most likely due disconnect to server ({})".format(
                        time.asctime()
                    ),
                    xnat_api_output=str(xnat_output),
                    form=str(form),
                    fields=str(fields),
                    conditions=str(conditions),
                    err_msg=str(err_msg),
                )
            else:
                slog.info(
                    "session.xnat_export_general",
                    "ERROR: querying XNAT failed at {}".format(time.asctime()),
                    form=str(form),
                    fields=str(fields),
                    conditions=str(conditions),
                    err_msg=str(err_msg),
                )
            return None

        if time_label:
            slog.takeTimer2("xnat_export_" + time_label)

        return xnat_data

    def __get_xnat_api__(self):
        if not self.api["xnat"]:
            slog.info("__get_xnat_api__", "Error: XNAT api not defined")
            return None

        return self.api["xnat"]

    def __get_xnat_http_api__(self):
        if not self.api["xnat_http"]:
            slog.info("__get_xnat_http_api__", "Error: XNAT_HTTP api not defined")
            return None

        return self.api["xnat_http"]

    def initialize_penncnp_wait(self):
        from selenium.webdriver.support.ui import WebDriverWait

        return WebDriverWait(
            self.api["browser_penncnp"]["browser"],
            self.__config_srv_data["penncnp"]["wait"],
        )

    def get_penncnp_export_report(self, wait):
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.common.by import By

        try:
            report = wait.until(EC.element_to_be_clickable((By.NAME, "Export Report")))
        except Exception as e:
            slog.info(
                "session.get_penncnp_export",
                "ERROR: Timeout, could not find Export Report",
                info="Try increasing wait time at WebDriverWait",
                msg=str(e),
            )
            return None

        return report

    def disconnect_penncnp(self):
        # Note, if python script is manually killed before reaching this function then the subprocesses (e.g. X display) are also automatically killed
        if not self.api["browser_penncnp"]:
            return

        self.api["browser_penncnp"]["browser"].quit()

        display=self.api["browser_penncnp"]["display"]
        if display != "":  
            if ("DISPLAY" in list(os.environ.keys()) and os.environ["DISPLAY"] == display):
                del os.environ["DISPLAY"]

        pip=self.api["browser_penncnp"]["pip"]
        if pip > 0 :     
            kill_cmd = "kill -9 " + str(pip)
            try:
                import subprocess
                err_msg = subprocess.check_output(kill_cmd, shell=True)
            except Exception as err:
                err_msg = err
                if err_msg:
                    slog.info(
                        "session.__connect_penncnp__",
                        "The following command failed %s with the following output %s"
                        % (kill_cmd, str(err_msg)),
                    )
                    return None

    def get_redcap_server_address(self):
        return self.__config_usr_data.get_value("redcap", "server")

    def get_redcap_base_address(self):
        return self.__config_usr_data.get_value("redcap", "base_address")

    def get_event_descrip_from_redcap_event_name(self, redcap_event_name: str):
        if redcap_event_name == "submission_4_inper_arm_6":
            return "Submission 4 In-person"
        visit_regex = r"(recovery_baseline)|(recovery_daily_\d*)|(recovery_weekly_\d)|(recovery_final)|(baseline_night_\d)|(\d*y_visit_night_\d)|(\d*y_visit)|(baseline_visit)|(\d*month_followup)|(submission_\d)"
        visit_match = re.match(visit_regex, redcap_event_name)
        if visit_match is None:
            raise ValueError(f"No matching visit for {redcap_event_name}")
        visit = visit_match.group()

        month_match = re.match(r"(\d*)month_followup", visit)
        if month_match:
            visit = month_match.group(1)  # '66month_followup' -> '66'
            event_descrip = visit + "-month follow-up"  # -> "66-month follow-up'
            return event_descrip

        yearly_standard_match = re.match(r"(\d*y)_visit$", visit)
        if yearly_standard_match:
            year = yearly_standard_match.group(1)  # '7y_visit' -> '7y'
            event_descrip = year + " visit"  # -> '7y visit'
            return event_descrip

        if visit == "baseline_visit":
            event_descrip = "Baseline visit"
            return event_descrip

        visit_words = visit.split("_")
        visit_words = [x.capitalize() for x in visit_words]
        event_descrip = " ".join(visit_words)
        return event_descrip

    def get_formattable_redcap_form_address(
        self,
        project_id: int,
        redcap_event_name: str,
        subject_id=None,
        name_of_form=None,
    ):
        # Returns a possibly formattable redcap link for the passed arguments, 3 mandatory:
        # project_name: see table in https://neuro.sri.com/labwiki/index.php?title=Locking_in_REDCap
        # redcap_event_name: e.g. 7y_visit_arm_1 or recovery_daily_29_arm_2
        # And 2 optional (if not passed, they will be replaced by the %s placeholder which can be replaced later with the real value):
        # subject_id: e.g. B-00002-F-2
        # name_of_form: e.g. stroop
        # To replace formatted args, do formattable_address % (subject_id, form_name)
        arm_match = re.search(r"arm_(\d*)", redcap_event_name)
        if arm_match is None:
            raise ValueError(f"No arm for {redcap_event_name}")
        arm_num = int(arm_match.group(1))

        event_descrip = self.get_event_descrip_from_redcap_event_name(redcap_event_name)

        self.connect_server("redcap_mysql_db", True)
        arm_id = self.get_mysql_arm_id_from_arm_num(arm_num, project_id)
        event_id = self.get_mysql_event_id(event_descrip, arm_id)

        if not name_of_form:
            name_of_form = "%s"
        if not subject_id:
            subject_id = "%s"

        base_address = self.get_redcap_base_address()
        version = str(self.get_redcap_version())
        formattable_address = (
            base_address
            + f"redcap_v{version}/DataEntry/index.php?pid={project_id}&id={subject_id}&event_id={event_id}&page={name_of_form}"
        )
        return formattable_address

    def get_formattable_redcap_subject_address(
        self, project_id: int, arm_num: int, subject_id=None
    ):
        # Returns a possibly formattable redcap link for the passed arguments, 2 mandatory:
        # project_name: see table in https://neuro.sri.com/labwiki/index.php?title=Locking_in_REDCap
        # arm_num: e.g. 1
        # And 1 optional (if not passed, they will be replaced by the %s placeholder which can be replaced later with the real value):
        # subject_id: e.g. B-00002-F-2
        # To replace formatted args, do formattable_address % (subject_id)

        if not subject_id:
            subject_id = "%s"

        base_address = self.get_redcap_base_address()
        version = str(self.get_redcap_version())
        formattable_address = (
            base_address
            + f"redcap_v{version}/DataEntry/record_home.php?pid={project_id}&arm={arm_num}&id={subject_id}"
        )
        return formattable_address

    #
    # REDCAP API CALLS
    #
    def __get_active_redcap_api__(self):
        project = self.__active_redcap_project__
        if not project:
            slog.info(
                "__get_active_redcap_api__",
                "Error: an active redcap project is currently not defined ! Most likely redcap api was not initialized correctly",
            )
            return None

        if not self.api[project]:
            slog.info(
                "__get_active_redcap_api__",
                "Error: " + str(project) + " api not defined",
            )
            return None

        return self.api[project]

    def get_redcap_version(self):
        api = self.__get_active_redcap_api__()
        if not api:
            return None
        return api.redcap_version

    def get_redcap_form_key(self):
        rc_version = self.get_redcap_version()
        if not rc_version:
            return ""

        if rc_version.major < 8:
            return "form_name"

        return "form"

    # if time_label is set then will take the time of the operation
    def redcap_export_records(self, time_label, **selectStmt):
        return self.redcap_export_records_from_api(time_label, None, **selectStmt)

    def redcap_export_records_from_api(self, time_label, api_type, **selectStmt):
        if api_type == None:
            red_api = self.__get_active_redcap_api__()
        else:
            if api_type in self.api:
                red_api = self.api[api_type]
            else:
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
                if (
                    "Specify dtype option on import or set low_memory=False"
                    not in w_str
                ):
                    slog.info(
                        "session.redcap_export_records",
                        "Waring: exporting data from REDCap caused warning at {}".format(
                            time.asctime()
                        ),
                        warning_msg=w_msg,
                        **selectStmt,
                    )

        except Exception as err_msg:
            slog.info(
                "session.redcap_export_records",
                "ERROR: exporting data from REDCap failed at {}".format(time.asctime()),
                err_msg=str(err_msg),
                **selectStmt,
            )
            return None

        if time_label:
            slog.takeTimer2("redcap_export_" + time_label)

        return redcap_data

    def redcap_import_record_to_api(
        self, records, api_type, error_label, time_label=None
    ):
        if len(records) == 0:
            return None

        if api_type == None:
            api_type = self.__active_redcap_project__

        if api_type in self.api:
            red_api = self.api[api_type]
        else:
            return None

        if not red_api:
            return None

        if time_label:
            slog.startTimer2()
        try:
            import_response = red_api.import_records(records, overwrite="overwrite")

        except requests.exceptions.RequestException as e:
            error = "session:redcap_import_record_to_api:Failed to import into REDCap"
            err_list = ast.literal_eval(str(e))["error"].split('","')
            error_label += "-" + hashlib.sha1(str(e).encode("utf-8")).hexdigest()[0:6]

            slog.info(error_label, error, requestError=str(e), red_api=api_type)
            return None

        if time_label:
            slog.takeTimer2("redcap_import_" + time_label, str(import_response))

        return import_response

    def redcap_import_record(
        self, error_label, subject_label, event, time_label, records, record_id=None
    ):
        if len(records) == 0:
            return None

        red_api = self.__get_active_redcap_api__()
        if not red_api:
            return None

        if time_label:
            slog.startTimer2()
        try:
            # deal with new redcap api not liking named multi-indexes.
            if isinstance(records.index, pd.MultiIndex) and None not in records.index.names:
                imp_records = records.reset_index(drop=False)
            else:
                imp_records = records

            import_response = red_api.import_records(imp_records, overwrite="overwrite", import_format="df")

        except requests.exceptions.RequestException as e:
            error = "session:redcap_import_record:Failed to import into REDCap"
            err_list = ast.literal_eval(str(e))["error"].split('","')
            error_label += "-" + hashlib.sha1(str(e).encode("utf-8")).hexdigest()[0:6]

            if len(records) > 1:
                slog.info(
                    error_label,
                    error,
                    requestError=str(e),
                    red_api=self.__active_redcap_project__,
                )

            else:
                if isinstance(records, list):
                    record = records[0]
                    record_event = None
                elif isinstance(records, pd.DataFrame):
                    record_ser = records.iloc[0]

                    # MultiIndex now hidden in pd.Series.name
                    try:
                        subject_label, record_event = record_ser.name
                    except ValueError:
                        # fallback if unpacking applied to single-index
                        # non-longitudinal projects
                        subject_label = record_ser.name[0]
                        record_event = None

                    error_label = "_".join(record_ser.name) + "_" + error_label
                    record = record_ser.to_dict()
                else:
                    slog.info(
                        error_label,
                        "ERROR: session:redcap_import_record: type is not yet implemented",
                        import_record_id=str(record_id),
                        requestError=str(e),
                        red_api=self.__active_redcap_project__,
                    )

                if (
                    len(err_list) > 3
                    and "This field is located on a form that is locked. You must first unlock this form for this record."
                    in err_list[3]
                ):
                    red_var = err_list[1]

                    try:
                        event = err_list[0].split("(")[1][:-1]
                    except IndexError:  # Try to obtain event from record if unextractable from error
                        if record_event is not None:
                            event = record_event
                        # otherwise, `event` stays as passed in function args

                    if subject_label and event is not None:
                        red_value_temp = self.redcap_export_records(
                            False,
                            fields=[red_var],
                            records=[subject_label],
                            events=[event],
                        )
                        if red_value_temp:
                            red_value = red_value_temp[0][red_var]
                            if (
                                "mri_xnat_sid" not in record
                                or "mri_xnat_eids" not in record
                            ):
                                slog.info(
                                    error_label,
                                    error,
                                    redcap_variable=red_var,
                                    redcap_event=event,
                                    redcap_value="'" + str(red_value) + "'",
                                    new_value="'" + str(err_list[2]) + "'",
                                    import_record_id=str(record_id),
                                    requestError=str(e),
                                    red_api=self.__active_redcap_project__,
                                )
                            else:
                                slog.info(
                                    error_label,
                                    error,
                                    redcap_value="'" + str(red_value) + "'",
                                    redcap_variable=red_var,
                                    redcap_event=event,
                                    new_value="'" + str(err_list[2]) + "'",
                                    xnat_sid=record["mri_xnat_sid"],
                                    xnat_eid=record["mri_xnat_eids"],
                                    requestError=str(e),
                                    red_api=self.__active_redcap_project__,
                                )
                        else:
                            slog.info(
                                error_label,
                                error,
                                redcap_variable=red_var,
                                redcap_event=event,
                                new_value="'" + str(err_list[2]) + "'",
                                import_record_id=str(record_id),
                                requestError=str(e),
                                red_api=self.__active_redcap_project__,
                            )
                    else:
                        slog.info(
                            error_label,
                            error,
                            redcap_variable=red_var,
                            redcap_event=event,
                            new_value="'" + str(err_list[2]) + "'",
                            import_record_id=str(record_id),
                            requestError=str(e),
                            red_api=self.__active_redcap_project__,
                        )

                elif "mri_xnat_sid" not in record or "mri_xnat_eids" not in record:
                    slog.info(
                        error_label,
                        error,
                        import_record_id=str(record_id),
                        requestError=str(e),
                        red_api=self.__active_redcap_project__,
                    )
                else:
                    slog.info(
                        error_label,
                        error,
                        xnat_sid=record["mri_xnat_sid"],
                        xnat_eid=record["mri_xnat_eids"],
                        requestError=str(e),
                        red_api=self.__active_redcap_project__,
                    )
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
        try:
            projects = pd.read_sql_table("redcap_projects", self.api["redcap_mysql_db"])
        except Exception as err_msg:
            slog.info(
                "session.get_mysql_project_id."
                + hashlib.sha1(str(err_msg).encode("utf-8")).hexdigest()[0:6],
                "ERROR: could not read sql table redcap_projects!",
                project_name=project_name,
                err_msg=str(err_msg),
            )
            return None

        project_id = projects[projects.project_name == project_name].project_id
        return int(project_id.iloc[0])

    def get_mysql_arm_id(self, arm_name, project_id):
        """
        Get an arm_id using the arm name and project_id

        :param arm_name: str
        :param project_id: int
        :return: int
        """
        arms = pd.read_sql_table("redcap_events_arms", self.api["redcap_mysql_db"])
        arm_id = arms[
            (arms.arm_name == arm_name) & (arms.project_id == project_id)
        ].arm_id
        return int(arm_id.iloc[0])

    def get_mysql_arm_id_from_arm_num(self, arm_num, project_id):
        """
        Get an arm_id using the arm name and project_id

        :param arm_name: int
        :param project_id: int
        :return: int
        """
        arms = pd.read_sql_table("redcap_events_arms", self.api["redcap_mysql_db"])
        arm_id = arms[
            (arms.arm_num == arm_num) & (arms.project_id == project_id)
        ].arm_id
        return int(arm_id.iloc[0])

    def get_mysql_event_id(self, event_descrip, arm_id):
        """
        Get an event_id using the event description and arm_id

        :param event_descrip: str
        :param arm_id: int
        :return: int
        """
        events = pd.read_sql_table(
            "redcap_events_metadata", self.api["redcap_mysql_db"]
        )
        event_id = events[
            (events.descrip == event_descrip) & (events.arm_id == arm_id)
        ].event_id
        return int(event_id.iloc[0])

    # 'redcap_locking_data'
    def get_mysql_table_records(
        self,
        table_name,
        project_name,
        arm_name=None,
        event_descrip=None,
        name_of_form=None,
        subject_id=None,
    ):
        """
        Get a dataframe of forms for a specific event

        :param project_name: str
        :param arm_name: str
        :param event_descrip: str
        :return: pandas.DataFrame`
        """
        table_record_df = pd.read_sql_table(table_name, self.api["redcap_mysql_db"])
        return self.get_mysql_table_records_from_dataframe(
            table_record_df,
            project_name,
            arm_name=arm_name,
            event_descrip=event_descrip,
            name_of_form=name_of_form,
            subject_id=subject_id,
        )

    def get_mysql_table_records_from_dataframe(
        self,
        table_records_df,
        project_name,
        arm_name=None,
        event_descrip=None,
        name_of_form=None,
        subject_id=None,
    ):
        """
        Get a dataframe of forms for a specific event

        :param project_name: str
        :param arm_name: str
        :param event_descrip: str
        :return: pandas.DataFrame`
        """

        project_id = self.get_mysql_project_id(project_name)
        if not project_id:
            return pd.DataFrame()

        table_records_df = table_records_df[table_records_df.project_id == project_id]

        if event_descrip and arm_name:
            arm_id = self.get_mysql_arm_id(arm_name, project_id)
            event_id = self.get_mysql_event_id(event_descrip, arm_id)
            table_records_df = table_records_df[table_records_df.event_id == event_id]

        if name_of_form:
            table_records_df = table_records_df[
                table_records_df.form_name == name_of_form
            ]

        if subject_id:
            table_records_df = table_records_df[table_records_df.record == subject_id]

        return table_records_df

    def get_mysql_project_records(
        self, project_name, arm_name, event_descrip, subject_id=None
    ):
        """
        Get a dataframe of records for a specific event

        :param project_name: str
        :param arm_name: str
        :param event_descrip: str
        :param engine: `sqlalchemy.Engine`
        :return: `pandas.DataFrame`
        """
        project_id = self.get_mysql_project_id(project_name)
        if not project_id:
            return pandas.DataFrame()

        arm_id = self.get_mysql_arm_id(arm_name, project_id)
        event_id = self.get_mysql_event_id(event_descrip, arm_id)
        sql = (
            "SELECT DISTINCT record "
            "FROM redcap.redcap_data AS rd "
            "WHERE rd.project_id = {0} "
            "AND rd.event_id = {1}".format(project_id, event_id)
        )
        if subject_id:
            sql += " AND rd.record = '{0}';".format(subject_id)
        else:
            sql += ";"

        return pd.read_sql_query(sql, self.api["redcap_mysql_db"])

    def delete_mysql_table_records(self, table_name, record_list):
        sql = (
            "DELETE FROM "
            + table_name
            + " WHERE "
            + table_name
            + ".ld_id IN ({0});".format(record_list)
        )
        execute(sql, self.api["redcap_mysql_db"])
        return len(record_list)

    def add_mysql_table_records(
        self,
        table_name,
        project_name,
        arm_name,
        event_descrip,
        name_of_form,
        record_list,
        outfile=None,
    ):
        # get the ids needed to lock the forms
        project_id = self.get_mysql_project_id(project_name)
        if not project_id:
            return -1

        arm_id = self.get_mysql_arm_id(arm_name, project_id)
        event_id = self.get_mysql_event_id(event_descrip, arm_id)

        len_list = len(record_list)
        user_name = self.__config_usr_data.get_category("redcap")["user"]

        project_id_series = [project_id] * len_list
        event_id_series = [event_id] * len_list
        form_name_series = [name_of_form] * len_list
        username_series = [user_name] * len_list
        additional_records = dict(
            project_id=project_id_series,
            record=record_list.record.tolist(),
            event_id=event_id_series,
            form_name=form_name_series,
            username=username_series,
            timestamp=datetime.datetime.now(),
        )

        dataframe = pd.DataFrame(data=additional_records)
        dataframe.to_sql(
            table_name, self.api["redcap_mysql_db"], if_exists="append", index=False
        )

        if outfile:
            dataframe.record.to_csv(outfile, index=False, header=False)

        return len(record_list)

    def svn_client(self):
        svn_laptop = self.api["svn_laptop"]
        if not svn_laptop:
            slog.info("session.svn_client", "ERROR: svn api is not defined")
            return None

        client = svn_laptop["client"]
        return client


if __name__ == "__main__":
    import argparse

    default = "default: %(default)s"
    formatter = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(
        prog="session.py",
        description="Call a specific function in session",
        formatter_class=formatter,
    )
    parser.add_argument(
        "-c",
        "--config",
        help="SIBIS config file. {}".format(default),
        default=os.environ.get("SIBIS_CONFIG"),
    )
    parser.add_argument("function_call", help="variable to get value for")
    argv = parser.parse_args()
    slog.init_log(False, False, "session", "session", None)
    sInstance = Session()
    sInstance.configure(argv.config)
    print(getattr(sInstance, argv.function_call)())
    sys.exit()
