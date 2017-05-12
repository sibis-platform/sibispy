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
import os
import time
import yaml
from sibispy import sibislogger as slog


class Session(object):
    """
    SIBIS Session Object
    ====================
    Main object that provides logging, data connections, and
    issues management.

    config_path: Path to sibis_config.yml.
                 Or set path as SIBIS_CONFIG environment variable
                 (default: ~/sibis-operations/sibis.config)
    connect: connects to all servers
             (default: None)
    """

    def __init__(self, config_path=None, initiate_slog=False):
        self.config = None
        self.config_path = config_path
        self.api = {'xnat': None, 'import_laptops' : None, 'import_webcnp' : None, 'data_entry' : None}    
        if initiate_slog :
            slog.init_log(False, False,'session.py', 'session')

    def configure(self):
        """
        Configures the session object by first checking for an
        environment variable, then in the home directory.
        """
        
        env = os.environ.get('SIBIS_CONFIG')
        if self.config_path:
            pass
        elif env:
            self.config_path = env
        else:
            cfg = os.path.join(os.path.expanduser('~'),
                               '.sibis-general-config.yml')
            self.config_path = cfg

        try:
            with open(self.config_path, 'r') as fi:
                self.config = yaml.load(fi)
        except IOError, err:
            slog.info('Configuring Session {}'.format(time.asctime()),
                              'No sibis_config.yml found: {}'.format(err),
                              env_path=env,
                              sibis_config_path=self.config_path)
            return None

        return self.config

    def connect_server(self,api_type):
        """
        Connect to servers, setting each property.
        """
        if api_type not in self.api :
            slog.info('connect_server','api type ' + api_type + ' not defined !',
                      api_types = str(api.keys()))
            return None
             
        if api_type == 'xnat' :
            return self.__connect_xnat__()

        return self.__connect_redcap__(api_type)

    def __connect_xnat__(self):
        import pyxnat
        cfg = self.config.get('xnat')
        try : 
            xnat = pyxnat.Interface(server=cfg.get('server'),
                                    user=cfg.get('user'),
                                    password=cfg.get('password'),
                                    cachedir=cfg.get('cachedir'))
        except Exception as err_msg: 
            self.info('Connect to xnat: {}'.format(time.asctime()), str(err_msg), server=cfg.get('server'))
            return None

        self.api['xnat'] = xnat
        return xnat

    def __connect_redcap__(self,api_type):
        import redcap
        import requests
        cfg = self.config.get('redcap')    
        try:
            data_entry = redcap.Project(cfg.get('server'),
                                        cfg.get(api_type + '_token'),
                                        verify_ssl=cfg.get('verify_ssl'))
            self.api[api_type] = data_entry

        except KeyError, err:
            slog.info('Connect to REDCap: {}'.format(time.asctime()),
                      '{}'.format(err),
                      server=cfg.get('server'),
                      api_type = api_type)
            return None 

        except requests.RequestException, err:
            slog.info('Connect to REDCap: {}'.format(time.asctime()),
                      '{}'.format(err),
                      server=cfg.get('server'),
                      api_type = api_type)
            return None

        return data_entry

    def get_log_dir(self):
        return self.config.get('logdir')

    def get_operations_dir(self):
        return self.config.get('operations')

    def xnat_select_general(self,form, fields, conditions): 
        if not self.api['xnat'] : 
            slog.info('xnat_select_general','Error: XNAT api not defined')  
            return None

        try:
            xnat_data = self.api['xnat'].select(form, fields).where(conditions).items()
        except Exception, err_msg:
            slog.info("xnat_select_general{}'.format(time.asctime())","ERROR: querying XNAT failed.",
                      form = str(form),
                      fields = str(fields),
                      conditions = str(conditions),
                      err_msg = str(err_msg))
            return None

        return xnat_data
