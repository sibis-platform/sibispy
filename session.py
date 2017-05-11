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
import sys
import time
import yaml
from sibispy import sibislogger as slog
# import logger


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

    def __init__(self, config_path=None, connect=None, initiate_slog=False):
        self.config = None
        self.api_xnat = None
        self.api_data_entry = None
        self.api_import_laptops = None
        self.config_path = config_path
        if initiate_slog :
            slog.init_log(False, False,'session.py', 'session')

        self.configure()
        if connect:
            self.connect_servers()
            

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
            sys.exit(err)

        return self.config

    def connect_servers(self):
        """
        Connect to servers, setting each property.
        """
        self._connect_xnat()
        self._connect_redcap()

    def connect_xnat(self):
        import pyxnat
        cfg = self.config.get('xnat')
        xnat = pyxnat.Interface(server=cfg.get('server'),
                                user=cfg.get('user'),
                                password=cfg.get('password'),
                                cachedir=cfg.get('cachedir'))
        self.api_xnat = xnat

    def connect_redcap(self,project_entry=True,project_import=True):
        import redcap
        import requests
        cfg = self.config.get('redcap')
        try:
            if project_entry: 
                data_entry = redcap.Project(cfg.get('server'),
                                            cfg.get('data_entry_token'),
                                            verify_ssl=cfg.get('verify_ssl'))
                self.api_data_entry = data_entry

            if project_import :
                import_laptops = redcap.Project(cfg.get('server'),
                                                cfg.get('import_laptops_token'),
                                                verify_ssl=cfg.get('verify_ssl'))
                self.api_import_laptops = import_laptops

        except KeyError, err:
            slog.info('Connect to REDCap: {}'.format(time.asctime()),
                              '{}'.format(err),
                              server=cfg.get('server'))
            sys.exit(err)

        except requests.RequestException, err:
            slog.info('Connect to REDCap: {}'.format(time.asctime()),
                              '{}'.format(err),
                              server=cfg.get('server'))
            sys.exit(err)

    def get_log_dir(self):
        return self.config.get('logdir')

    def get_operations_dir(self):
        return self.config.get('operations')






