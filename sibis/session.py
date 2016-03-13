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

import logger


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

    def __init__(self, config_path=None, connect=None):
        self.config = None
        self.api_issues = None
        self.api_imaging = None
        self.api_data_entry = None
        self.api_import_laptops = None
        self.config_path = config_path
        self.logging = logger.Logging()

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
                               '.sibis-operations',
                               'sibis_config.yml')
            self.config_path = cfg

        try:
            with open(self.config_path, 'r') as fi:
                self.config = yaml.load(fi)
        except IOError, err:
            self.logging.info('Configuring Session {}'.format(time.asctime()),
                              'No sibis_config.yml found: {}'.format(err),
                              env_path=env,
                              sibis_config_path=self.config_path)
        return self.config

    def connect_servers(self):
        """
        Connect to servers, setting each property.
        """
        self._connect_xnat()
        self._connect_redcap()
        self._connect_github()

    def _connect_xnat(self):
        import pyxnat
        cfg = self.config.get('xnat')
        xnat = pyxnat.Interface(server=cfg.get('server'),
                                user=cfg.get('user'),
                                password=cfg.get('password'),
                                cachedir=cfg.get('cachedir'))
        self.api_imaging = xnat

    def _connect_redcap(self):
        import redcap
        import requests
        cfg = self.config.get('redcap')
        try:
            data_entry = redcap.Project(cfg.get('server'),
                                        cfg.get('data_entry_token'),
                                        verify_ssl=cfg.get('verify_ssl'))
            import_laptops = redcap.Project(cfg.get('server'),
                                            cfg.get('import_laptops_token'),
                                            verify_ssl=cfg.get('verify_ssl'))
            self.api_data_entry = data_entry
            self.api_import_laptops = import_laptops
        except KeyError, err:
            self.logging.info('Connect to REDCap: {}'.format(time.asctime()),
                              '{}'.format(err),
                              server=cfg.get('server'))
            sys.exit(err)

        except requests.RequestException, err:
            self.logging.info('Connect to REDCap: {}'.format(time.asctime()),
                              '{}'.format(err),
                              server=cfg.get('server'))
            sys.exit(err)

    def _connect_github(self):
        import github
        cfg = self.config.get('github')
        g = github.Github(cfg.get('user'),
                          cfg.get('password'))
        self.api_issues = g







