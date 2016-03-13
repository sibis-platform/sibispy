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

import logger


class Session(object):
    def __init__(self, config_path=None):
        self.xnat_api = None
        self.redcap_api = None
        self.github_api = None
        self.sibis_home = None
        self.sibis_config = None
        self.sibis_config_path = config_path
        self.logging = logger.Logging()

        self.configure()

    def configure(self):
        """
        Configures the session object by first checking for an
        environment variable, then in the home directory.
        """
        env = os.environ.get('SIBIS_CONFIG')
        cfg = os.path.join(os.path.expanduser('~'),
                           '.sibis-operations',
                           'sibis_config.yml')
        if self.sibis_config_path:
            pass
        elif env:
            self.sibis_config_path = env
        elif cfg:
            self.sibis_config_path = cfg
        else:
            self.sibis_config_path = None

        try:
            with open(self.sibis_config_path, 'r') as fi:
                self.sibis_config = yaml.load(fi)
        except IOError, err:
            self.logging.info('Configuring Session {}'.format(time.asctime()),
                              'No sibis_config.yml found: {}'.format(err),
                              env_path=env,
                              cfg_path=cfg,
                              sibis_config_path=self.sibis_config_path)
        return self.sibis_config


