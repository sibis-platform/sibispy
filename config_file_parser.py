##
##  Copyright 2017 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##
"""
Parse Arguments From Config File 
"""
import os
import yaml

class config_file_parser(object):
    """
    config_file_parser Object
    ====================
    Main object that provides access to config file 
    config_file: yml file specifying configuration
                 Or set path as SIBIS_CONFIG environment variable
                 (default: default_config_file)
    assumes 
      import config_file_parser 
      cfp = config_file_parser() 
      err_msg = cfp.config()
      
    is called initially 

    """

    def __init__(self):
        self.__config_dict = None
        self.__config_file = None 
   
    def configure(self, config_file=None, ):
        """
        Configures the session object by first checking for an
        environment variable, then in the home directory.
        """

        if config_file :
            self.__config_file = config_file
        else :  
            env = os.environ.get('SIBIS_CONFIG')
            if env:
                self.__config_file = env
            else:
                self.__config_file = os.path.join(os.path.expanduser('~'), '.sibis-general-config.yml')

        try:
            with open(self.__config_file, 'r') as fi:
                self.__config_dict = yaml.load(fi)

        except IOError, err:
            return err

        return None

    def get_value(self,category,subject=None):
        cfg = self.__config_dict.get(category)
        if cfg and subject:
            return cfg.get(subject)
            
        return cfg


    def get_category(self,category):
        return self.__config_dict.get(category)

    def get_config_file(self):
        return self.__config_file
    
    
