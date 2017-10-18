##
##  Copyright 2017 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##
"""
Parse Arguments From Config File 
"""
import os
import yaml
from collections import OrderedDict

def __ordered_load__(stream, Loader=yaml.Loader, object_pairs_hook=OrderedDict):
    class OrderedLoader(Loader):
        pass
    def construct_mapping(loader, node):
        loader.flatten_mapping(node)
        return object_pairs_hook(loader.construct_pairs(node))
    OrderedLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
        construct_mapping)
    return yaml.load(stream, OrderedLoader)


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
   
    def configure(self, config_file=None, ordered_load=False):
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
                if ordered_load:
                    self.__config_dict = __ordered_load__(fi, yaml.SafeLoader)
                else : 
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

    def keys(self):
        return self.__config_dict.keys()


    def get_config_file(self):
        return self.__config_file
    
    
