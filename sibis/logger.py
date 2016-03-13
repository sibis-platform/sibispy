##
##  Copyright 2016 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##

import sys
import json
import logging
import collections


class Logging(object):
    """
    SIBIS Logging Module
    """
    def __init__(self):
        self.logging = logging
        self.logging.basicConfig(level=logging.INFO, format='%(message)s')
        self.log = collections.OrderedDict()

    def info(self, uid, message, **kwargs):
        """
        Relpaces logging.info
        """
        self.log.update(experiment_site_id=uid,
                        error=message)
        self.log.update(kwargs)
        log = json.dumps(self.log)
        self.log.clear()
        return self.logging.info(log)
