##
##  See COPYING file distributed along with the package for the copyright and
## license terms
##

import sys
import json
import logging
import collections


class Logging(object):
    """Provides custom issue logging format.

    Returns:
        A sibis.logger.Logging object.
    """
    def __init__(self):
        self.logging = logging
        self.logging.basicConfig(level=logging.INFO, format='%(message)s')
        self.log = collections.OrderedDict()

    def info(self, uid, message, **kwargs):
        """Relpaces logging.info

        Args:
            uid (str): A unique identifier for issue.
            message (str): The error message to report.

        Returns:
            A str serialized json object.
        """
        self.log.update(experiment_site_id=uid,
                        error=message)
        self.log.update(kwargs)
        log = json.dumps(self.log)
        self.log.clear()
        return self.logging.info(log)
