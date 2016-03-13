##
##  Copyright 2016 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##
import json
import logging

import sibis


def test_logging_init():
    assert(isinstance(sibis.logging, sibis.logger.Logging))


def test_logging_info(caplog):
    caplog.setLevel(logging.INFO)
    sibis.logging.info('uid', 'message', key='value')
    for record in caplog.records():
        data = json.loads(record.message)
        assert(data.get('experiment_site_id') == 'uid')
        assert(data.get('error') == 'message')
        assert(data.get('key') == 'value')




