##
##  Copyright 2016 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##
import os

import sibis

path = os.path.join(os.path.dirname(__file__), 'data', 'sibis_config.yml')


def test_session_init_path():
    # setting explicitly
    session = sibis.Session(config_path=path)
    assert(session.sibis_config_path == path)


def test_session_init_env():
    os.environ.update(SIBIS_CONFIG=path)
    session = sibis.Session()
    os.environ.clear()
    assert(session.sibis_config_path == path)


def test_session_init_cfg():
    default = os.path.join(os.path.expanduser('~'),
                           '.sibis-operations', 'sibis_config.yml')
    session = sibis.Session()
    assert(session.sibis_config_path == default)