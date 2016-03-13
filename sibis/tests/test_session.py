##
##  Copyright 2016 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##
import os

import sibis


def test_session_init_path():
    path = os.path.join(os.path.expanduser('~'),
                        'sibis-config', 'sibis_config.yml')
    # setting explicitly
    session = sibis.Session(config_path=path)
    assert session.sibis_config_path == path


def test_session_init_env():
    path = os.path.join(os.path.expanduser('~'),
                        'sibis-config', 'sibis_config.yml')
    os.environ.update(SIBIS_CONFIG=path)
    session = sibis.Session()
    assert session.sibis_config_path == path


def test_session_init_cfg():
    path = os.path.join(os.path.expanduser('~'),
                        'sibis-config', 'sibis_config.yml')
    session = sibis.Session()
    assert session.sibis_config_path == path