##
##  See COPYING file distributed along with the package for the copyright and
##  license terms
##
import os

import requests

import sibis

path = os.path.join(os.path.dirname(__file__), 'data', 'sibis_config.yml')


def test_session_init_path():
    # setting explicitly
    session = sibis.Session(config_path=path)
    assert(session.config_path == path)


def test_session_init_env():
    os.environ.update(SIBIS_CONFIG=path)
    session = sibis.Session()
    os.environ.clear()
    assert(session.config_path == path)


def test_session_init_cfg():
    default = os.path.join(os.path.expanduser('~'),
                           '.sibis-operations', 'sibis_config.yml')
    session = sibis.Session()
    assert(session.config_path == default)


def test_session_configure():
    truth = '/home/ubuntu/.sibis-operations'
    session = sibis.Session(config_path=path)
    assert(session.config.get('operations') == truth)


def test_session_connect_servers():
    session = sibis.Session(config_path=path)
    try:
        session.connect_servers()
    except SystemExit, err:
        assert(isinstance(err.message, requests.RequestException))
