from __future__ import absolute_import, print_function

import os, yaml


def pytest_addoption(parser):
    sibis = parser.getgroup('sibis', description='SIBIS specific test options', after='usage')
    sibis.addoption("--config-file", action="store", default=os.path.expanduser("~/.sibis-general-config.yml"),
                    help="Path to SIBIS General Configuration File")
    sibis.addoption("--cluster-job", action="store_true", default=False, help="Running as cluster job")
    sibis.addoption("--enable-special", nargs="*", choices=['sample_session', 'default_general_config'])


def pytest_generate_tests(metafunc):
    option_value = metafunc.config.option.config_file
    print("opt_val: >{0}<".format(option_value))
    with open(option_value, 'r') as f:
        general_cfg = yaml.safe_load(f)
        if 'config' in metafunc.fixturenames and general_cfg is not None:
            print("general_config: " + repr(general_cfg))
            metafunc.parametrize("config", [general_cfg])
    if 'config_file' in metafunc.fixturenames and general_cfg is not None:
        metafunc.parametrize("config_file", [option_value])

    option_value = metafunc.config.option.cluster_job
    if 'cluster_job' in metafunc.fixturenames and option_value is not None:
        metafunc.parametrize("cluster_job", [option_value])

    special_opts = metafunc.config.option.enable_special or ['none']

    if 'special_opts' in metafunc.fixturenames and special_opts is not None:
        metafunc.parametrize('special_opts', special_opts)


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "cluster_job(enabled): marks tests as cluster enabled. Run these tests with --cluster-job option."
    )
