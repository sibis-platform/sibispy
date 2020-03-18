import pytest
import os
import pwd
import re
from pathlib import Path
from . import utils
from shutil import rmtree

from sibispy.cluster_util import SlurmScheduler

@pytest.fixture
def session(config_file):
    """
    Return a sibispy.Session configured by the provided config_file fixture.
    """
    return utils.get_session(config_file)


@pytest.fixture
def test_config(session):
    return utils.get_test_config("test_cluster_util", session)


@pytest.fixture
def local_out_dir(test_config):
    odir = Path(test_config.get('local_out_dir', '/tmp'))
    rmtree(odir, ignore_errors=True)
    odir.mkdir(parents=True, exist_ok=True)
    return odir


@pytest.fixture
def shared_out_dir(test_config):
    return Path(test_config.get('shared_out_dir', '/tmp'))


class CurLogin(object):
    uid = property(lambda self: os.getuid())
    login = property(lambda self: pwd.getpwuid(self.uid)[0])


@pytest.fixture()
def cur_login() -> CurLogin:
    return CurLogin()


@pytest.fixture()
def cluster_config(test_config):
    return test_config['cluster_config']


def test_make_bugtitle():
    job_title = "My Job Title"
    uniq_id = "This is the dawning of the age of Aquarius"

    import hashlib
    sha = hashlib.sha1(uniq_id.encode('utf-8')).hexdigest()

    bug_title = SlurmScheduler._make_bug_title(job_title, uniq_id)
    assert bug_title.find(job_title) > -1, "Job title is missing from the bug title"
    assert bug_title.find(sha) > -1, "Job title is missing the hashed uniq identifier"


def test_slurm_make_connection(cluster_config):
    from fabric import Connection

    cfg = SlurmScheduler._get_connection(cluster_config['connection'])

    assert cfg, "connection should not be none"
    assert isinstance(cfg, Connection), "connection is not a fabric.Connection"


def test_slurm_get_command_str(cluster_config):
    job_title = "My Job Title"
    job_log = "some-job-file.log"
    job_script = "touch some-kind-of-file.txt; rm -f some-kind-of-file.txt;"

    slurm = SlurmScheduler(cluster_config)
    cmd_str = slurm.get_command_str(job_title, job_log, job_script)

    assert cmd_str and len(cmd_str) > 0, "Command should not be None"
    assert cmd_str.find(job_title) > -1, "Job title missing from command"
    assert cmd_str.find(job_log) > -1, "Job log missing from command"
    assert cmd_str.find(job_script) > -1, "Job script missing from command"


def test_slurm_make_cmd_opts(cluster_config):

    slurm = SlurmScheduler(cluster_config)

    cmd_opts = cluster_config['options']
    assert cmd_opts and isinstance(cmd_opts, dict)

    num_keys = len(cmd_opts.keys())

    opts = slurm._get_cmd_options()

    assert len(opts) > 0, "options should not be empty"

    opt_list = opts.split(" ")
    assert len(opt_list) == num_keys, "number of options returned does not match cluster_config"


@pytest.mark.cluster_job(True)
def test_slurm_schedule_job(capsys, monkeypatch, request, session, test_config, local_out_dir: Path, shared_out_dir: Path, cur_login: CurLogin):
    cluster_submit_log = local_out_dir / test_config['submit_log']
    cluster_job_log = shared_out_dir / test_config['job_log']

    if cluster_job_log.exists():
        cluster_job_log.unlink()

    cluster_test_file = shared_out_dir / test_config['case_file']
    user_id = os.getuid()
    user_name = pwd.getpwuid(user_id)[0]

    cmd_str = (f'echo "Submitting User: {cur_login.login} ({cur_login.uid})"; '
               f'echo "Executing User: ${{LOGNAME}} (${{UID}})"; '
               f'touch {cluster_test_file}; '
               f'rm -f {cluster_test_file}; ')

    # NOTE: fabric has an issue with pytest can when it captures output.  Disable output capture while using fabric

    slurm = SlurmScheduler(test_config['cluster_config'])
    with capsys.disabled():
        monkeypatch.setattr('sys.stdin', open('/dev/null'))
        assert slurm.schedule_job(cmd_str, request.module.__name__, cluster_submit_log, cluster_job_log, True)

    print(f"Please check {cluster_job_log} if cluster job was successfully executed !")