from abc import ABC, abstractmethod

from fabric import Connection, Result
from pathlib import Path
from paramiko.ssh_exception import SSHException

from sibispy import sibislogger as slog
from typing import Dict, Union
import hashlib
from datetime import datetime


class ClusterConfigError(ValueError):
    def __init__(self, msg, *args, **kwargs):
        self._msg = msg
        super().__init__(*args, **kwargs)

    @property
    def msg(self):
        return self._msg


class ClusterScheduler(ABC):
    def __init__(self, cluster_config):
        self.config = cluster_config
        self._validate_config()

    def _validate_config(self):
        for required in ['connection', 'base_cmd']:
            if required not in self.config.keys():
                raise ClusterConfigError(f"ClusterScheduler is missing configuration for {required}")

    @staticmethod
    def _get_connection(conn_cfg) -> Union[Connection, str, None]:
        if isinstance(conn_cfg, str):
            return conn_cfg
        elif 'host' in conn_cfg:
            host = conn_cfg['host']
            opts = {}
            if 'connect_kwargs' in conn_cfg:
                opts['connect_kwargs'] = conn_cfg['connect_kwargs']
            if 'gateway' in conn_cfg:
                opts['gateway'] = SlurmScheduler._get_connection(conn_cfg['gateway'])
            return Connection(host, **opts)
        else:
            return None

    @staticmethod
    def _make_bug_title(job_title :str, uniq_id: str):
        return job_title + "-" + hashlib.sha1(str(uniq_id).encode('utf-8')).hexdigest()

    def _get_cmd_options(self, extra_cmds: dict = {}) -> str:
        all_opts = {}
        all_opts.update(**extra_cmds)

        if "options" in self.config:
            all_opts.update(self.config["options"])

        opts = []
        for k, v in all_opts.items():
            opts.append(f"{k}={v}")
        return " ".join(opts)

    def get_connection(self) -> Connection:
        conn = self._get_connection(self.config['connection'])
        if conn is None or not isinstance(conn, Connection):
            raise ClusterConfigError("Connection configuration is missing from cluster_config")
        return conn

    @abstractmethod
    def get_command_str(self, job_title, job_log, job_script) -> str:
        """
        Abstract method that should be implemented that returns the command string that should run on the remote host

        :param job_title: Job Title
        :param job_log: Log file that should be located on a shared directory
        :param job_script: Shell commands to execute on a single line that will schedule the job.
        :return:
        """
        raise NotImplementedError("You must implement get_command_str")

    def schedule_job(self, job_script: str, job_title: str,
                     submit_log: Union[str, Path] = None, job_log: str = '/dev/null', verbose: bool = False) -> bool:
        """
        Schedule job on remote cluster
        :param job_script:
        :param job_title:
        :param submit_log:
        :param job_log:
        :param verbose:
        :return:
        """
        bug_title = self._make_bug_title(job_title, str(job_script))

        cmd_str = self.get_command_str(job_title, job_log, job_script)
        now_str = str(datetime.now())

        try:
            with self.get_connection() as conn:
                r: Result = conn.run(cmd_str, hide=True)

            if r.stderr and r.stderr != '':
                slog.info(self._make_bug_title(job_title, r.stderr), "ERROR: Failed to schedule job! " + now_str,
                          cmd_str=r.command, err_msg=r.stderr)
                return False, -1

            if verbose:
                if r:
                    print(f"cmd: {r.command}\n"
                          f"stdout: {r.stdout}\n")

        except SSHException as e:
            slog.info(bug_title, "ERROR: Failed to schedule job! " + now_str, cmd=cmd_str, err_msg=str(e))
            return False, -1

        if submit_log:
            with open(submit_log, 'a') as sl:
                sl.write(f"CONNECTION: {r.connection}\n")
                sl.write(f"CMD: {r.command}\n")
                sl.write(f"EXIT: {r.exited}\n")
                sl.write(f"STDOUT: {r.stdout}\n")
                sl.write(f"STDERR: {r.stderr}\n")

        job_id = int(r.stdout.split(' ')[-1])

        return True, job_id


class SlurmScheduler(ClusterScheduler):

    def get_command_str(self, job_title, job_log, job_script) -> str:
        slurm_opts = {
            "--job-name": f"'{job_title}'",
            "--output": job_log,
            "--wrap": f"'{job_script}'",
            "--open-mode": "append"
        }

        base_cmd = self.config['base_cmd']
        cmd_opts = self._get_cmd_options(slurm_opts)

        return f"{base_cmd} {cmd_opts}"
