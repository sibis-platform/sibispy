import pytest
import subprocess
import pandas as pd

@pytest.fixture(scope="session")
def temp_dirs(tmp_path_factory):
    """
    Creates all needed temporary directories that will persist for session
    """
    indiv_csv_dir = tmp_path_factory.mktemp("tmp_ndar_dir") / "indiv_csv_dir"
    indiv_csv_dir.mkdir()
    upload2ndar_dir = tmp_path_factory.mktemp("temp_upload2ndar_dir")
    # upload2ndar_dir.mkdir()
    config_file = tmp_path_factory.mktemp("config") / ".sibis_general_config.yml"

    dirs = {
        "indiv_csv_dir": indiv_csv_dir,
        "upload2ndar_dir": upload2ndar_dir,
        "config_file": config_file,
    }
    return dirs

@pytest.fixture
def create_indiv_script_call():
    """fixture for indiv csv script call"""
    script = '/sibis-software/python-packages/sibispy/ndar_upload/ndar_create_indiv_csv.py'
    return script

@pytest.fixture
def indiv_csv_args(temp_dirs):
    indiv_args = [
        '--ndar_dir', str(temp_dirs['indiv_csv_dir']),
        'ncanda',
        '--subject',  'NCANDA_S00169',
        '--release_year', '8',
        '--followup_year', '8'
    ]
    return indiv_args

def test_ncanda_indiv_csv_call(create_indiv_script_call, indiv_csv_args):
    """Test that full individual csv creation process works"""
    completed_process = subprocess.run([create_indiv_script_call] + indiv_csv_args)
    assert completed_process.returncode == 0

@pytest.fixture
def ndar_indiv_csv_call(create_indiv_script_call, indiv_csv_args):
    """Indiv csv creation step to generate for movetoupload step"""
    completed_process = subprocess.run([create_indiv_script_call] + indiv_csv_args)
    return indiv_csv_args

@pytest.fixture
def move_to_upload2ndar_script_call():
    call = '/sibis-software/python-packages/sibispy/ndar_upload/move_to_upload2ndar.sh'
    return call

def test_move_to_upload2ndar(temp_dirs, move_to_upload2ndar_script_call):
    args = [
        'ncanda_upload_to_ndar',
        str(temp_dirs['indiv_csv_dir']) + '/NCANDA_S00169/followup_8y',
        '/fs/neurosci01/ncanda/releases/internal/followup_8y',
        str(temp_dirs['upload2ndar_dir']),
    ]
    completed_process = subprocess.run([move_to_upload2ndar_script_call] + args)
    assert completed_process.returncode == 0

@pytest.fixture
def create_summs_script_call():
    call = '/sibis-software/python-packages/sibispy/ndar_upload/ndar_create_summary_csv.py'
    return call

@pytest.fixture
def update_gen_config(temp_dirs):
    """
    Create a copy of sibis-gen-config templace and fill w/ temp dir locations.
    Returns file path to generated config file
    """
    config_template = pd.read_csv('/sibis-software/python-packages/sibispy/tests/data/.sibis-general-config.yml')

    # fill in the data for temp file locations

    #TODO: add the filling in and translaion
    config = config_template

    return config

def test_summ_csv(create_summs_script_call):
    """Currently only tests that script call works, not full script functionality"""
    full_args = [
        '-v',
        '-n',
        '-r',
        '--sibis_general_config', '/sibis-software/python-packages/sibispy/tests/data/.sibis-general-config.yml',
        'ncanda',
        '--subject', 'NCANDA_S00169',
        '--followup_year', '8',
    ]
    help_args = [
        '-v',
        '-n',
        '-r',
        'ncanda',
        '-h'
    ]
    completed_process = subprocess.run([create_summs_script_call] + help_args)
    assert completed_process.returncode == 0

@pytest.fixture
def upload_summs_script_call():
    call = '/sibis-software/python-packages/sibispy/ndar_upload/ndar_upload_summary.py'
    return call

def test_upload(upload_summs_script_call):
    help_args = [
        '-v',
        '-x',
        '-y',
        'ncanda',
        '-h'
    ]
    completed_process = subprocess.run([upload_summs_script_call] + help_args)
    assert completed_process.returncode == 0
