import pytest
import subprocess

# create a temporary output dir
@pytest.fixture
def temp_out_dir(tmp_path):
    d = tmp_path / "ndar_output_dir"
    d.mkdir()
    return d

#fixture for script call
@pytest.fixture
def create_indiv_script_call():
    script = '/sibis-software/python-packages/sibispy/ndar_upload/ndar_create_indiv_csv.py'
    return script

def test_with_no_args(create_indiv_script_call):
    """Test that process fails without any required args given"""
    completed_process = subprocess.run([create_indiv_script_call])
    assert completed_process.returncode != 0

def test_general_help_output(create_indiv_script_call):
    """Test that help output of script works"""
    completed_process = subprocess.run([create_indiv_script_call, '-h'])
    assert completed_process.returncode == 0

def test_hivalc_help_output(create_indiv_script_call):
    """Test that the help output for hivalc source works"""
    args = ['hivalc', '-h']
    completed_process = subprocess.run([create_indiv_script_call] + args)
    assert completed_process.returncode == 0

def test_ncanda_help_output(create_indiv_script_call):
    """Test that the help output for ncanda source works"""
    args = ['ncanda', '-h']
    completed_process = subprocess.run([create_indiv_script_call] + args)
    assert completed_process.returncode == 0

def test_full_hivalc_call(create_indiv_script_call, temp_out_dir):
    """Test that full process works"""
    args = [
        '--ndar_dir', str(temp_out_dir),
        'hivalc',
        '--subject',  'LAB_S01671',
        '--visit', '20220503_6886_05032022'
    ]
    completed_process = subprocess.run([create_indiv_script_call] + args)
    assert completed_process.returncode == 0

def test_full_ncanda_call(create_indiv_script_call, temp_out_dir):
    """Test that full process works"""
    args = [
        '--ndar_dir', str(temp_out_dir),
        'ncanda',
        '--subject',  'NCANDA_S00169',
        '--release_year', '8',
        '--followup_year', '8'
    ]
    completed_process = subprocess.run([create_indiv_script_call] + args)
    assert completed_process.returncode == 0