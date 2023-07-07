import pytest
import subprocess

# create a temporary output dir
@pytest.fixture
def hivalc_temp_out_dir(tmp_path):
    d = tmp_path / "ndar_output_dir"
    d.mkdir()
    return d

#fixture for hivalc script call
@pytest.fixture
def hivalc_script_call():
    hivalc_script = '/sibis-software/python-packages/sibispy/ndar_upload/ndar_create_indiv_csv.py'
    return hivalc_script

def test_help_output(hivalc_script_call):
    """Test that help output of script works"""
    completed_process = subprocess.run([hivalc_script_call, '-h'])
    assert completed_process.returncode == 0

def test_with_no_args(hivalc_script_call):
    """Test that process fails without any required args given"""
    completed_process = subprocess.run([hivalc_script_call])
    assert completed_process.returncode != 0

def test_full_call(hivalc_script_call, hivalc_temp_out_dir):
    """Test that full process works"""
    args = [
        '-v',
        '--ndar_dir', str(hivalc_temp_out_dir),
        'hivalc',
        '--subject',  'LAB_S01671',
        '--visit', '20220503_6886_05032022'
    ]
    completed_process = subprocess.run([hivalc_script_call] + args)
    assert completed_process.returncode == 0