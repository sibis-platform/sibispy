from builtins import ExceptionGroup
import copy
import traceback
import pytest


from sibispy import utils
from sibispy import sibislogger as slog
from . import utils as test_utils

@pytest.fixture
def logger():
    '''
    Return a sibislogger instance initialized for a test session.
    '''
    slog.init_log(True, True,'test_try_export_records', 'testing', None)
    return slog

@pytest.fixture
def session(config_file):
    """
    Return a sibispy.Session configured by the provided config_file fixture.
    """
    return test_utils.get_session(config_file)

class MockError(Exception):
    ...

class MockRedcapProject():
    def __init__(self, num_errors_before_success=0, error_args={}, retval=True ):
        self.retval = retval
        self.error_args = error_args
        self.num_errors_before_success = num_errors_before_success
    

    def export_records(self, *args, **kwargs):
        if self.num_errors_before_success > 0:
            self.num_errors_before_success += -1
            if isinstance(self.error_args, Exception):
                raise copy.deepcopy(self.error_args)
            elif isinstance(self.error_args, dict):
                raise MockError(**self.error_args)
            else:
                raise MockError()
        else:
            return self.retval

        
@pytest.mark.parametrize("project,max_tries,timeout_secs,kwarg_dict", [
    (MockRedcapProject(0, retval=2), 1, 5, {}), # immediate success no failure
    (MockRedcapProject(2, retval=2), 3, 5, {"subject":"TEST123", "instruments":["one", "two", "three"]}), # success after two failures
    (MockRedcapProject(3, retval=2), 3, 5, {"subject":"TEST123", "instruments":["one", "two", "three"]}), # complete fail
    (MockRedcapProject(3, error_args=ValueError("Random parser error"), retval=2), 3, 5, {"subject":"TEST123", "instruments":["one", "two", "three"]}) # complete fail with custom error
])
def test_try_export_records(session, logger, project, max_tries, timeout_secs, kwarg_dict):

    if max_tries > project.num_errors_before_success:
        ret_val = utils.try_redcap_export_records(project, max_tries, timeout_secs, **kwarg_dict)
        assert ret_val == project.retval
    else:
        if isinstance(project.error_args, Exception):
            ex_type = type(project.error_args)
        else:
            ex_type = MockError
        
        ex_type = [ ex_type for x in range(project.num_errors_before_success)]

        with pytest.RaisesGroup(*ex_type) as ex_info:
            _ = utils.try_redcap_export_records(project, max_tries, timeout_secs, **kwarg_dict)

        for error in ex_info.value.exceptions:
            assert None is not error.__notes__

        traceback.print_exception(ex_info.value)
       