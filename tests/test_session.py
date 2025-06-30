#!/usr/bin/env python

##
##  Copyright 2016 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##

# if test script is run with argument then it will run script with the sibis config file defined by that argument
# for example test_session.py ~/.sibis-general-config.yml
# otherwise will run with data/.sibis-general-config.yml

from __future__ import absolute_import
from __future__ import print_function
from builtins import str
import os
import pytest
import shutil
import tempfile
import warnings
from sibispy import session as sess
from . import utils
import pandas as pd


@pytest.fixture
def session(config_file):
    """
    Return a sibispy.Session configured by the provided config_file fixture.
    """
    return utils.get_session(config_file)


@pytest.fixture
def slog():
    """
    Return a sibislogger instance initialized for a test session.
    """
    from sibispy import sibislogger as slog

    timeLogFile = "/tmp/test_session-time_log.csv"
    if os.path.isfile(timeLogFile):
        os.remove(timeLogFile)

    slog.init_log(False, False, "test_session", "test_session", "/tmp")
    return slog


@pytest.fixture
def sys_file_parser(session):
    # Load in test specific settings :
    (parser, err_msg) = session.get_config_test_parser()
    assert err_msg is None, "Error: session.get_config_test_parser:" + err_msg

    return parser


@pytest.fixture
def config_test_data(sys_file_parser):
    config_test_data = sys_file_parser.get_category("test_session")
    if not config_test_data:
        warnings.warn(
            UserWarning("Warning: test_session specific settings not defined!")
        )
        config_test_data = dict()
    return config_test_data


#
# short sample script
#
def test_short_sample_script(special_opts):
    if "sample_session" != special_opts:
        pytest.skip("Test not enabled.")
    import sibispy
    from sibispy import sibislogger as slog

    slog.init_log(False, False, "test_session", "test_session", "/tmp")

    session = sibispy.Session()
    session.configure()
    server = session.connect_server("data_entry", True)
    assert server != None, "server should not be None"


def test_config_test_parser(session):
    parser, error = session.get_config_test_parser()
    assert error == None, "There should not be an error: {}".format(error)
    assert parser != None, "Config Parser for test should not be None."

    category = parser.get_category("test_session")
    assert category, "`test_session` category should exist."

    cfg = category["test_config_parser"]
    assert cfg != None, "Test config should exist."
    assert cfg["alpha"] == 12345678, "Values should match"
    assert cfg["bravo"] == "http://example.com"
    assert cfg["charlie"] == ["uno", "dos", "tres", "cuarto", "cinco"]
    assert cfg["denver"]["California"] == "Sacramento"
    assert cfg["denver"]["Colorado"] == "Denver"
    assert cfg["denver"]["Texas"] == "Austin"


def test_config_sys_parser(session):
    (sys_file_parser, err_msg) = session.get_config_sys_parser()
    assert not err_msg, "Error: session.get_config_sys_parser:" + err_msg
    assert sys_file_parser, "Error: `sys_file_parser` should not be None"

    session_data = sys_file_parser.get_category("session")
    assert session_data, "`session_data` should not be None"


@pytest.mark.parametrize("api_type", ["svn_laptop"])
@pytest.mark.svn_laptop
def test_connect_server(session, api_type):
    connection = session.connect_server(api_type)
    assert connection != None, "Expected to have a connection"


@pytest.mark.xnat
def test_session_xnat_export_general(session, slog):
    project = "xnat"
    server = session.connect_server(project, True)

    xnat_sessions_fields = [
        "xnat:mrSessionData/SESSION_ID",
        "xnat:mrSessionData/SUBJECT_ID",
        "xnat:mrSessionData/PROJECTS",
        "xnat:mrSessionData/DATE",
        "xnat:mrSessionData/SCANNER",
    ]

    xnat_sessions_list = session.xnat_export_general(
        "xnat:mrSessionData",
        xnat_sessions_fields,
        [("xnat:mrSessionData/SESSION_ID", "LIKE", "%")],
        "subject_session_data",
    )
    assert xnat_sessions_list != None
    xnat_sessions_dict = dict()
    for (session_id, session_subject_id, projects, date, scanner) in xnat_sessions_list:
        xnat_sessions_dict[session_id] = (date, scanner, projects)

@pytest.mark.svn_laptop
def test_session_api_svn_laptop(session, config):
    laptop_cfg = config["svn_laptop"]
    if laptop_cfg == None:
        warnings.warn(UserWarning("Expected config file to have `svn_laptop` config"))
        pytest.skip("Skipping test, `svn_laptop` config missing.")

    connected_client = session.connect_server("svn_laptop")

    client_info = session.api["svn_laptop"]
    assert client_info != None, "Expected client_info to not be None"

    user = client_info["user"]
    assert (
        user == laptop_cfg["user"] and user != None
    ), "Expected user to be {} and not be None. got: {} ".format(
        laptop_cfg["user"], user
    )

    password = client_info["password"]
    assert (
        password == laptop_cfg["password"] and password != None
    ), "Expected password to be {} and not be None. got: {} ".format(
        laptop_cfg["password"], password
    )

    client = client_info["client"]
    assert client != None, "Expected client to not be None"
    assert connected_client == client, "Clients should be the same object"


@pytest.mark.svn_laptop
def test_session_connect_server_info(session):
    connected_client = session.connect_server("svn_laptop")
    svn_info = connected_client.info()
    assert svn_info != None, "Response sholuld not be None"
    assert (
        svn_info["wcinfo_wcroot_abspath"] == session.get_laptop_svn_dir()
    ), "SVN Working directories should match."


@pytest.mark.xnat
def test_session_xnat_non_empty_query(slog, config_file, session):
    project = "xnat"
    server = session.connect_server(project, True)

    assert server, (
        "Error: could not connect server! Make sure "
        + project
        + " is correctly defined in "
        + config_file
    )

    # 1. XNAT Test: Non-Empty querry
    with sess.Capturing() as xnat_output:
        searchResult = session.xnat_export_general(
            "xnat:subjectData",
            [
                "xnat:subjectData/SUBJECT_LABEL",
                "xnat:subjectData/SUBJECT_ID",
                "xnat:subjectData/PROJECT",
            ],
            [("xnat:subjectData/SUBJECT_LABEL", "LIKE", "%")],
            "subject_list",
        )

    if '"err_msg": "Apache Tomcat' in xnat_output.__str__():
        warnings.warn(
            UserWarning(
                "Info: username or password might be incorrect - check crudentials by using them to manually log in XNAT! "
            )
        )

    assert (
        xnat_output.__str__() == "[]"
    ), "Error: session.xnat_export_general: failed to perform querry. Got: {}".format(
        xnat_output.__str__()
    )

    assert (
        searchResult != None
    ), "Error: session.xnat_export_general: Test returned empty record"

@pytest.mark.xnat
def test_session_xnat_get_experiment(slog, config_file, session, config_test_data):
    project = "xnat"
    server = session.connect_server(project, True)

    assert server, (
        "Error: could not connect server! Make sure "
        + project
        + " is correctly defined in "
        + config_file
    )
    #
    # xnat_get_experiment
    #
    eid = r"DOES-NOT-EXIST"
    with sess.Capturing() as xnat_output:
        exp = session.xnat_get_experiment(eid)

    assert None == exp, (
        "Error: session.xnat_get_experiment: " + eid + " should not exist!"
    )

    if "xnat_uri_test" in list(config_test_data):
        [project, subject, eid] = config_test_data["xnat_uri_test"].split(",")
        experiment = session.xnat_get_experiment(eid)
        assert None != experiment, (
            "Error: session.xnat_get_experiment: " + eid + " should exist!"
        )

        # Difference in the call - which one you use will decide where data is stored on hard drive !
        print("URI direct:", experiment.resources["nifti"].uri)

        experiment = session.xnat_get_experiment(
            eid, project=project, subject_label=subject
        )
        assert (
            None != experiment
        ), "Error: session.xnat_get_experiment: {} should exist in project {} and subject {} !".format(
            eid, project, subject
        )

        print("URI with subject:", experiment.resources["nifti"].uri)

        # zip_path="/tmp/tmpQcABtX/1_ncanda-localizer-v1.zip"
        # file_path= exp.resource('nifti')._uri
        # if not os.path.exists(file_path) :
        #        print "Error: xnat configuration wrong !" + file_path + " does not exist !"
        # server.select.project(project).subject(subject).experiment(eid).resource('nifti').put_zip(zip_path, overwrite=True,extract=True)
    else:
        warnings.warn(
            RuntimeWarning("Warning: Skipping XNAT uri test as it is not defined")
        )

@pytest.mark.xnat
def test_session_xnat_stress_test(slog, config_file, session, config_test_data):
    project = "xnat"
    client = session.connect_server(project, True)
    #
    # Stress Test:
    #

    if "xnat_stress_test" in list(config_test_data):
        [xnat_eid, resource_id, resource_file_bname] = config_test_data[
            "xnat_stress_test"
        ].split("/")
        tmpdir = tempfile.mkdtemp()

        print("Start XNAT stress test ...")
        slog.startTimer2()
        # If fails, MIKE solution
        target_file = os.path.join(tmpdir, "blub.tar.gz")
        client.download_file(xnat_eid, resource_id, resource_file_bname, target_file)
        assert os.path.exists(target_file) and os.path.isfile(
            target_file
        ), "Expected file `{}` to download.".format(target_file)
        slog.takeTimer2("XNATStressTest", "XNAT Stress Test")
        print("... completed")

        shutil.rmtree(tmpdir)
    else:
        warnings.warn(
            RuntimeWarning("Warning: Skipping XNAT stress test as it is not defined")
        )

@pytest.mark.xnat
def test_session_xnat_failed_query(slog, config_file, session, config_test_data):
    project = "xnat"
    server = session.connect_server(project, True)

    test_data = config_test_data["xnat_subject_attribute_test"]

    # 3. XNAT Test: Failed querry
    response = (1, None)
    with sess.CapturingTee() as xnat_output:
        response = session.xnat_get_subject_attribute("blub", "blub", "blub")

    assert response[0] == None, "Expected no attribute, got {}".format(response[0])
    assert response[1] != None, "Expected Error, got None!"

    if (
        "ERROR: session.xnat_get_subject_attribute: subject"
        not in xnat_output.__str__()
    ):
        print(
            "Error: session.xnat_get_subject_attribute: Test returned wrong error message"
        )
        print(xnat_output.__str__())

    response = (1, None)
    with sess.CapturingTee() as xnat_output:
        response = session.xnat_get_subject_attribute(
            test_data["project"], "blub", "blub"
        )

    assert response[0] == None, "Expected no attribute, got {}".format(response[0])
    assert response[1] != None, "Expected Error, got None!"

    if (
        "ERROR: session.xnat_get_subject_attribute: subject"
        not in xnat_output.__str__()
    ):
        print(
            "Error: session.xnat_get_subject_attribute: Test returned wrong error message"
        )
        print(xnat_output.__str__())

    response = (1, None)
    with sess.CapturingTee() as xnat_output:
        response = session.xnat_get_subject_attribute(
            test_data["project"], test_data["subject"], "blub"
        )

    assert response[0] == None, "Expected no attribute, got {}".format(response[0])
    assert response[1] != None, "Expected Error, got None!"

    if "ERROR: attribute could not be found" not in xnat_output.__str__():
        print(
            "Error: session.xnat_get_subject_attribute: Test returned wrong error message"
        )
        print(xnat_output.__str__())

    response = (1, None)
    with sess.CapturingTee() as xnat_output:
        response = session.xnat_get_subject_attribute(
            test_data["project"], test_data["subject"], "label"
        )

    assert response[1] == None, "Expected there to be no errors. Got: {}".format(
        response[1]
    )
    assert response[0] != None, "Expected response, got Nothing."

    response = (1, None)
    with sess.CapturingTee() as xnat_output:
        response = session.xnat_get_subject_attribute(
            test_data["project"], test_data["subject"], "ID"
        )

    assert response[1] == None, "Expected there to be no errors. Got: {}".format(
        response[1]
    )
    assert response[0] != None, "Expected response, got Nothing."


@pytest.mark.xnat
def test_session_xnat_session_address(slog, session):
    experiment_id = "NCANDA_E00000"
    test_link = f"{session.get_xnat_server_address()}/data/experiments/{experiment_id}?format=html"
    assert session.get_xnat_session_address(experiment_id, "html") == test_link


@pytest.mark.xnat
def test_session_xnat_search(slog, config_file, session, config_test_data):

    client = session.connect_server("xnat", True)

    subject_project_list = list(
        client.search(
            "xnat:subjectData",
            [
                "xnat:subjectData/SUBJECT_LABEL",
                "xnat:subjectData/SUBJECT_ID",
                "xnat:subjectData/PROJECT",
            ],
        )
        .where([("xnat:subjectData/SUBJECT_LABEL", "LIKE", "%")])
        .items()
    )
    assert subject_project_list != None, "Search result should not be None."


@pytest.mark.redcap_db
@pytest.mark.redcap_data_entry
def test_session_event_descrip_from_redcap_event_name(slog, session):
    session.connect_server("data_entry", True)
    session.connect_server("redcap_mysql_db", True)
    event_descrips = pd.read_sql_table(
        "redcap_events_metadata", session.api["redcap_mysql_db"]
    )["descrip"].to_list()

    entry_data = session.redcap_export_records_from_api(
        time_label=None,
        api_type="data_entry",
        fields=["study_id"],
        event_name="unique",
        format_type="df",
        export_data_access_groups=False,
    ).reset_index()
    redcap_event_names = entry_data["redcap_event_name"].to_list()
    for redcap_event_name in redcap_event_names:
        event_descrip = session.get_event_descrip_from_redcap_event_name(
            redcap_event_name
        )
        assert (
            event_descrip in event_descrips
        ), f"{event_descrip} incorrect event descrip for {redcap_event_name}"


@pytest.mark.redcap_data_entry
def test_session_formattable_redcap_form_address(slog, session):
    session.configure(ordered_config_load_flag=True)
    redcap_project = session.connect_server("data_entry", True)
    assert redcap_project, "Failed to connect to data_entry"
    project_id = redcap_project.export_project_info()["project_id"]
    arm_num = 1
    visit = "7y_visit"
    redcap_event_name = f"{visit}_arm_{arm_num}"
    event_id = 493
    subject_id = "A-00002-F-2"
    form_name = "clinical"
    version = str(session.get_redcap_version())
    test_link = f"{session.get_redcap_base_address()}redcap_v{version}/DataEntry/index.php?pid={project_id}&id={subject_id}&event_id={event_id}&page={form_name}"

    # Test formatting when passing neither subject_id nor form_name
    formattable_address = session.get_formattable_redcap_form_address(
        project_id, redcap_event_name
    )
    formatted_address = formattable_address % (subject_id, form_name)
    assert formatted_address == test_link

    # Test passing only subject_id
    missing_form = session.get_formattable_redcap_form_address(
        project_id, redcap_event_name, subject_id=subject_id
    )
    formatted_address = missing_form % (form_name)
    assert formatted_address == test_link

    # Test passing only subject_id
    missing_sid = session.get_formattable_redcap_form_address(
        project_id, redcap_event_name, subject_id=None, name_of_form=form_name
    )
    formatted_address = missing_sid % (subject_id)
    assert formatted_address == test_link

    # Test passing both directly to function
    complete_address = session.get_formattable_redcap_form_address(
        project_id, redcap_event_name, subject_id, form_name
    )
    assert complete_address == test_link


@pytest.mark.redcap_data_entry
def test_session_formattable_redcap_subject_address(slog, session):
    redcap_project = session.connect_server("data_entry", True)
    assert redcap_project, "Failed to connect to data_entry"
    project_id = redcap_project.export_project_info()["project_id"]
    arm_num = 1
    subject_id = "A-00002-F-2"
    version = str(session.get_redcap_version())
    test_link = f"{session.get_redcap_base_address()}redcap_v{version}/DataEntry/record_home.php?pid={project_id}&arm={arm_num}&id={subject_id}"

    # Test formatting when not passing subject_id
    formattable_address = session.get_formattable_redcap_subject_address(
        project_id, arm_num
    )

    formatted_address = formattable_address % (subject_id)
    assert formatted_address == test_link

    # Test passing subject_id directly to function
    complete_address = session.get_formattable_redcap_subject_address(
        project_id, arm_num, subject_id
    )
    assert complete_address == test_link


#@pytest.fixture
def penncnp_cleanup(session):
    yield
    session.disconnect_penncnp()


#@pytest.mark.browser_penncnp
@pytest.mark.skip ("obsolete")
def test_session_browser_penncnp(
    slog, config_file, session, config_test_data, penncnp_cleanup
):
    project = "browser_penncnp"
    with session.connect_server(project, True) as server:
        assert server != None, (
            "Error: could not connect server! Make sure "
            + project
            + " is correctly defined in "
            + config_file
        )

        wait = session.initialize_penncnp_wait()
        assert session.get_penncnp_export_report(
            wait
        ), "Error: could not export report."


#@pytest.mark.browser_penncnp
@pytest.mark.skip ("obsolete")
def test_penncnp_exits(slog, config_file, session, config_test_data, penncnp_cleanup):
    project = "browser_penncnp"
    with session.connect_server(project, True) as server:
        pass
    with session.connect_server(project, True) as server2:
        assert (
            server2 != None
        ), "Error: could not connect second server! Make sure the selenium process exits correctly."


def test_session_legacy(config_file, special_opts):
    import os
    import glob
    import pandas as pd
    import sys
    import sibispy
    import traceback
    from sibispy import sibislogger as slog
    from sibispy import config_file_parser as cfg_parser
    import tempfile
    import shutil

    #
    # MAIN
    #

    # if sys.argv.__len__() > 1 :
    #     config_file = sys.argv[1]
    # else :
    #     config_file = os.path.join(os.path.dirname(sys.argv[0]), 'data', '.sibis-general-config.yml')
    if special_opts == "default_general_config":
        config_file = os.path.join(
            os.path.dirname(sys.argv[0]), "data", ".sibis-general-config.yml"
        )

    timeLogFile = "/tmp/test_session-time_log.csv"
    if os.path.isfile(timeLogFile):
        os.remove(timeLogFile)

    slog.init_log(False, False, "test_session", "test_session", "/tmp")

    session = sess.Session()
    assert session.configure(
        config_file
    ), "Configuration File `{}` is missing or not readable.".format(config_file)

    errors = False

    # Check that the file infrastructure is setup correctly
    for DIR in [
        session.get_log_dir(),
        session.get_operations_dir(),
        session.get_cases_dir(),
        session.get_summaries_dir(),
        session.get_dvd_dir(),
        session.get_datadict_dir(),
    ]:
        if not os.path.exists(DIR):
            print("ERROR: " + DIR + " does not exist!")
            errors = True

    for DIR in [
        session.get_laptop_imported_dir(),
        session.get_laptop_svn_dir(),
        session.get_xnat_dir(),
        session.get_redcap_uploads_dir(),
    ]:
        if not os.path.exists(DIR):
            print("ERROR: " + DIR + " does not exist! Ignore if this is back end")
            errors = True

    # Make sure directories are assigned to the correct user
    user_id = os.getuid()
    for DIR in [session.get_laptop_imported_dir(), session.get_laptop_svn_dir()]:
        path_uid = os.stat(DIR).st_uid
        if user_id != path_uid:
            print(
                "ERROR: Dir '" + DIR + "' owned by user with id",
                path_uid,
                " and not user running the script (id: " + str(user_id) + ")",
            )
            errors = True

    for DIR in glob.glob(os.path.join(session.get_log_dir(), "*")):
        path_uid = os.stat(DIR).st_uid
        if user_id != path_uid:
            print(
                "ERROR: Dir '" + DIR + "' owned by user with id",
                path_uid,
                " and not user running the script (id: " + str(user_id) + ")",
            )
            errors = True

    bDir = session.get_beta_dir()
    if os.path.exists(bDir):
        perm = os.stat(bDir).st_mode & 0o777
        if perm != 0o777:
            print("ERROR: Permission of " + bDir + " have to be 777 !")
            errors = True
    else:
        print("ERROR: " + bDir + " does not exist!")
        errors = True

    # Load in test specific settings :
    (sys_file_parser, err_msg) = session.get_config_test_parser()
    if err_msg:
        print("Error: session.get_config_test_parser:" + err_msg)
        errors = True

    if errors:
        assert not errors, "Errors occured, see output."

    config_test_data = sys_file_parser.get_category("test_session")
    if not config_test_data:
        warnings.warn(
            UserWarning("Warning: test_session specific settings not defined!")
        )
        config_test_data = dict()

    # Check that the servers are accessible
    with sess.Capturing() as xnat_output:
        assert session.xnat_get_subject_attribute("blub", "blub", "blub")[0] == None

    assert "Error: XNAT api not defined" in xnat_output.__str__(), (
        "Error: session.xnat_get_subject_attribute: Test did not return correct error message\n"
        + xnat_output.__str__()
    )

    for project in [
        "xnat",
        "xnat_http",
        "svn_laptop",
        "data_entry",
        #"browser_penncnp",
        "import_laptops",
        "redcap_mysql_db",
    ]:
        print("==== Testing " + project + " ====")
        try:
            server = session.connect_server(project, True)

            if not server:
                print(
                    "Error: could not connect server! Make sure "
                    + project
                    + " is correctly defined in "
                    + config_file
                )
                errors = True
                continue

            if project == "xnat_http":

                experiments = session.xnat_http_get_all_experiments()

                if not experiments or experiments.text.count("\n") < 2:
                    print(
                        "Error: session.xnat_http_get_all_experiments: failed to perform querry"
                    )
                    errors = True
                    continue

                experiment_id = (
                    experiments.text.splitlines()[1].split(",")[0].replace('"', "")
                )
                exp_html = session.xnat_http_get_experiment_xml(experiment_id).text
                if not [x for x in exp_html.splitlines() if "xnat:MRSession" in x]:
                    print(
                        "Error: session.xnat_http_get_experiment_xml: failed to perform querry"
                    )
                    errors = True
                    continue

            if project == "xnat":
                print("XNAT Tests moved to different test methods")
                # # 1. XNAT Test: Non-Empty querry
                # with sess.Capturing() as xnat_output:
                #     searchResult = session.xnat_export_general( 'xnat:subjectData', ['xnat:subjectData/SUBJECT_LABEL', 'xnat:subjectData/SUBJECT_ID','xnat:subjectData/PROJECT'], [ ('xnat:subjectData/SUBJECT_LABEL','LIKE', '%')],"subject_list")

                # if xnat_output.__str__() != '[]' :
                #     errors = True
                #     print("Error: session.xnat_export_general: failed to perform querry")
                #     if '"err_msg": "Apache Tomcat' in xnat_output.__str__():
                #         warnings.warn(UserWarning("Info: username or password might be incorrect - check crudentials by using them to manually log in XNAT! "))

                #     print(xnat_output.__str__())

                # elif searchResult == None :
                #     print("Error: session.xnat_export_general: Test returned empty record")
                #     errors = True

                # #
                # # xnat_get_experiment
                # #
                # eid = "DOES-NOT-EXIST"
                # with sess.Capturing() as xnat_output:
                #     exp = session.xnat_get_experiment(eid)

                # if exp :
                #     print("Error: session.xnat_get_experiment: " + eid + " should not exist!")
                #     errors = True

                # if "xnat_uri_test" in list(config_test_data):
                #     [project,subject,eid] = config_test_data["xnat_uri_test"].split(',')
                #     experiment = session.xnat_get_experiment(eid)
                #     if not experiment :
                #         print("Error: session.xnat_get_experiment: " + eid + " should exist!")
                #         errors = True
                #     else :
                #         # Difference in the call - which one you use will decide where data is stored on hard drive !
                #         print("URI direct:", experiment.resource('nifti')._uri)

                #     experiment = session.xnat_get_experiment(eid,project = project,subject_label = subject)
                #     if not experiment :
                #         print("Error: session.xnat_get_experiment: " + eid + " should exist in project", project, "and subject ", subject_label, "!")
                #         errors = True
                #     else :
                #         print("URI with subject:", experiment.resource('nifti')._uri)

                #     # zip_path="/tmp/tmpQcABtX/1_ncanda-localizer-v1.zip"
                #     #file_path= exp.resource('nifti')._uri
                #     #if not os.path.exists(file_path) :
                #     #        print "Error: xnat configuration wrong !" + file_path + " does not exist !"
                #     # server.select.project(project).subject(subject).experiment(eid).resource('nifti').put_zip(zip_path, overwrite=True,extract=True)
                # else :
                #     warnings.warn(RuntimeWarning("Warning: Skipping XNAT uri test as it is not defined"))

                # #
                # # Stress Test:
                # #

                # if "xnat_stress_test" in list(config_test_data) :
                #     [xnat_eid, resource_id, resource_file_bname] = config_test_data["xnat_stress_test"].split('/')
                #     tmpdir = tempfile.mkdtemp()

                #     print("Start XNAT stress test ...")
                #     slog.startTimer2()
                #     # If fails, MIKE solution
                #     server.select.experiment(xnat_eid).resource(resource_id).file(resource_file_bname).get_copy(os.path.join(tmpdir, "blub.tar.gz"))
                #     slog.takeTimer2("XNATStressTest","XNAT Stress Test")
                #     print("... completed")

                #     shutil.rmtree(tmpdir)
                # else :
                #     warnings.warn(RuntimeWarning("Warning: Skipping XNAT stress test as it is not defined"))

                # # 3. XNAT Test: Failed querry
                # with sess.Capturing() as xnat_output:
                #     assert session.xnat_get_subject_attribute('blub','blub','blub')[0] == None

                # if "ERROR: attribute could not be found" not in xnat_output.__str__():
                #     print("Error: session.xnat_get_subject_attribute: Test returned wrong error message")
                #     print(xnat_output.__str__())
                #     errors = True

                # # no xnat tests after this one as it breaks the interface for some reason
                server = None

            elif project == "svn_laptop":
                print("== Only works for frontend right now ! ==")

                svn_client = session.svn_client()
                assert svn_client, "Client should not be None"

                svn_info = svn_client.info()
                assert svn_info, "Info should not be None"

                # To speed up test
                lapDir = session.get_laptop_svn_dir()
                svn_dir = [
                    name
                    for name in os.listdir(lapDir)
                    if name != ".svn" and os.path.isdir(os.path.join(lapDir, name))
                ][0]
                # and now test
                assert svn_client.log(rel_filepath=svn_dir)

            elif project == "browser_penncnp":
                print("browser_penncnp Tests moved to different test methods")
                # wait = session.initialize_penncnp_wait()
                # assert session.get_penncnp_export_report(wait)

            elif project == "import_laptops":
                if "redcap_version_test" in list(config_test_data):
                    (form_prefix, name_of_form) = config_test_data[
                        "redcap_version_test"
                    ].split(",")
                    complete_label = "%s_complete" % name_of_form
                    exclude_label = "%s_exclude" % form_prefix

                    # If test fails Mike with message that redord_id is missing than it uses wrong redcap lib - use egg version
                    import_complete_records = server.export_records(
                        fields=[complete_label, exclude_label], format_type="df"
                    )
                else:
                    warnings.warn(
                        RuntimeWarning(
                            "Warning: Skipping REDCap version test as it is not defined"
                        )
                    )

            elif project == "data_entry":
                print("Testing REDCap Version:", session.get_redcap_version())

                form_event_mapping = server.export_instrument_event_mappings(format_type="df")
                assert not form_event_mapping.empty
                # Note: the name of form_name is version-independent in mysql tables - see for example server.metadata
                assert session.get_redcap_form_key() in form_event_mapping
                assert len(
                    server.export_records(
                        fields=["study_id"], event_name="unique", format_type="df"
                    )
                )

                if "redcap_stress_test" in list(config_test_data):
                    all_forms = config_test_data["redcap_stress_test"]
                    form_prefixes = list(all_forms.keys())
                    names_of_forms = list(all_forms.values())

                    entry_data_fields = (
                        [("%s_complete" % form) for form in names_of_forms]
                        + [("%s_missing" % form) for form in form_prefixes]
                        + [("%s_record_id" % form) for form in form_prefixes]
                    )
                    entry_data_fields += [
                        "study_id",
                        "dob",
                        "redcap_event_name",
                        "visit_date",
                        "exclude",
                        "sleep_date",
                    ]
                    entry_data_fields += ["parentreport_manual"]
                    print("Start REDCap stress test ...")
                    slog.startTimer2()
                    # If tests fails, Mike
                    session.redcap_export_records(
                        "RCStressTest",
                        fields=entry_data_fields,
                        event_name="unique",
                        format_type="df",
                    )
                    slog.takeTimer2("RCStressTest", "REDCap Stress Test")
                    print(".... completed")
                else:
                    warnings.warn(
                        RuntimeWarning(
                            "Warning: Skipping REDCap stress test as it is not defined"
                        )
                    )

            elif project == "redcap_mysql_db":
                pd.read_sql_table("redcap_projects", server)
                # more detailed testing in test_redcap_locking_data

        except AssertionError as ae:
            _, _, tb = sys.exc_info()
            # traceback.print_tb(tb) # Fixed format
            tb_info = traceback.extract_tb(tb)
            filename, line, func, text = tb_info[-1]
            print(
                "Error: Assertion: occurred on line {} in statement '{}'".format(
                    line, text
                )
            )
            errors = True

        except Exception as err_msg:
            print(
                "Error: Failed to retrieve content from "
                + project
                + ". Server responded :"
            )
            print(str(err_msg))
            errors = True

        # if project == 'browser_penncnp' :
        #     session.disconnect_penncnp()

        assert not errors, "Errors occurred, see previous output."

    print("Info: Time log writen to " + timeLogFile)
