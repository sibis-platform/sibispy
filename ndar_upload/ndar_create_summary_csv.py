#!/usr/bin/env python
"""
This script retrieves the sibis-general-config from either the passed location or ~/.sibis/.sibis-general-config if no location is passed. From this, it extracts three locations:
- staging_path, containing folders for the visits to be moved based on their validation and consent status
- consent_path, where consent is found
- data_path, where the actual visits reside

Consent for NDAR upload for the passed visit is checked. If it has been denied, the visit is moded permanently into $staging_path/non_consent. If we are still waiting for consent, 
the visit is moved into $staging_path/waiting_for_consent. If we have consent, we first validate the visit with NDATools. If it passes validation, it's rsynced to the 
staging/summaries directory, where the individual subject files are appended to the summary files, and the respective imaging files are copied to the summaries dir as well.

   
"""

from dataclasses import dataclass
import argparse
import os
import pathlib
import sys

import yaml
from typing import List
import logging
import pandas as pd
import shutil
import subprocess
import json
import csv
from io import StringIO

import NDATools
from NDATools.Validation import Validation
from NDATools.clientscripts import vtcmd

def check_consent(args, mappings, included_visits, path_to_visits, consent_path, staging_path):
    consented_visits = []
    if args.do_not_remove:
        do_not_rm_flag = True
    else:
        do_not_rm_flag = False
    
    for visit in included_visits:
        logging.info(f"Checking consent for visit {visit}")
        visit_path = mappings.get_visit_path(args, path_to_visits, visit)
        consent = mappings.get_consent(
            visit_path, consent_path,
        )  # Retrieve consent from consent_dir
        if consent is None:
            ...
            validation_errors_path = staging_path / StagingPaths.validation_errors
            error_path: pathlib.Path = validation_errors_path / "consents_error_log.csv"

            with error_path.open("a") as fh:
                fh.write("{consent_path},Problem locating consent in demographics.csv\n")

            if do_not_rm_flag:
                copy_visits(path_to_visits, [visit_path], validation_errors_path)
            else:
                move_visits(path_to_visits, [visit_path], validation_errors_path)
        else:
            consent_given = mappings.process_consent(
                visit_path, staging_path, consent, do_not_rm_flag, False
            )  # Move visit to correct place in staging if consent not given
            if consent_given:
                # If consent given, add to list of visits to validate
                consented_visits.append(visit)
    # initiate mass move/copy of non-consented visits
    mappings.process_consent(None, None, None, do_not_rm_flag, True)
    
    return consented_visits


def stretch_list(l: list, n: int):
    # stretch_list([1,2,3], 2) -> [1,1,2,2,3,3]
    stretched_list = [x for x in l for _ in range(n)]
    return stretched_list


def stringify_list(l: list):
    l = [str(x) for x in l]
    return "\n".join(l)


def unpack_errors(errors_dict: dict):
    # errors is a dict whose keys are error types and whose values are lists of dicts which
    # contain the actual errors of that type. E.g.:
    # {'missingRequiredColumn': [{'columnName': 'subjectkey', 'message': 'Data column subjectkey is required'},
    # {'columnName': 'src_subject_id', 'message': 'Data column src_subject_id is required'}]}
    # We want to turn it into two lists, one of the actual errors, stringified and without commas so it can be put into a csv,
    # and one of the error types, expanded in length to match that of the errors. E.g. we want to return:
    # ['missingRequiredColumn', 'missingRequiredColumn'], ['{'columnName': 'subjectkey'     'message': 'Data column subjectkey is required'}',
    # '{'columnName': 'subjectkey'     'message': 'Data column src_subject_id is required'}']
    error_types = []
    all_errors = []
    for error_type in errors_dict.keys():
        errors_of_type = errors_dict[error_type]
        stringified_errors = [str(x).replace(",", "    ") for x in errors_of_type]
        error_types.extend([error_type] * len(stringified_errors))
        all_errors.extend(stringified_errors)
    return error_types, all_errors


def ndar_validate(path_to_visits, visits, files_to_validate, vtcmd_config):
    """
    Validates the ndar files using the NDATools validate() function.
    Credentials have been added to the validation config to comply with nda-tools==0.2.17.

    ;param path_to_visits: base path to the directory containing visit information
    :param visits: specific visits from which files will be validated
    :param files_to_validate: E.g. ndar_subject01.csv, image03.csv, etc.
    :param vtcmd_config: contains all config paramters for vtcmd

    :returns: validation_results_df, dataframe containing the results of 
    validate() for each file to validate.
    """
    validation_responses = []
    
    for i in range(len(visits)):
        path_to_validate = path_to_visits / visits[i] / files_to_validate[i]

        # test if the file to validate needs a manifest path
        if "ndar_subject01.csv" != path_to_validate.name:
            vtcmd_config.manifest_path = [path_to_validate.parent]

        validation = Validation(
            [ path_to_validate ] ,
            config=vtcmd_config,
            hide_progress=vtcmd_config.hideProgress,
            thread_num=1,
            allow_exit=True,
        )
        logging.info(f"Validating files:\n{str(path_to_validate)}")

        validateFlag=True
        if validateFlag :   
            validation.validate()
            validation_response=validation.responses
        else :
            print(f'WARNING:skip ndar validation !')
            validation_response=visits

        validation_responses.append(validation_response)

    passed_validation = []
    error_types_list = []
    errors_list = []
    for visit, file_to_validate, response_tuple in zip(visits, files_to_validate, validation_responses):
        file_string = f"{visit}/{file_to_validate}"
        if validateFlag:
            response, _ = response_tuple[0]
            if response["status"] == vtcmd.Status.SYSERROR:
                # If there's a system error while validating
                passed_validation.append(False)
                error_types_list.append(["SystemError"])
                errors_list.append(["SystemError while validating"])
                logging.warn("\nSystemError while validating: {}".format(file_string))
                logging.warn("Please contact NDAHelp@mail.nih.gov")
            
            elif response["errors"] != {}:
                # If there are validation errors
                passed_validation.append(False)
                error_types, errors = unpack_errors(response["errors"])
                error_types_list.append(error_types)
                errors_list.append(errors)
                logging.info(f"Error validating {file_string}:\n{stringify_list(errors)}.")

            else:
                # If there were no errors
                passed_validation.append(True)
                error_types_list.append([""])
                errors_list.append([""])
                logging.info(f"No errors validating {file_string}.")

        else :
            passed_validation.append(True)
            error_types_list.append([""])
            errors_list.append([""])
            logging.info(f"No errors validating {file_string}.")


    validation_results_df = pd.DataFrame(
        {
            "visit": visits,
            "file_to_validate": files_to_validate,
            "passed_validation": passed_validation,
            "error_type": error_types_list,
            "error": errors_list,
        }
    )
    validation_results_df = validation_results_df.apply(
        pd.Series.explode
    )  # Expand the columns with lists into separate rows
    return validation_results_df


HEADERTYPE_MAP = {
    "GUID": 'str',
    "String": 'str',
    "Date": 'str',
    "Integer": 'Int64',
    "Float": 'str',
    "File": 'str',
    "Thumbnail": 'str',
    "Manifest": 'str'
}

@dataclass
class CSVMeta():
    file_definition: str
    header_row: List[str]

csv_meta_map = {
    "ndar_subject01.csv": CSVMeta("ndar_subject01_definitions.csv", ["ndar_subject", "01"]),
    "image03.csv": CSVMeta("image03_definitions.csv", ["image", "03"]),
    "asr01.csv": CSVMeta("measurements/asr01_definitions.csv", ["asr", "01"]),
    "fgatb01.csv": CSVMeta("measurements/fgatb01_definitions.csv", ["fgatb", "01"]),
    "grooved_peg02.csv": CSVMeta("measurements/grooved_peg02_definitions.csv", ["grooved_peg", "02"]),
    "macses01.csv": CSVMeta("measurements/macses01_definitions.csv", ["macses", "01"]),
    "sre01.csv": CSVMeta("measurements/sre01_definitions.csv", ["sre", "01"]),
    "tipi01.csv": CSVMeta("measurements/tipi01_definitions.csv", ["tipi", "01"]),
    "uclals01.csv": CSVMeta("measurements/uclals01_definitions.csv", ["uclals", "01"]),
    "upps01.csv": CSVMeta("measurements/upps01_definitions.csv", ["upps", "01"]),
    "wrat401.csv": CSVMeta("measurements/wrat401_definitions.csv", ["wrat4", "01"]),
}

def write_summary_csv(old_summary_csv, new_subj_csv, data_dict_path):
    """
    Function writes the new ndar_subject01.csv summary file. Will need to be extended later 
    to handle the imaging summary files. 
    Requires the previous summary csv file that already exists, and the new subject csv file
    which contains the data we will be appending.
    1. Read in the two files, skipping the first line to ignore the first header
    2. Using the ndar)subject01_definitions.csv data dictionary, define the datatypes
      for each column (May be utilizing a dataclass to do so)
    3. Initially write the first header to the output summary_csv file 
    4. Write the concatenated csv file.

    :param old_summary_csv: previous summary csv of the given file type
    :param new_subj_csv: individual subject csv to be appended to old_summary
    :param data_dict_path: path to the directory containing ndar file header
        data types.
    """
    # Get the data dictionary definitions for the corresponding summary file.
    file_type = old_summary_csv.name

    try:
        meta = csv_meta_map[file_type]
    except KeyError:
        raise ValueError(f"Unknown file_type: {file_type} for creating summary files.")

    data_dict_path = data_dict_path / meta.file_definition

    data_dict = pd.read_csv(data_dict_path).T.to_dict()
    
    data_dict_dtype = {}
    parse_dates = []

    for row in data_dict:
        data_dict_dtype[data_dict[row]['ElementName']] = data_dict[row]['DataType']
    
    # check if the column needs to be a parsed date before changing to strings
    parse_dates = [d for d, v in data_dict_dtype.items() if v == 'Date']
    
    # custom date_parser function in order to properly handle null values
    date_parser = lambda d: pd.to_datetime(d, errors='ignore', format='%m/%d/%y')

    # map data dictionary types to actual data types
    for row in data_dict_dtype:
        data_dict_dtype[row] = HEADERTYPE_MAP[data_dict_dtype[row]]

    # pass data dictionary to pandas to load values with datatypes, skipping the first line
    old_summary_df = pd.read_csv(old_summary_csv, skiprows=1, dtype=data_dict_dtype, date_parser=date_parser, parse_dates=parse_dates)
    new_subject_df = pd.read_csv(new_subj_csv, skiprows=1, dtype=data_dict_dtype, date_parser=date_parser, parse_dates=parse_dates)

    new_summary_df = pd.concat([old_summary_df, new_subject_df])

    # create a StringIO file object buffer to write the first header to the csv output
    buffer = StringIO()
    c = csv.writer(buffer, quoting=csv.QUOTE_ALL)
    c.writerow(meta.header_row)

    new_summary_csv = new_summary_df.to_csv(buffer, index=False, quoting=csv.QUOTE_NONNUMERIC, date_format='%m/%d/%Y')

    # write the file object content to previous summary csv file
    with open(old_summary_csv, 'w') as csv_file:
        buffer.seek(0)
        shutil.copyfileobj(buffer, csv_file)


def move_validated_files(validation_results_df, staging_path, path_to_visits, data_dict_path, args):
    summaries_path = staging_path / "staging" / "summaries"
    passed_validation_df = validation_results_df[
        validation_results_df["passed_validation"]
    ].drop(columns=["passed_validation"])
    for index, row in passed_validation_df.iterrows():
        visit_path = path_to_visits / row["visit"]
        file_to_validate = row["file_to_validate"]
        valid_file_path = visit_path / file_to_validate
        summaries_file_path = summaries_path / file_to_validate.name
        valid_file_df = pd.read_csv(valid_file_path, skiprows=1, dtype=str)

        if summaries_file_path.exists():
            # If a summaries file already exists, append the contents of the validated file to it
            logging.info(
                f"Summaries file exists already, appending {valid_file_path} to {summaries_file_path}"
            )
            # Old summary file = summaries_file_path
            # new subject summary file that needs to be appended = valid_file_path
            write_summary_csv(summaries_file_path, valid_file_path, data_dict_path)
            
            # and delete the validated file
            if not args.do_not_remove :
                valid_file_path.unlink()
            
        else:
            # Otherwise begin a summaries file by rysncing the validated file to summaries
            logging.info(
                f"Summaries file does not exist already, creating one by moving {valid_file_path} to {summaries_file_path}"
            )
            # rsync the files from the valid file path to the summaries file path
            if args.do_not_remove:
                subprocess.call(["rsync", "-a", str(valid_file_path), str(summaries_file_path)])
            else:
                subprocess.call(["rsync", "-a --remove-source-files", str(valid_file_path), str(summaries_file_path)])
        
        # Now load validated file to check if it has manifest file
        if "manifest" in valid_file_df:
            manifest_relative_path = valid_file_df["manifest"][0]
            if manifest_relative_path:
                valid_file_manifest_path = valid_file_path.parent / manifest_relative_path
                valid_manifest_dir = valid_file_manifest_path.parent
                summaries_manifest_path = summaries_file_path.parent / manifest_relative_path
                summaries_manifest_dir = summaries_manifest_path.parent

                if not valid_file_manifest_path.exists() :
                    #DEBUG if it happens as it should not at this stage
                    print(f"ERROR:{valid_file_manifest_path} does not exist!")
                    sys.exit(1)
                    
                # load the manifest file
                with open(valid_file_manifest_path) as data_file:    
                    manifest_dict = json.load(data_file)
                    # e.g. {"files": [{"path": "NDARXW277CFD/202205/structural/t2.nii.gz", "name": "t2.nii.gz", "size": "63169822", "md5sum": "e3a922358f99f96f2bc1d43eb88750ed"}]}

                # Collect all the top-level directories specified in the manifest file
                relative_roots = list()
                for file_dict in manifest_dict["files"]:
                    relative_path = file_dict["path"]
                    if not pathlib.Path(valid_manifest_dir / relative_path).exists():
                        logging.error(
                            f"{valid_manifest_dir}/{relative_path} does not exist as promised in manifest"
                        )
                        print(f"This needs to be caught when validating files for errors - DEBUG!")
                        sys.exit(1)
                        
                    else:
                        relative_root = relative_path.split("/")[0]  # e.g. NDARXW277CFD
                        # make sure only unique root directories are added
                        if relative_root not in relative_roots:
                            relative_roots.append(relative_root)

                for relative_root in relative_roots:
                    # and move each of the unique top-level directories to summaries
                    logging.info(
                        f"Moving associated files {valid_manifest_dir / relative_root} to {summaries_manifest_dir}"
                    )
                    summaries_root_dir = summaries_manifest_dir / relative_root

                    subprocess.call(["rsync", "-a", (str(valid_manifest_dir / relative_root) + "/"), str(summaries_root_dir)])
                        
                # Then move the manifest file
                logging.info(
                    f"Moving manifest file {valid_file_manifest_path} to {summaries_manifest_dir}"
                )
                if summaries_manifest_path.exists() :
                    logging.info(f"Removing existing {summaries_manifest_path}!")
                    summaries_manifest_path.unlink()

                if args.do_not_remove:
                    subprocess.call(["rsync", "-a", str(valid_file_manifest_path), str(summaries_manifest_path)])
                else:
                    subprocess.call(["rsync", "-a --remove-source-files", str(valid_file_manifest_path), str(summaries_manifest_path)])
                    

def update_error_log(validation_errors_df, validation_errors_path):
    # Create path to validation errors log file
    error_path = validation_errors_path / "validation_error_log.csv"
    if error_path.exists():
        # If it exists, append to it
        validation_errors_df.to_csv(error_path, index=False, mode="a", header=False)
    else:
        # Otherwise create it
        validation_errors_df.to_csv(error_path, index=False)


def recheck_validation(included_visits, staging_path, args, path_to_visits, files_to_validate):
    # Load validation errors log
    validation_errors_path = staging_path / StagingPaths.validation_errors
    error_path = validation_errors_path / "validation_error_log.csv"
    error_log_df = pd.read_csv(error_path)

    # Only revalidate passed visits (remember this is all visits if none were passed)
    revalidate_df = error_log_df[error_log_df["visit"].isin(included_visits)]
    revalidate_df = revalidate_df.set_index(
        ["visit", "file_to_validate"]
    )  # Group errors by vist and file to get unique visit/file pairs
    multiindex = (
        revalidate_df.index.unique()
    )  # Extract unique multiindexes (ie unique visit/file tuples)
    visits = multiindex.get_level_values(0).values
    files_to_validate = multiindex.get_level_values(1).values

    # but keep track of errors for visits we're not rechecking
    remaining_errors_df = error_log_df[~error_log_df["visit"].isin(included_visits)]

    # Revalidate
    vtcmd_config = vtcmd.configure(args)
    validation_results_df = ndar_validate(
        path_to_visits, visits, files_to_validate, vtcmd_config
    )
    move_validated_files(validation_results_df, staging_path, path_to_visits, args)
    # Filter only rows failing validation
    new_errors_df = validation_results_df[
        ~validation_results_df["passed_validation"]
    ].drop(columns=["passed_validation"])

    # Delete visits with no more errors
    invalid_visits = list(new_errors_df["visit"].unique())
    valid_visits = list(set(visits) - set(invalid_visits))
    delete_visits(path_to_visits, valid_visits)

    # Add new errors to old errors
    errors_to_write_df = pd.concat([remaining_errors_df, new_errors_df])
    # And overwrite error log
    errors_to_write_df.to_csv(error_path, index=False)


def move_visits(
    path_to_visits: pathlib.Path, visits: list, destination_path: pathlib.Path
):
    for visit in visits:
        visit_path = path_to_visits / visit
        shutil.move(str(visit_path), str(destination_path))
        logging.info(f"Moving {visit_path} to {destination_path}")

def copy_visits(path_to_visits, visits, destination_path):
    for visit in visits:
        visit_path = path_to_visits / visit
        logging.info(f"Copying {visit_path} to {destination_path}")
        shutil.copytree(str(visit_path), str(destination_path / visit.name), dirs_exist_ok=True)
        logging.info("Copying complete.")

def delete_visits(path_to_visits: pathlib.Path, visits: list):
    for visit in visits:
        visit_path = path_to_visits / visit
        shutil.rmtree(str(visit_path))
        logging.info(f"Removing {visit_path}")


def is_file(file_path: str) -> pathlib.Path:
    maybe_file = pathlib.Path(file_path).expanduser()
    if maybe_file.exists() and maybe_file.is_file():
        return maybe_file
    return argparse.ArgumentTypeError(f"file: {file_path} does not exist or is not a file.")


def _parse_args(input_args: List = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    config_args = parser.add_argument_group('Config', 'Config args regardless of project (Enter before project).')
    config_args.add_argument(
        "--sibis_general_config",
        help="Path of sibis-general-config.yml **in the current context**. Relevant if this is "
        "run in a container and the cases directory is mounted to a different "
        "location. Defaults to /fs/storage/share/operations/secrets/.sibis/.sibis-general-config.yml.",
        type=is_file,
        default="/fs/storage/share/operations/secrets/.sibis/.sibis-general-config.yml",
    )

    # We can only perform one of these operations.
    me_parser = config_args.add_mutually_exclusive_group(required=True)
    me_parser.add_argument(
        "-n",
        "--check_new",
        help="Check a new visit.",
        action="store_true",
    )
    me_parser.add_argument(
        "-c",
        "--recheck_consent",
        help="Recheck consent of a visit in the waiting_for_consent directory. Can pass a list of visits to recheck specific ones or don't pass any to check everything in the waiting_for_consent directory.",
        action="store_true",
    )
    me_parser.add_argument(
        "-s",
        "--recheck_study",
        help="Recheck study designation of a visit in the waiting_for_study directory. Can pass a list of visits to recheck specific ones or don't pass any to check everything in the waiting_for_study directory.",
        action="store_true",
    )
    me_parser.add_argument(
        "-d",
        "--recheck_validation",
        help="Revalidate a visit in the validation_errors directory. Can pass a list of visits to recheck specific ones or don't pass any to check everything in the validation_errors directory.",
        action="store_true",
    )

    config_args.add_argument(
        "-u",
        "--username",
        metavar="<arg>",
        type=str,
        action="store",
        help="NDA username",
    )

    config_args.add_argument(
        "-p",
        "--password",
        metavar="<arg>",
        type=str,
        action="store",
        help="NDA password",
    )
    config_args.add_argument(
        "-v", "--verbose", help="Verbose operation", action="store_true"
    )
    config_args.add_argument(
        "-r", "--do_not_remove", help="Do not remove files once they pass validation", action="store_true"
    )
    config_args.add_argument(
        '--validation-timeout', 
        default=300, 
        type=int, 
        action='store', 
        help='Timeout in seconds until the program errors out with an error. Default=300s'
    )

    # HIVALC Specific Args
    subparsers = parser.add_subparsers(title='Project', dest='project', help='Define the project [mci_cb, cns_deficit, ncanda]')
    mci_cb_parser = subparsers.add_parser('mci_cb')
    mci_cb_parser.add_argument(
        "--visits",
        help="Space separated list of visits to upload, e.g. LAB_S01669_20220517_6910_05172022.",
        type=str,
        nargs="+",
    )

    cns_deficit_parser = subparsers.add_parser('cns_deficit')
    cns_deficit_parser.add_argument(
        "--visits",
        help="Space separated list of visits to upload, e.g. LAB_S01669_20220517_6910_05172022.",
        type=str,
        nargs="+",
    )

    hiv_parser = subparsers.add_parser('hiv')
    hiv_parser.add_argument(
        "--visits",
        help="Space separated list of visits to upload, e.g. LAB_S01669_20220517_6910_05172022.",
        type=str,
        nargs="+",
    )

    # NCANDA Specific Arms
    ncanda_parser = subparsers.add_parser('ncanda')
    ncanda_parser.add_argument(
        '--subject', dest='visits',
        help="The Subject ID of specific subject to add to summary file, typ: NCANDA_SXXXXX",
        type=str
    )
    ncanda_parser.add_argument(
        "--followup_year",
        help="Followup year of the data being added to summary file (number only).",
        type=str, required=True,
    )

    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(
            format="%(levelname)s: %(message)s", level=logging.DEBUG, force=True
        )
        logging.info("Verbose output.")
    else:
        logging.basicConfig(format="%(levelname)s: %(message)s")

    logging.info(f"Loading sibis-general-config from {args.sibis_general_config}")
    args.sibis_general_config = pathlib.Path(args.sibis_general_config)

    # Add defaults for args the vtcmd module expects
    vtcmd_args = [
        "accessKey",
        "secretKey",
        "collectionID",
        "alternateEndpoint",
        "listDir",
        "manifestPath",
        "s3Bucket",
        "s3Prefix",
        "title",
        "description",
        "scope",
        "validationAPI",
        "JSON",
        "hideProgress",
        "skipLocalAssocFileCheck",
        "buildPackage",
        "resume",
        "workerThreads",
        "replace_submission",
        "force",
        "warning",
    ]
    for arg in vtcmd_args:
        setattr(args, arg, None)

    return args

@dataclass(frozen=True)
class StagingPaths:
    staging: pathlib.Path = pathlib.Path("staging")
    waiting_for_consent: pathlib.Path = staging / "waiting_for_consent"
    waiting_for_study: pathlib.Path = staging / "waiting_for_study"
    validation_errors: pathlib.Path = staging / "validation_errors"
    exempt_from_release: pathlib.Path = staging / "exempt_from_release"


def doMain():
    args = _parse_args()

    if not args.sibis_general_config.exists():
        raise ValueError(f"{args.sibis_general_config} does not exist")
    with open(args.sibis_general_config, "r") as f:
        config = yaml.safe_load(f)
    config = config.get("ndar").get(args.project)

    sys.path.append(config.get('mappings_dir'))

    if args.project == 'ncanda':
        import ncanda_mappings as mappings
    else:
        import hivalc_mappings as mappings

    # Get base paths for everything from config
    staging_path, data_path, consent_path, files_to_validate, data_dict_path = mappings.get_paths_from_config(args, config)
    
    # set the path to the visit directory depending on args operation type
    path_to_visits = mappings.set_visit_path(args, staging_path, data_path)
    
    # if no specific visits were specified, then get a list of visits from path_to_visits
    if args.visits:
        visit_list=args.visits
    else :
        # Check all visits in path_to_visits if no vists were passed
        logging.info(f"No visits given, checking all visits in {path_to_visits}")
        visit_list = os.listdir(path_to_visits)

    # Drop all visits that are not associated w/ the current study/project
    included_visits = mappings.check_study_designation(visit_list, path_to_visits, consent_path, staging_path, args)

    # check_new, recheck_consent, and recheck_study are the same operation, difference is location of the visit directories:
    # check_new = data directory, recheck_consent = waiting for consent directory, recheck_study = waiting for study dir    
    if args.check_new or args.recheck_consent or args.recheck_study:
        
        ## Step 1: Check Consents 
        consented_visits = check_consent(args, mappings, included_visits, path_to_visits, consent_path, staging_path)

        ## Step 2: Validate visits that have consents and study designation.
        if len(consented_visits) > 0:
            logging.info("Proceeding to validation.")
            # In the following code, we duplicate the list of visits and files so that
            # zipping them gives every combination of visit and file to validate
            # E.g. if consented_visits = [1,2] and files_to_validate = [a,b,c], then
            visits = stretch_list(
                consented_visits, len(files_to_validate)
            )  # -> [1,1,1,2,2,2]
            files_to_validate = files_to_validate * len(
                consented_visits
            )  # -> [a,b,c,a,b,c]
            vtcmd_config = vtcmd.configure(args)
            visits, files_to_validate = mappings.restrict_to_existing_files(
                args, path_to_visits, visits, files_to_validate
            )
            if len(visits) == 0 or len(files_to_validate) == 0 :
                logging.info(f"Nothing to validate!")
                sys.exit(0)

            validation_results_df = ndar_validate(
                path_to_visits, visits, files_to_validate, vtcmd_config
            )

            move_validated_files(validation_results_df, staging_path, path_to_visits, data_dict_path, args)
            # Filter only rows failing validation
            validation_errors_df = validation_results_df[
                ~validation_results_df["passed_validation"]
            ].drop(columns=["passed_validation"])


            # Create path to validation errors log file
            validation_errors_path = staging_path / StagingPaths.validation_errors
            # Update the log of validation errors
            update_error_log(validation_errors_df, staging_path)

            # then move any visit with an invalid file to validation_errors_path
            invalid_visits = list(validation_errors_df["visit"].unique())
            if args.do_not_remove:
                copy_visits(path_to_visits, invalid_visits, validation_errors_path)
            else:
                move_visits(path_to_visits, invalid_visits, validation_errors_path)

            # Delete the directories for all other visits (these are now empty
            # of everything but the directory structure skeleton and possibly the
            # validated csv files if there was already a summary file and their contents
            # were just copied over)
            valid_visits = list(set(consented_visits) - set(invalid_visits))

            if not args.do_not_remove :
                delete_visits(path_to_visits, valid_visits)

    else:  # if args.check_validation
        recheck_validation(included_visits, staging_path, args, path_to_visits, files_to_validate)


if __name__ == "__main__":
    doMain()
