#!/usr/bin/env python
"""
This script retrieves the sibis-general-config from either the passed location or ~/.sibis/.sibis-general-config if no location is passed. 
From this, it extracts two locations:
- summaries_path
- uploaded_path

The files in the summaries directory contain csv summary files of the individual subject files
that have already passed validation.
All files that are in the summaries path are to be validated and submitted to NDAR.
After the summaries files are submitted, they are moved to the uploaded directory (out of staging) under a directory that is titled as the
timestamp of the submission.
"""

from dataclasses import dataclass
import datetime
import argparse
from distutils.dir_util import copy_tree
from distutils.log import warn
import os
import pathlib
import re
from traceback import FrameSummary

from sqlalchemy import desc
import sibispy
import yaml
from typing import Any, List
import logging
import pandas as pd
import shutil
import json
import datetime
import sys

import NDATools
from NDATools.Validation import Validation
from NDATools.Submission import Submission
from NDATools.BuildPackage import SubmissionPackage
from NDATools.clientscripts import vtcmd

def is_file(file_path: str) -> pathlib.Path:
    maybe_file = pathlib.Path(file_path).expanduser()
    if maybe_file.exists() and maybe_file.is_file():
        return maybe_file
    return argparse.ArgumentTypeError(f"File: {file_path} does not exist or is not a file.")

def build_df(responses):
    """
    Transfrom the responses from the various steps into dataframes with relevant columns
    """
    arr = []

    for response in responses:
        result = response[0][0]
        result.pop('warnings')
        result_series = pd.Series(result)
        result_df = result_series.to_frame().T
        arr.append(result_df)

    new_df = pd.concat(arr, ignore_index=True)

    return new_df

def validate_summary_file(vtcmd_config, summaries_path, files_to_upload):
    """
    Validate all files that are in the summaries directory
    """
    validation_responses = []
    

    for file in files_to_upload:
        # file represents PosixPath - PosixPath('ndar_subject01.csv')

        path_to_validate = summaries_path / file

        if not path_to_validate.is_file():
            logging.warning(f'WARNING: {file} does not exist in the summaries directory!')
            continue
        

        if "ndar_subject01.csv" != path_to_validate.name:
            vtcmd_config.manifest_path = [path_to_validate.parent]

        validation = Validation(
            [ path_to_validate ],
            config = vtcmd_config,
            hide_progress=vtcmd_config.hideProgress,
            thread_num=1,
            allow_exit=True,
        )

        logging.info(f"Validating summary file:\n{str(path_to_validate)}")

        validation.validate()

        validation_responses.append(validation.responses)

    validation_responses_df = build_df(validation_responses)
  
    return validation_responses_df

def handle_validation_errors(validation_results_df):
    """
    Check the dataframe containing results of the validation step for the presence of errors.
    If there are errors, halt program.
    """

    error_list = []

    for _, row in validation_results_df.iterrows():
        if row['errors']:
            error_list.append(row['errors'])

    return error_list

def check_upload(validation_result_df):
    """
    Prompt the user to confirm that they want to proceed with the package build and 
    submission after the files have been validated.
    If they confirm submission, keep the do_not_upload argument as False.
    """
    files = validation_result_df['short_name'].tolist()
    status = validation_result_df['status'].tolist()

    # output files that are to be uploaded
    logging.info(f"Validation of summary files has completed.")
    for i in range(len(files)):
        logging.info(f"File: {files[i]} has passed validation w/ the status: {status[i]}.")

    user_input = input("Would you like to proceed w/ building and submitting the dataset package?\n Enter Y/y/yes to confirm, any other key to abort: ")

    if user_input in ["Y", "y", "yes"]:
        return False

    return True
    
def write_dataset_info(validation_result_df):
    """
    Create the dataset title and description
    Title: day that the package is being built (YYYY-MM-DD)
    Description: CSV files that are being uploaded
    """
    title = datetime.date.today().isoformat()

    valid_files = []

    for _, row in validation_result_df.iterrows():
        if not row['errors']:
            valid_files.append(row['short_name'])

    description = ', '.join(valid_files)

    dataset_info = {
        'title': title,
        'description': description
    }

    return dataset_info

def create_uploaded_dir(dataset_info, uploaded_path):
    """
    Create the directory in uploaded, titled w/ the date of the submission.
    """
    new_uploaded_dir = dataset_info['title'].replace('-', '')

    # create new directory in uploaded path that is titled as uploaded data
    uploaded_dest = uploaded_path / new_uploaded_dir

    # if the uploaded destination already exists, an upload already occured today
    if os.path.isdir(uploaded_dest):
        logging.error(f"\n ERROR: ${uploaded_dest} already exists.")
        sys.exit(1)

    return uploaded_dest

def build_submission_package(validation_result_df, dataset_info, vtcmd_config):
    """
    Builds the submission package for the upload, which uses all of the
    UUID's of the validated files. It returns a submission package UUID which
    is what is uploaded to NDAR.
    """
    logging.info("Building submission package.")

    # store the list of UUID's from the validation result df
    uuids = validation_result_df['id'].tolist()

    submission_package = SubmissionPackage(
        uuid = uuids,
        associated_files = None,
        config = vtcmd_config,
        allow_exit=True,
        collection = vtcmd_config.collection_id,
        title = dataset_info['title'],
        description = dataset_info['description']
    )

    submission_package.set_upload_destination(hide_input=False)
    submission_package.build_package()

    logging.info(f"Package built w/ UUID: ${submission_package.submission_package_uuid}")

    sub_dict = {
        'validation_results': submission_package.validation_results,
        'submission_package_uuid': submission_package.submission_package_uuid,
        'create_date': submission_package.create_date,
        'expiration_date': submission_package.expiration_date
    }

    submission_package_df = pd.DataFrame(sub_dict)

    return submission_package_df
    
@dataclass(frozen=True)
class Status:
    UPLOADING = 'Uploading'
    SYSERROR = 'SystemError'
    COMPLETE = 'Complete'
    ERROR = 'Error'
    PROCESSING = 'In Progress'

def upload_package(submission_package_df, summaries_path, vtcmd_config):
    """
    Upload the built submission package to NDAR.
    """
    package_uuid = submission_package_df.iloc[0]['submission_package_uuid']

    submission = Submission(
        package_id=package_uuid,
        full_file_path=summaries_path,
        config=vtcmd_config,
        allow_exit=False,
    )
    logging.info(f"Submitting files in: \n{summaries_path}")

    # Submission prevention flag
    submit_upload = True

    if submit_upload:
        submission.submit()

        logging.info(f"Submission ID: ${submission.submission_id}.")
        logging.info(f"Submission completed w/ status: ${submission.status}")

    return submission.status

def move_summaries_to_uploaded(summaries_path, uploaded_dir, args):
    """
    Create the new directory in the uploaded directory and move all files from summaries
    into it.
    """
    if args.do_not_move:
        logging.info(f"Copying uploaded files from ${summaries_path} to ${uploaded_dir}.")
        shutil.copytree(summaries_path, uploaded_dir)
    else:
        logging.info(f"Moving uploaded files from ${summaries_path} to ${uploaded_dir}.")
        shutil.move(summaries_path, uploaded_dir, copy_function=copy_tree)

def _parse_args(input_args: List = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
    description='This application allows you to submit data into NDA. '
                    'If your data contains manifest files, you must specify the location of the manifests. '
                    'If your data also includes associated files, you must enter a list of at least one directory '
                    'where the associated files are saved. '
                    'Any files that are created while running the client (ie. results files) will be downloaded in '
                    'your home directory under NDAValidationResults. If your submission was interrupted in the middle'
                    ', you may resume your upload by entering a valid submission ID. ',
        usage='%(prog)s <file_list>')

    config_args = parser.add_argument_group('Config', 'Config args regardless of project (Enter before project).')
    config_args.add_argument(
        "--sibis_general_config",
        help="Path of sibis-general-config.yml **in the current context**. Relevant if this is "
        "run in a container and the cases directory is mounted to a different "
        "location. Defaults to ~/.sibis/.sibis-general-config.yml.",
        type=is_file,
        default="~/.sibis/.sibis-general-config.yml",
    )
    # config_args.add_argument(
    #     "--project",
    #     help="Project to upload data for.",
    #     type=str,
    #     choices=["cns_deficit", "mci_cb", "ncanda"],
    #     required=True,
    # )
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
        "-a",
        "--alternateEndpoint",
        metavar="<arg>",
        type=str,
        action="store",
        help="An alternate upload location for the submission package",
    )
    config_args.add_argument(
        "-b",
        "--buildPackage",
        action="store_true",
        help="Flag whether to construct the submission package",
    )
    config_args.add_argument(
        "-v", "--verbose", help="Verbose operation", action="store_true",
    )
    config_args.add_argument(
        "-x", "--do_not_upload", 
        help="Do not build submission package and upload to ndar, only validate",
        action="store_true"
    )
    config_args.add_argument(
        "-y", "--do_not_move",
        help="Do not move the files from summaries to uploaded, copy them instead.",
        action="store_true"
    )
    config_args.add_argument(
        '--validation-timeout', 
        default=300, 
        type=int, 
        action='store', 
        help='Timeout in seconds until the program errors out with an error. Default=300s'
    )

    subparsers = parser.add_subparsers(title='Project', dest='project', help='Define the project [mci_cb, cns_deficit, ncanda]')
    mci_cb_parser = subparsers.add_parser('mci_cb')
    cns_deficit_parser = subparsers.add_parser('cns_deficit')
    
    # specific ncanda parser for followup year of values to be uploaded
    ncanda_parser = subparsers.add_parser('ncanda')
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
        "listDir",
        "manifestPath",
        "s3Bucket",
        "s3Prefix",
        "scope",
        "validationAPI",
        "JSON",
        "hideProgress",
        "skipLocalAssocFileCheck",
        "workerThreads",
        "resume",
        "title",
        "description",
        "manifestPath",
        "replace_submission",
        "force",
        "warning",
    ]
    for arg in vtcmd_args:
        setattr(args, arg, None)

    return args

def doMain():
    # parse arguments
    args = _parse_args()

    if not args.sibis_general_config.exists():
        raise ValueError(f"{args.sibis_general_config} does not exist")
    with open(args.sibis_general_config, "r") as f:
        config = yaml.safe_load(f)
    config = config.get("ndar").get(args.project)

    if args.project == 'ncanda':
        import ncanda_mappings as mappings
    else:
        import hivalc_mappings as mappings

    # define collection id based on project being uploaded
    setattr(args, 'collectionID', config.get('collection_id'))

    # get base paths from config (summaries and uploaded)
    summaries_path, uploaded_path, files_to_upload = mappings.get_upload_paths_from_config(args, config)

    vtcmd_config = vtcmd.configure(args)

    # validate all files in the summaries directory
    validation_result_df = validate_summary_file(vtcmd_config, summaries_path, files_to_upload)

    # If there are errors in validation, halt the upload process
    error_list = handle_validation_errors(validation_result_df)

    if error_list:
        for error in error_list:
            logging.error(error)
        raise ValueError("Files did not pass validation. Halting upload process.")

    # if the script is called w/ do_not_upload, do not build package and submit
    if not args.do_not_upload:
        # confirm package build and upload
        args.do_not_upload = check_upload(validation_result_df)

    if not args.do_not_upload:
        dataset_info = write_dataset_info(validation_result_df)

        uploaded_dir = create_uploaded_dir(dataset_info, uploaded_path)

        # build the submission package for the dataset
        submission_package_df = build_submission_package(validation_result_df, dataset_info, vtcmd_config)

        # submit package
        submission_status = upload_package(submission_package_df, summaries_path, vtcmd_config)

        # move the contents of summaries that has been uploaded to uploaded dir
        move_summaries_to_uploaded(summaries_path, uploaded_dir, args)

if __name__ == "__main__":
    doMain()
    
