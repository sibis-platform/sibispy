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

from asyncore import file_dispatcher
from dataclasses import dataclass
import datetime
import argparse
from importlib.resources import path
import os
import pathlib
import re
import string
import sys
from this import d
from tokenize import String
from isodate import date_isoformat

from pymysql import Date
from sqlalchemy import Float
from traitlets import Integer
import sibispy
import yaml
from typing import Any, List
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




def decompose_visit(visit: str):
    regex = "(LAB_S\d{5})_(\d{8}_\d{4}_\d{8})"
    matches = re.match(regex, visit)
    lab_id = matches.group(1)
    visit_date = matches.group(2)
    return lab_id, visit_date


@dataclass
class StudyDesignations():
    cns_deficit: int
    mci_cb: int
    other: int
    excluded: int
    
    def get_study(self, study_name):
        if study_name == "cns_deficit":
            return self.cns_deficit
        elif study_name == "mci_cb":
            return self.mci_cb
        else:
            return self.other
    
    def get_study_sum(self):
        sum = self.cns_deficit + self.mci_cb + self.other
        return sum
    
    def check_excluded(self):
        if self.excluded == 1:
            return True
        return False
            
def check_study_designation(included_visits, path_to_visits, consent_path, staging_path, args) -> list:
    """
    Retuns the visit list w/ subjects who's study designation matches the current project.
    If the subjects study designation does not match, the directory is moved to the exempt from release
    directory in releases.
    
    :param included_visits: List of visits that have consent validated
    :param path_to_visits: base path to visits in upload2ndar
    :param consent_path: base path to subjects demographics file
    :param staging_path: base path to staging parent directory in releases
    :param args: contains argument for current project that is being validated
    
    :returns: Updated consented visits list that only contains visits w/ matching study designation.
    """
    
    for visit in included_visits:
        logging.info(f"Checking study designation for visit {visit}")
        visit_path = (path_to_visits / visit)
        exempt_path = staging_path / "exempt_from_release"
        waiting_for_study = staging_path / "staging" / "waiting_for_study"
        
        # get study designations:
        lab_id, visit_date = decompose_visit(visit)
        
        demographics_path = (
            consent_path / lab_id / "standard" / visit_date / "redcap" / "demographics.csv"
        )

        try:
            demographics_csv = pd.read_csv(demographics_path, keep_default_na=False)
            # demographics vars: demo_ndar_study___cns,demo_ndar_study___mci,demo_ndar_study___oth, demo_ndar_excluded
            study_designations = StudyDesignations(
                cns_deficit= demographics_csv["demo_ndar_study___cns"].item(),
                mci_cb= demographics_csv["demo_ndar_study___mci"].item(),
                other= demographics_csv["demo_ndar_study___oth"].item(),
                excluded = demographics_csv["demo_ndar_excluded"].item(),
            )
            
            # if visit is exempt from all studies, move to exempt
            if study_designations.check_excluded():
                logging.info(f"Visit is exempt from all studies. Moving {visit_path} to {exempt_path}")
                shutil.move(str(visit_path), str(exempt_path))
            
            current_study = args.project
            
            # if there are no study designations, move it to waiting_for_study in staging
            # e.g: releases/ndar/mci_cb/staging/waiting_for_study
            if study_designations.get_study_sum() == 0:
                logging.info(f"There are no study designations for this visit. Moving {visit_path} to {waiting_for_study}.")
                shutil.move(str(visit_path), str(waiting_for_study))

                # and remove visit from consented visits
                included_visits.remove(visit)
                
            # if subject is not under the current study but under others, move to the projects exempt from release
            # e.g: releases/ndar/mci_cb/exempt_from_release
            elif study_designations.get_study(current_study) == 0:              
                logging.info(f"Study designation does not match project. Moving {visit_path} to {exempt_path}")
                shutil.move(str(visit_path), str(exempt_path))
                
                # remove visit from visit list
                included_visits.remove(visit)
            
            # otherwise, just continue on to next visit

        except Exception as e:
            logging.error(f"Problem reading study designation from {demographics_path}", e)
            included_visits.remove(visit)
        
    return included_visits


def get_consent(visit_path: pathlib.Path, consent_path: pathlib.Path):
    visit = visit_path.name
    lab_id, visit_date = decompose_visit(visit)

    # Check for consent
    demographics_path = (
        consent_path / lab_id / "standard" / visit_date / "redcap" / "demographics.csv"
    )
    try:
        # keep_default_na turns blank fields to empty string
        demographics = pd.read_csv(demographics_path, keep_default_na=False)
        consent = demographics["demo_ndar_consent"].item()
        return consent
    except Exception as e:
        logging.error(f"Problem reading consent from {demographics_path}", e)
        return None


def process_consent(visit_path: pathlib.Path, staging_path: pathlib.Path, consent: int):
    # If consent denied
    if consent in [1, 9]:
        # move to non_consent dir
        non_consent_path = staging_path / "non_consent"
        logging.info(f"Consent denied. Moving {visit_path} to {non_consent_path}")
        shutil.move(str(visit_path), str(non_consent_path))
        return False
    
    # if the subject is exempt from upload
    if consent == 7:
        # move to exempt_from_upload dir
        exempt_path = staging_path / "exempt_from_release"
        logging.info(f"Visit is exempt from uploading to NDAR. Moving {visit_path} to {exempt_path}")
        shutil.move(str(visit_path), str(exempt_path))
        return False

    # If consent 0 or empty
    elif consent == 0 or consent == "":
        # move to waiting_for_consent dir (RAs still need to ask for consent)
        waiting_for_consent_path = staging_path / "staging" / "waiting_for_consent"
        logging.info(
            f"Still waiting for consent. Moving {visit_path} to {waiting_for_consent_path}"
        )
        shutil.move(str(visit_path), str(waiting_for_consent_path))
        return False

    # If consent 5
    elif consent == 5:
        # Don't move anywhere yet, still have to validate
        logging.info("Consent given.")
        return True
    else:
        # If the consent value was not 0,1,5,9, or empty, complain
        raise ValueError(
            f"Value {consent} for demo_ndar_consent unrecognized in demographics file."
        )


def check_consent(included_visits, path_to_visits, consent_path, staging_path):
    consented_visits = []
    for visit in included_visits:
        logging.info(f"Checking consent for visit {visit}")
        visit_path = (
            path_to_visits / visit
        )  # e.g. /fs/neurosci01/lab/upload2ndar/mci_cb/LAB_S01669_20220517_6909_05172022 or  /fs/neurosci01/lab/releases/ndar/staging/waiting_for_consent/LAB_S01669_20220517_6909_05172022
        consent = get_consent(
            visit_path, consent_path
        )  # Retrieve consent from consent_dir
        if consent is None:
            ...
            validation_errors_path = staging_path / StagingPaths.validation_errors
            error_path: pathlib.Path = validation_errors_path / "consents_error_log.csv"

            with error_path.open("a") as fh:
                fh.write("{consent_path},Problem locating consent in demographics.csv\n")

            move_visits(path_to_visits, [visit_path], validation_errors_path)
        else:
            consent_given = process_consent(
                visit_path, staging_path, consent
            )  # Move visit to correct place in staging if consent not given
            if consent_given:
                # If consent given, add to list of visits to validate
                consented_visits.append(visit)
                
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


def restrict_to_existing_files(path_to_visits, visits, files_to_validate):
    existing_visits, existing_files = [], []
    for visit, file_to_validate in zip(visits, files_to_validate):
        path = path_to_visits / visit / file_to_validate
        if path.exists():
            existing_visits.append(visit)
            existing_files.append(file_to_validate)
        else:
            if file_to_validate == "ndar_subject01.csv":
                raise ValueError("ndar_subject01.csv file not found for {visit}")
    return existing_visits, existing_files


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
    "image03.csv": CSVMeta("image03_definitions.csv", ["image", "03"])
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

                    subprocess.call(["rsync", "-a", str(valid_manifest_dir / relative_root), str(summaries_root_dir)])
                        
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
    parser.add_argument(
        "--sibis_general_config",
        help="Path of sibis-general-config.yml **in the current context**. Relevant if this is "
        "run in a container and the cases directory is mounted to a different "
        "location. Defaults to ~/.sibis/.sibis-general-config.yml.",
        type=is_file,
        default="~/.sibis/.sibis-general-config.yml",
    )
    parser.add_argument(
        "--project",
        help="Project to upload data for.",
        type=str,
        choices=["cns_deficit", "mci_cb"],
        required=True,
    )
    parser.add_argument(
        "--visits",
        help="Space separated list of visits to upload, e.g. LAB_S01669_20220517_6910_05172022.",
        type=str,
        nargs="+",
    )

    # We can only perform one of these operations.
    me_parser = parser.add_mutually_exclusive_group(required=True)
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

    parser.add_argument(
        "-u",
        "--username",
        metavar="<arg>",
        type=str,
        action="store",
        help="NDA username",
    )

    parser.add_argument(
        "-p",
        "--password",
        metavar="<arg>",
        type=str,
        action="store",
        help="NDA password",
    )
    parser.add_argument(
        "-v", "--verbose", help="Verbose operation", action="store_true"
    )
    parser.add_argument(
        "-r", "--do_not_remove", help="Do not remove files once they pass validation", action="store_true"
    )
    parser.add_argument(
        '--validation-timeout', 
        default=300, 
        type=int, 
        action='store', 
        help='Timeout in seconds until the program errors out with an error. Default=300s'
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

    #FIXME: Is this even needed w/ the added mutually exclusive group? Think it can be deleted
    if args.check_new + args.recheck_consent + args.recheck_validation + args.recheck_study != 1:
        raise ValueError(
            "Please use 1 and only 1 of check_new, recheck_consent, recheck_study and recheck_validation"
        )

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

def get_paths_from_config(args: argparse.Namespace, config: Any) -> tuple:
    '''
    This returns a tuple:
        - staging_path: the base dir for the ndar staging workflow
        - data_path: the base dir where ndar csv files and imagery that are to be located live
        - consent_path: the base dir where consent data can be located
        - files_to_validate: a list of filenames in ndar csv format that should be validated
    '''
    try:
        staging_path = pathlib.Path(
            config.get("staging_directory")
        )  # e.g. /fs/neurosci01/lab/releases/ndar/mci_cb
    except:
        raise ValueError(f"No staging_directory in {args.sibis_general_config}")

    try:
        data_path = pathlib.Path(
            config.get("data_directory")
        )  # e.g. /fs/neurosci01/lab/upload2ndar/mci_cb
    except:
        raise ValueError(f"No data_directory in {args.sibis_general_config}")

    try:
        consent_path = pathlib.Path(
            config.get("cases_directory")
        )  # e.g. /fs/neurosci01/lab/cases_next/
    except:
        raise ValueError(f"No cases_directory in {args.sibis_general_config}")

    try:
        files_to_validate = config.get(
            "files_to_validate"
        )  # e.g. ['ndar_subject01.csv, 't1/image03.csv', 't2/image03.csv']
        files_to_validate = list(
            map(pathlib.Path, files_to_validate)
        )  # Map strings to paths
    except:
        raise ValueError(f"No ndar_cases_directory in {args.sibis_general_config}")

    try:
        data_dict_path = pathlib.Path(
        config.get("data_dict_directory")
        ) # e.g. '/fs/share/datadict/ndar'
    except:
        raise ValueError(f"No data_dict_directory in {args.sibis_general_config}")

    return staging_path, data_path, consent_path, files_to_validate, data_dict_path


def set_path(args, staging_path, data_path):
    """
    Set the path to the visit directories based upon whether we are:
    1. checking new - this should pull data from the data_path
    2. rechecking consents = this should pull data from the waiting for consent path
    3. rechecking study = this should pull data from the waiting for study path
    4. checking validation - this should pull data only from validation_errors path
    """
    
    if args.check_new:
        path_to_visits = data_path
    elif args.recheck_consent:
        path_to_visits = staging_path / StagingPaths.waiting_for_consent
    elif args.recheck_study:
        path_to_visits = staging_path / StagingPaths.waiting_for_study
    else:  # if args.check_validation
        path_to_visits = staging_path / StagingPaths.validation_errors
        
    return path_to_visits


def doMain():
    args = _parse_args()

    if not args.sibis_general_config.exists():
        raise ValueError(f"{args.sibis_general_config} does not exist")
    with open(args.sibis_general_config, "r") as f:
        config = yaml.safe_load(f)
    config = config.get("ndar").get(args.project)

    # Get base paths for everything from config
    staging_path, data_path, consent_path, files_to_validate, data_dict_path = get_paths_from_config(args, config)
    
    # set the path to the visit directory depending on args operation type
    path_to_visits = set_path(args, staging_path, data_path)
    
    # if no specific visits were specified, then get a list of visits from path_to_visits
    if args.visits:
        visit_list=args.visits
    else :
        # Check all visits in path_to_visits if no vists were passed
        logging.info(f"No visits given, checking all visits in {path_to_visits}")
        visit_list = os.listdir(path_to_visits)

    # Regardless of operation, drop all visits that are not associated w/ the current study/project
    included_visits = check_study_designation(visit_list, path_to_visits, consent_path, staging_path, args)        

    # check_new, recheck_consent, and recheck_study are the same operation, difference is location of the visit directories:
    # check_new = data directory, recheck_consent = waiting for consent directory, recheck_study = waiting for study dir    
    if args.check_new or args.recheck_consent or args.recheck_study:
        
        ## Step 1: Check Consents
        consented_visits = check_consent(included_visits, path_to_visits, consent_path, staging_path)

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
            visits, files_to_validate = restrict_to_existing_files(
                path_to_visits, visits, files_to_validate
            )
            if  len(visits) == 0 or len(files_to_validate) == 0 :
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
