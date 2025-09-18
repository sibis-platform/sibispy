#!/usr/bin/env python3

"""
This script checks that all non-imaging files in the given project satisfy the 
cumulative requirement from NDA. Meaning that all subjects who were included in 
past submissions have data in the current submission.

If no new data exists for a given subject in the current upload, the last record
of their information is copied into the current summary form upload with no
alterations required.

This script is to be executed after the summary_csv creation step has been ran.

Example function call: ./ndar_update_cumul_files.py -v --project mci_cb
"""

import os
import sys
import csv
import yaml
import pathlib
import argparse
import importlib
import pandas as pd
from pandas.errors import EmptyDataError
from typing import List, Dict

def update_curr_data(args, to_upload_dir: pathlib.Path, past_data: Dict):
    """
    Given location of current data to be uploaded and the dictionary of all past
    subject data organized via form key, check each form for any that are missing
    previously submitted subjects. 
    If a subject is found as missing from current upload, it will be appended to the
    summary file set to be uploaded.
    """

    for form in past_data.keys():
        # get list of past subjects to be added to current upload
        past_df = past_data[form]
        prev_uploaded_subjects = past_df['subjectkey'].to_list()
        to_upload_file = to_upload_dir / form
        to_upload_df = pd.read_csv(to_upload_file, skiprows=1)
        to_upload_subjects = to_upload_df['subjectkey'].to_list()
        subjects_to_add = [s for s in prev_uploaded_subjects if s not in to_upload_subjects]

        # Append past data to current file to be uploaded
        prev_subjects_data = past_df[past_df['subjectkey'].isin(subjects_to_add)]
        prev_subjects_data.to_csv(to_upload_file, mode='a', header=False, index=False, quoting=csv.QUOTE_NONNUMERIC, date_format='%m/%d/%Y')

        if args.verbose:
            print(f"INFO: Finished updating {form}, added {len(subjects_to_add)} past subjects data")

def get_past_data(args, non_imaging_files: List, prev_upload_dir: pathlib.Path) -> Dict:
    """
    Given a list of non-imaging files, the path to the previous uploaded dir, and the exent of
    past upload history to include, return a dictionary of past data.
    The key value pairs correspond to:
        non-imaging file: dataframe of all relevant past data for file type
    """
    past_data = {}

    if args.verbose:
        print("INFO: Searching full previous upload history")

    for file_type in non_imaging_files:
        dfs = []
        # if all past upload history needed, collect data from all previous upload directories
        for child_directory in prev_upload_dir.iterdir():
            if child_directory.is_dir():
                file_path = child_directory / file_type.name
                if file_path.exists():
                    try:
                        df = pd.read_csv(file_path, skiprows=1)
                    except EmptyDataError:
                        # if file is empty, skip
                        continue

                    df.set_index(['subjectkey', 'interview_date'], inplace=True)
                    dfs.append(df)
                    
        # Concatenate all DataFrames into a single DataFrame
        if dfs:
            combined_df = pd.concat(dfs)
            past_data[file_type.name] = combined_df
        else:
            raise FileNotFoundError("No files found.")

    if args.verbose:
        print("INFO: Selecting most recent data for all subjects in window.")

    # for each dataframe, make sure there is only the most recently uploaded data per subject
    for file_name, df in past_data.items():
        # Reset index to make 'subjectkey' and 'interview_date' columns and put date to 3rd col
        df_reset = df.reset_index()
        cols = df_reset.columns.tolist()
        cols.insert(2, cols.pop(cols.index('interview_date')))
        df_reset = df_reset[cols]

        # Convert 'interview_date' to datetime
        df_reset['interview_date'] = pd.to_datetime(df_reset['interview_date'], format='%m/%d/%Y')

        # Group by 'subjectkey' and get the row with the most recent 'interview_date'
        latest_df = df_reset.loc[df_reset.groupby('subjectkey')['interview_date'].idxmax()]

        past_data[file_name] = latest_df

    return past_data


def get_relevant_files(args, files_to_validate) -> List:
    """
    Given all files to validate for the project (list of paths), return list of non-imaging files that 
    should be cumulative.
    """
    non_imaging_files = [f for f in files_to_validate if "image03.csv" not in f.name]
    if args.verbose:
        print(f"INFO: Non-imaging files to update: {non_imaging_files}")

    return non_imaging_files


def get_summ_dirs(args, staging_path: pathlib.Path) -> tuple:
    """
    Given staging path from the sibis-general-config file,
    determine and return the locations of both the previously uploaded summary file
    dir and the current dir for sum data to be submitted.
    """
    prev_upload_dir = staging_path / "uploaded"
    to_be_uploaded_dir = staging_path / "staging" / "summaries"

    if not prev_upload_dir.is_dir() or not to_be_uploaded_dir.is_dir():
        print(f"ERROR: Uploaded dir ({prev_upload_dir}) or summary dir ({to_be_uploaded_dir}) doesn't exist, exiting.")
        sys.exit(1)

    if args.verbose:
        print(f"INFO: Set uploaded path: {prev_upload_dir} and current summaries path: {to_be_uploaded_dir}")

    return to_be_uploaded_dir, prev_upload_dir


def is_file(file_path: str) -> pathlib.Path:
    maybe_file = pathlib.Path(file_path).expanduser()
    if maybe_file.exists() and maybe_file.is_file():
        return maybe_file
    return argparse.ArgumentTypeError(f"file: {file_path} does not exist or is not a file.")


def _parse_args() -> argparse.Namespace:
    """Get config file, project, and extent of past record search"""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--sibis_general_config",
        help="Path of sibis-general-config.yml **in the current context**. Relevant if this is "
        "run in a container and the cases directory is mounted to a different "
        "location. Defaults to /fs/storage/share/operations/secrets/.sibis/.sibis-general-config.yml.",
        type=is_file,
        default="/fs/storage/share/operations/secrets/.sibis/.sibis-general-config.yml",
    )

    parser.add_argument(
        "-v", "--verbose",
        help="Verbose output of script progress",
        required=False, action="store_true"
    )

    parser.add_argument(
        "--project", help="Enter project name of summary files to be updated (cns_deficit, mci_cb, etc.)",
        required=True, type=str 
    )
    parser.add_argument(
        "--mappings_dir",
        help="Override directory that contains hivalc_mappings.py / ncanda_mappings.py",
        type=pathlib.Path,
        required=False,
    )

    args = parser.parse_args()
    return args

def main():
    args = _parse_args()

    if not args.sibis_general_config.exists():
        raise ValueError(f"ERROR: {args.sibis_general_config} does not exist")
    with open(args.sibis_general_config, "r") as f:
        config = yaml.safe_load(f)
    # config = config.get("ndar").get(args.project)
    if 'ndar' in config and args.project in config['ndar']:
        project_config = config['ndar'][args.project]
    else:
        raise KeyError(f"ERROR: Project '{args.project}' not found in 'ndar' section of the config file.")

    # Resolve mappings dir (CLI override wins; fallback to project config)
    mappings_dir = args.mappings_dir or project_config.get('mappings_dir')
    if not mappings_dir:
        raise KeyError("No mappings_dir provided via --mappings_dir and none found in project config.")
    mappings_dir = pathlib.Path(mappings_dir)
    if not mappings_dir.exists():
        raise FileNotFoundError(f"mappings_dir does not exist: {mappings_dir}")
    sys.path.insert(0, str(mappings_dir.resolve()))

    # Import the right mappings module from the resolved directory
    mappings_module_name = 'ncanda_mappings' if args.project == 'ncanda' else 'hivalc_mappings'
    mappings = importlib.import_module(mappings_module_name)

    # NOTE: pass the *project* block (flat keys expected by mappings.get_paths_from_config)
    consent_path, staging_path, src_data_path, files_to_validate, data_dict_path, upload2ndar_path = mappings.get_paths_from_config(args, project_config)

    # For the given project, get the current and previous data dirs
    to_be_uploaded_dir, prev_upload_dir = get_summ_dirs(args, staging_path)
    
    # Get list of relevant non_imaging files to check
    non_imaging_files = get_relevant_files(args, files_to_validate)

    # Gather relevant past uploaded data for each non-imaging file type
    past_data = get_past_data(args, non_imaging_files, prev_upload_dir)

    # Compare and update current data to be uploaded
    update_curr_data(args, to_be_uploaded_dir, past_data)

if __name__ == "__main__":
    main()