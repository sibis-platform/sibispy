#!/usr/bin/env python3

"""
This script checks that all non-imaging files in the given project satisfy the 
cumulative requirement from NDA. Meaning that all subjects who were included in 
past submissions have data in the current submission.

If no new data exists for a given subject in the current upload, the last record
of their information is copied into the current summary form upload with no
alterations required.

This script is to be executed after the summary_csv creation step has been ran.

Example function call:
"""

import os
import sys
import yaml
import pathlib
import argparse
import pandas as pd
from pandas.errors import EmptyDataError
from typing import List, Dict

def update_curr_data(to_be_uploaded_dir: pathlib.Path, past_data: Dict):
    """
    Given location of current data to be uploaded and the dictionary of all past
    subject data organized via form key, check each form for any that are missing
    previously submitted subjects. 
    If a subject is found as missing from current upload, it will be appended to the
    summary file set to be uploaded.
    """
    # TODO:


    return updated_data

def get_past_data(non_imaging_files: List, prev_upload_dir: pathlib.Path, full_history: bool) -> Dict:
    """
    Given a list of non-imaging files, the path to the previous uploaded dir, and the exent of
    past upload history to include, return a dictionary of past data.
    The key value pairs correspond to:
        non-imaging file: dataframe of all relevant past data for file type
    """
    past_data = {}

    for file_type in non_imaging_files:
        if full_history:
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
        else:
            # only get most recently uploaded dir data
            # TODO: Not sure if the approach of only getting most recent dir is useful or not...
            continue

    # for each dataframe, make sure there is only the most recently uploaded data per subject
    for file_name, df in past_data.items():
        # Reset index to make 'subjectkey' and 'interview_date' columns
        df_reset = df.reset_index()

        # Convert 'interview_date' to datetime
        df_reset['interview_date'] = pd.to_datetime(df_reset['interview_date'], format='%m/%d/%Y')

        # Group by 'subjectkey' and get the row with the most recent 'interview_date'
        latest_df = df_reset.loc[df_reset.groupby('subjectkey')['interview_date'].idxmax()]

        past_data[file_name] = latest_df

    return past_data


def get_relevant_files(files_to_validate) -> List:
    """
    Given all files to validate for the project (list of paths), return list of non-imaging files that 
    should be cumulative.
    """
    non_imaging_files = [f for f in files_to_validate if "image03.csv" not in f.name]
    return non_imaging_files


def get_summ_dirs(staging_path: pathlib.Path) -> tuple:
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
        "--project", help="Enter project name of summary files to be updated (cns_deficit, mci_cb, etc.)",
        required=True, type=str 
    )

    parser.add_argument(
        "-f", "--full_history", 
        help="Check all previosuly uploaded files for subject data. Useful for initial cumulation.",
        required=False, action="store_true"
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

    sys.path.append(project_config.get('mappings_dir'))

    if args.project == 'ncanda':
        import ncanda_mappings as mappings
    else:
        import hivalc_mappings as mappings

    # Get base paths for everything from config
    staging_path, data_path, consent_path, files_to_validate, data_dict_path = mappings.get_paths_from_config(args, project_config)

    # For the given project, get the current and previous data dirs
    to_be_uploaded_dir, prev_upload_dir = get_summ_dirs(staging_path)
    
    # Get list of relevant non_imaging files to check
    non_imaging_files = get_relevant_files(files_to_validate)

    # Gather relevant past uploaded data for each non-imaging file type
    past_data = get_past_data(non_imaging_files, prev_upload_dir, args.full_history)

    # Compare and update current data to be uploaded
    updated_data = update_curr_data(to_be_uploaded_dir, past_data)



if __name__ == "__main__":
    main()