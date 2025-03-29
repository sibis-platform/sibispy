#!/usr/bin/env python
"""
This script retrieves the sibis-general-config from either the passed location or /fs/storage/share/operations/secrets/.sibis/.sibis-general-config.yml if no location is passed. From this, it extracts three locations:
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


def stretch_list(l: list, n: int):
    # stretch_list([1,2,3], 2) -> [1,1,2,2,3,3]
    stretched_list = [x for x in l for _ in range(n)]
    return stretched_list


def stringify_list(l: list):
    l = [str(x) for x in l]
    return "\n".join(l)


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
    "image03.csv": CSVMeta("image03_definitions.csv", ["image", "03"]),
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
    
    # map data dictionary types to actual data types
    for row in data_dict_dtype:
        data_dict_dtype[row] = HEADERTYPE_MAP[data_dict_dtype[row]]

    # pass data dictionary to pandas to load values with datatypes, skipping the first line
    old_summary_df = pd.read_csv(old_summary_csv, skiprows=1, dtype=data_dict_dtype)
    new_subject_df = pd.read_csv(new_subj_csv, skiprows=1, dtype=data_dict_dtype)

    # convert the date columns after loading, if they exist
    for col in parse_dates:
        if col in old_summary_df.columns:
            old_summary_df[col] = pd.to_datetime(old_summary_df[col], format='%m/%d/%y', errors='ignore')
        if col in new_subject_df.columns:
            new_subject_df[col] = pd.to_datetime(new_subject_df[col], format='%m/%d/%y', errors='ignore')

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
        if isinstance(visit, str):
            shutil.copytree(str(visit_path), str(destination_path / visit), dirs_exist_ok=True)
        else:
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
    config_args.add_argument(
        "-v", "--verbose", help="Verbose operation", action="store_true"
    )
    config_args.add_argument(
        "-r", "--do_not_remove", help="Do not remove files once they pass validation", action="store_true"
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
            format="%(levelname)s: %(message)s", level=logging.DEBUG
        )
        logging.info("Verbose output.")
    else:
        logging.basicConfig(format="%(levelname)s: %(message)s")

    logging.info(f"Loading sibis-general-config from {args.sibis_general_config}")
    args.sibis_general_config = pathlib.Path(args.sibis_general_config)

    return args


def main():
    args = _parse_args()

    if not args.sibis_general_config.exists():
        raise ValueError(f"{args.sibis_general_config} does not exist")
    with open(args.sibis_general_config, "r") as f:
        config = yaml.safe_load(f)
    config = config.get("ndar").get(args.project)

    # Add mappings file to system path and import
    sys.path.append(config['mappings_dir'])
    if args.project == 'ncanda':
        globals()['mappings'] = __import__('ncanda_mappings')
    else:
        globals()['mappings'] = __import__('hivalc_mappings')

    # Get base paths for everything from config
    consent_path, staging_path, src_data_path, files_to_validate, data_dict_path, upload2ndar_path = mappings.get_paths_from_config(args, config)

    # Get list of all available visits who have consent
    subj_list = mappings.get_subj_list(args, consent_path)
    logging.info(f"INFO: Found {len(subj_list)} eligible subjects to include in submission package.")

    # Filter visit list to visits for this upload
    filtered_visit_list = mappings.filter_visit_list(args, subj_list, upload2ndar_path)
    logging.info(f"INFO: Post filtering event count to upload: {len(filtered_visit_list)}")

    # Generate list of imaging modalities to include
    image_mods = [str(f.parent) for f in files_to_validate if f.name == 'image03.csv']

    summaries_dir = staging_path / 'staging' / 'summaries'

    # For each subject in visit list, for all scans found within copy their src files to summaries and append image03 as row to summ
    for visit_path in filtered_visit_list:
        for mod in image_mods:
            mod_dir = visit_path / mod  # Construct the path for the modality
            if mod_dir.is_dir():
                for item in mod_dir.iterdir():
                    if item.name == 'image03.csv':
                        if item.is_file():
                            image03_summ_csv = summaries_dir / 'image03.csv'
                            if not image03_summ_csv.is_file():   # Create summary csv if it doesn't already exist
                                # Copy current image row csv and use as base of new summary file
                                dest_dir = image03_summ_csv.parent
                                dest_dir.mkdir(parents=True, exist_ok=True)
                                shutil.copy2(item, image03_summ_csv)
                                logging.info(f"No image03 summary file found, now created via copy to path {image03_summ_csv}")
                            else:
                                # Append new row to summary file
                                write_summary_csv(image03_summ_csv, item, data_dict_path)
                    elif 'NDAR' in item.name:
                        if '.json' in item.name:
                            logging.info(f"Copying {item} to {summaries_dir}")
                            shutil.copy2(item, (summaries_dir / item.name))
                        else:
                            logging.info(f"Copying {item} to {summaries_dir}")
                            shutil.copytree(item, summaries_dir / item.name, dirs_exist_ok=True)

    logging.info("Finished copying")
    
    


if __name__ == "__main__":
    main()
