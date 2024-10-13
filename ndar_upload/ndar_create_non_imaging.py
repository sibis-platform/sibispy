#!/usr/bin/env python3
"""
This script creates the non-imaging cumulative data for all subjects
that are to be included in the indicated release.
For HIVALC projects, that subset is determined by all previously uploaded   ts + 
visits that have occurred since last upload.
- Given that subset we then take the latest subject info from the production summaries files.
For NCANDA, the subset is determined by all subjects who have consented to release their
data, and it the data included is baseline -> indicated release year.
- Given that subject, we then take the latest subject info from the corresponding internal
snapshot release summaries files.

For all projects, once we have the latest relevant source data, we then apply the conversions
found in the operations mapping files to get ndar compliant data. From that, the summary
csv files are then generated that adhere to ndar's format standards.
"""

import os
import re
import csv
import sys
import yaml
import pathlib
import argparse
import numpy as np
import pandas as pd

def shift_binary(src_col, src_vals, new_bin_vals):
    """
    For cases where NDAR binary values are different than source (0,1 vs. 1,2)
    Given a source column and target value range, shift binary values to match
    NDAR expected value range.
    """
    return src_col.replace(src_vals, new_bin_vals)


def recode_missing(src_col, missing_value):
    """
    Given a column that is required and specifies a specific value to be used in the case of missing values,
    fill in the empty values of the src column with the missing value and return to fill target df.
    """
    filled_col = src_col.fillna(missing_value)
    return filled_col


def test_reverse(map_row):
    """
    If ndar scores values as reversed, reverse our values to match
    Example of reversed:
    NCANDA Range value: [1:7], 1=Disagree strongly; 2=Disagree moderately; etc.
    NDAR Range value: [1:7], 7=Disagree strongly; 6=Disagree moderately; etc.
    """
    reverse_str = "reverse scored"
    notes_str = map_row['Notes'].values[0]
    if isinstance(notes_str, str) and reverse_str in notes_str.lower():
        return True
    return False

def reverse_val(val, ndar_range, ncanda_range):
    """Reverse value to match NDAR value range meaning (assuming range is same, just reversed meaning)"""
    try:
        if pd.isna(val):
            return val
        # Get the inverse position of the value
        ncanda_idx = ncanda_range.index(val)
        new_value = ndar_range[ncanda_idx]
        return new_value
    except (ValueError, IndexError) as e:
        print(f"ERROR: Failed to reverse value {val} due to {e}")
        return val

def reverse_vals(target_vals, map_row, def_df):
    """Prepare ranges and apply reverse_val function to the entire column"""
    try:
        ndar_var = map_row['NDA_ElementName'].values[0]
        field_spec = def_df[def_df['ElementName'] == ndar_var]
        ndar_range_str = field_spec['ValueRange'].values[0].split(';')[0]  # e.g., '1::7'
        ndar_range_ends = list(map(int, re.findall(r'\d+', ndar_range_str)))
        ndar_range = list(range(ndar_range_ends[0], ndar_range_ends[1] + 1))
        ndar_range.reverse()  # Reverse NDAR range

        ncanda_range_str = map_row['ncanda_value_range'].values[0]
        ncanda_range = list(map(int, re.findall(r'\d+', ncanda_range_str)))

        # Apply reverse_val to the entire column
        reversed_vals = target_vals.apply(lambda val: reverse_val(val, ndar_range, ncanda_range))
        return reversed_vals
    except Exception as e:
        print(f"ERROR: Failed to process reverse values: {e}")
        return target_vals  # Return the original if something goes wrong

def safe_int_conv(val):
    """
    Given a value, safely convert it from float to int.
    This is used as part of an apply function for cases where typical column type casting fails.
    """
    if not pd.isna(val):
        return int(round(val))
    return pd.NA

# Mapping between NDAR data types to Python/Pandas types
dtype_mapping = {
    'GUID': 'object',
    'Date': 'datetime64[ns]',
    'String': 'object',
    'Integer': 'Int64',
    'Float': 'Float64',
}

def check_dtype_category(col):
    if pd.api.types.is_object_dtype(col):
        return 'object'
    elif pd.api.types.is_datetime64_any_dtype(col):
        return 'datetime64[ns]'
    elif pd.api.types.is_int64_dtype(col):
        return 'Int64'
    elif pd.api.types.is_float_dtype(col):
        return 'Float64'
    else:
        return 'Unknown'

def convert_to_nda_format(raw_ndar_values, new_df_col):
    """
    Given a df column of newly converted NDAR values along with the NDAR required data format, apply a safe 
    conversion to the format. Return the converted values to be assigned to the new output target df.
    """
    # Determine source and target datatypes
    src_dtype = check_dtype_category(raw_ndar_values)
    target_dtype = check_dtype_category(new_df_col)

    # Apply appropriate conversion
    if src_dtype == target_dtype:
        # no conversion required
        return raw_ndar_values

    try:
        if target_dtype == 'object':
            # Convert everything to string if target is object
            return raw_ndar_values.astype(object).where(raw_ndar_values.notna(), '').astype(str)
        elif target_dtype == 'datetime64[ns]':
            # Convert to datetime
            vals = pd.to_datetime(raw_ndar_values, errors='coerce')
            formatted_vals = vals.dt.strftime('%m/%d/%Y')
            return formatted_vals
        elif target_dtype == 'Int64':
            # Convert to nullable integer
            if src_dtype == 'object':
                return pd.to_numeric(raw_ndar_values, errors='coerce').astype('Int64')
            else:
                return raw_ndar_values.apply(lambda val: safe_int_conv(val)).astype('Int64')
        elif target_dtype == 'Float64':
            # Convert to float
            return pd.to_numeric(raw_ndar_values, errors='coerce').astype('float')
        else:
            return raw_ndar_values  # No conversion available
    except Exception as e:
        print(f"ERROR: Failed converting column: {e}. NDAR COL: {raw_ndar_values}")
        return raw_ndar_values  # Return unmodified values in case of error


def fill_rows(df, column, value):
    """
    Given a value, fill all rows of a particular dataframe with that value.
    This is used in subject/measurements maps to return a value via eval statement.
    """
    df[column] = value
    return


def get_measurement_timept(demographics):
    """Return visit number/name for measurements required fields"""
    try:
        timept = int(re.search(r"\d+", demographics["visit"]).group())
    except AttributeError:
        # visit is baseline, return 0 to indicate baseline visit
        timept = 0
    return timept


def get_race(race: int) -> str:
    RACE_MAP = mappings.race_map
    try:
        race = RACE_MAP[race]
    except KeyError:
        race = "Unknown or not reported"
    return race


def write_ndar_files(staging_path, target_dfs):
    """
    Given dictionary of target dataframes to generate, write out a new NDAR summary file for each
    file key to the staging path summaries dir. Then return total number of files written.
    """
    output_dir = staging_path / 'staging' / 'summaries'
    if not output_dir.exists():
        output_dir.mkdir(parents=True, exist_ok=True)

    files_written = 0

    for file_name, df in target_dfs.items():
        # Set output file path
        output_file_path = output_dir / (file_name + '.csv')
        # Set NDAR file type header
        file_type_header = f'"{file_name[:-2]}","{file_name[-2:]}"\n'

        # Fill NaN's with ""
        #df = df.replace(np.nan, "")
        # Write out file
        with open(output_file_path, 'w') as f:
            f.write(file_type_header)
            df.to_csv(f, index=False, quoting=csv.QUOTE_NONNUMERIC, na_rep="")

        files_written += 1

    return files_written, output_dir


def gen_target_dfs(src_dfs: dict, target_data_specs: dict):
    """
    Given a dict of source data dataframes (src format) and a dict of target data
    mappings and definitions files (NDAR format), create a df for each target file and 
    convert source data to match target specifications
    """
    target_dfs = {}
    
    for ndar_file, spec_dfs in target_data_specs.items():
        print(f"WORKING ON {ndar_file}")
        column_names = target_data_specs[ndar_file]['definitions']['ElementName'].values
        data_types = target_data_specs[ndar_file]['definitions']['DataType'].values
        if ndar_file == 'ndar_subject01':
            mappings_df = None
        else:
            mappings_df = target_data_specs[ndar_file]['mappings']

        definitions_df = target_data_specs[ndar_file]['definitions']

        # Create a dictionary to map columns to their data types
        column_dtypes = {col: dtype_mapping[dt] for col, dt in zip(column_names, data_types)}

        # Create a new empty dataframe with NDAR vars and datatypes
        new_df = pd.DataFrame(columns=column_names)
        new_df = new_df.astype(column_dtypes)

        for col in new_df.columns:
            if col in mappings.src_to_ndar_map:
                conversion_func = mappings.src_to_ndar_map.get(col)
                if 'fill_rows' in conversion_func:
                    # we can assume that fill_rows sets datatype correctly
                    converted_vals = eval(conversion_func)
                else:
                    converted_vals = eval(conversion_func)
                    new_df[col] = convert_to_nda_format(converted_vals, new_df[col])
            else:
                if ndar_file == 'ndar_subject01':
                    continue

                # Check if there is a direct match between src and ndar values
                row = mappings_df[mappings_df['NDA_ElementName'] == col]
                try:
                    src_var = row['src_variable'].values[0]
                    src_csv = row['src_csv'].values[0]
                    if pd.isna(src_var):
                        # Leave target variables empty that don't have an src variable match
                        continue
                    else:
                        if pd.isna(src_csv):
                            print(f"WARNING: Found matched src Var '{src_var}' without a source csv listed in {ndar_file} mappings. Leaving blank.")
                            continue
                        else:
                            try:
                                # Copy direct match src vars to target df
                                target_vals = src_dfs[src_csv][src_var]
                                # Test if they need to be reverse scored
                                if test_reverse(row):
                                    target_vals = reverse_vals(target_vals, row, definitions_df)

                                new_df[col] = convert_to_nda_format(target_vals, new_df[col])
                            except KeyError as e:
                                print(f"ERROR: Failed to get target vals for {src_var} from {src_csv}")
                                continue

                except IndexError as e:
                    # NDAR value not in mappings file, leave blank
                    continue
        # Add newly created df to target dfs
        target_dfs[ndar_file] = new_df

    return target_dfs


def get_target_data_specs(files_to_validate: list, data_dict_path: pathlib.Path):
    """
    Given a list of paths of files to validate and the target base directory for data dictionarys,
    read in all non-imaging definitions and mappings files, and return a dictionary of
    dataframes with the key being the specific mapping/definition file name.

    Note that definition files == NDAR data dictionaries for their forms
    Mappings files == conversion definition files of how to get from NCANDA source data to 
    their expected format.

    target data spec format example:
    target_data_specs = {
        measurement_file_name: {
            'mappings': *mappings_df*,
            'definitions': *definitions_df*
        }
    }
    """
    target_data_specs = {}
    non_imaging_files = {f for f in files_to_validate if f.stem != 'image03'}
    for f in non_imaging_files:
        if str(f.parent) == 'measurements':
            map_f_path = data_dict_path / 'measurements' / (f.stem + '_mappings.csv')
            def_f_path = data_dict_path / 'measurements' / (f.stem + '_definitions.csv')

            try:
                map_df = pd.read_csv(map_f_path)
                def_df = pd.read_csv(def_f_path)
            except FileNotFoundError as e:
                print(f"ERROR: Specification file not found: {e}")
                continue
            except pd.errors.EmptyDataError as e:
                print(f"ERROR: Empty CSV file: {e}")
                continue
        
            # Initialize the dictionary if not already present
            if f.stem not in target_data_specs:
                target_data_specs[f.stem] = {}

            target_data_specs[f.stem]['mappings'] = map_df
            target_data_specs[f.stem]['definitions'] = def_df

        elif str(f.stem) == 'ndar_subject01':
            new_f_name = f.stem + '_definitions.csv'
            new_f_path = data_dict_path / new_f_name
            
            try:
                def_df = pd.read_csv(new_f_path)
            except FileNotFoundError as e:
                print(f"File not found: {e}")
                continue
            except pd.errors.EmptyDataError as e:
                print(f"Empty CSV file: {e}")
                continue

            if f.stem not in target_data_specs:
                target_data_specs[f.stem] = {}

            target_data_specs[f.stem]['definitions'] = def_df
            target_data_specs[f.stem]['mappings'] = None
        else:
            # skip image03 files
            continue

    return target_data_specs


def fill_src_dfs(src_dfs):
    """
    Given source dataframes, make sure that all visits are included across all dfs. Also assure that the order accross them
    are the same
    """
    # Demographics has the full list of visits to include
    demographics_df = src_dfs["demographics"]

    for df_name, df in src_dfs.items():
        # Get the missing subject, visit rows from demographics
        missing_rows = demographics_df.loc[
            ~demographics_df[['subject', 'visit']].apply(tuple, axis=1).isin(df[['subject', 'visit']].apply(tuple, axis=1)),
            ['subject', 'visit']
        ]
        
        # Append missing subject, visit rows to the current DataFrame, keeping other columns NaN
        updated_df = pd.concat([df, missing_rows], axis=0, ignore_index=True)

        # Reassign the updated DataFrame to the dictionary
        src_dfs[df_name] = updated_df.sort_values(by=['subject', 'visit']).reset_index(drop=True)

    return src_dfs


def get_config_data(args):
    """
    Given args, test that the given project is valid for the current sibis-general-config file, and return
    a dictionary object that contains all relevant config data for the project. Note these are template paths
    that will be filled in later w/ correct data for current run.

    Structure of config:
    {'staging_directory': str, 
    'data_directory': str, 
    'cases_directory': str, 
    'data_dict_directory': str, 
    'consent_directory': str,
    'mappings_dir': str, 
    'collection_id': int, 
    'files_to_validate': ['str', ...], 
    'files_to_upload': ['str', ...]}
    """
    # Test that it is a valid sibis-general-config file
    cfg_file = pathlib.Path(args.sibis_general_config) if pathlib.Path(args.sibis_general_config).is_file() else None
    if not cfg_file:
        print(f"ERROR: Invalid config file path {cfg_file}. Exiting.")
        sys.exit(1)

    with cfg_file.open("r") as config_file:
        cfg = yaml.safe_load(config_file)

        try:
            config = cfg['ndar'][args.project]
        except KeyError:
            print(f"ERROR: Could not find {args.project} under ndar in config file {cfg_file}. Exiting.")
            sys.exit(1)

    return config


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sibis_general_config",
        help="Path of sibis-general-config.yml **in the current context**. Relevant if this is "
        "run in a container and the cases directory is mounted to a different "
        "location. Defaults to /fs/storage/share/operations/secrets/.sibis/.sibis-general-config.yml.",
        default="/fs/storage/share/operations/secrets/.sibis/.sibis-general-config.yml",
    )
    parser.add_argument(
        "--project",
        help="Project to generate non-imaginge data for. E.g. ncanda, mci_cb, cns_deficit, etc.",
        type=str,
        action="store",
        required=True
    )
    parser.add_argument(
        "--followup-year",
        help="The follow-up year timepoint for NCANDA project. E.g., 0 (baseline), 1, 2...",
        type=str,
        action="store"
    )

    args = parser.parse_args()

    # Check if project is ncanda and ensure followup-year is provided
    if args.project.lower() == "ncanda" and args.followup_year is None:
        print("Error: The '--followup-year' argument is required for the NCANDA project.", file=sys.stderr)
        sys.exit(1)

    return args


def main():
    args = _parse_args()

    config_data = get_config_data(args)

    # Add mappings file to system path and import
    sys.path.append(config_data['mappings_dir'])
    if args.project == 'ncanda':
        globals()['mappings'] = __import__('ncanda_mappings')
    else:
        globals()['mappings'] = __import__('hivalc_mappings')

    # Set file paths for specific run from config path templates
    consent_path, staging_path, src_data_path, files_to_validate, data_dict_path, upload2ndar_path = mappings.get_paths_from_config(args, config_data)

    # Get list of all available visits who have consent
    subj_list = mappings.get_subj_list(args, consent_path)
    print(f"INFO: Found {len(subj_list)} subjects eligible to include in submission package.")

    # Pull all subject data from source data summary files
    raw_subj_dfs = {}
    # for csv_file in src_data_path.glob("*.csv"):      # After this release, will transition to this approach
        # if csv_file.name == 'demographics.csv':
        #     # replace demographics path with consent path base to get latest information
        #     csv_file = consent_path / 'demographics.csv'
    for csv_file in consent_path.glob("*.csv"):
        df = pd.read_csv(csv_file, low_memory=False)
        raw_subj_dfs[csv_file.stem] = df

    # Add any additional source data that is outside of src_data_path
    raw_subj_dfs = mappings.add_additional_src_data(raw_subj_dfs, src_data_path)
    
    # Filter source data so it only contains data for this run
    src_dfs = mappings.filter_raw_dfs(args, raw_subj_dfs, subj_list, src_data_path, staging_path)

    # Make sure that every src df has every subject and every visit desired
    src_dfs = fill_src_dfs(src_dfs)

    # Read in all relevant NDAR defintion and mappings files
    target_data_specs = get_target_data_specs(files_to_validate, data_dict_path)
    
    # Generate dataframes for each definition file
    target_dfs = gen_target_dfs(src_dfs, target_data_specs)

    # Write out converted ndar files
    files_written, output_dir = write_ndar_files(staging_path, target_dfs)

    print(f"Wrote {files_written} files to {output_dir}")


if __name__ == "__main__":
    main()
