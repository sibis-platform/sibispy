#!/usr/bin/env python3
import logging
import re
import pathlib
import pandas as pd
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger("ndar_create_csv")

#
# Ndar individual csv creation mappings
#
race_map = {
    1: "American Indian/Alaskan Native",
    2: "Asian",
    3: "Black or African American",
    4: "Hawaiin or Pacific Islander",
    5: "White",
    6: "Other",
    8: "Unknown or not reported",
    9: "Unkown or not reported",
}
      
phenotype_map = {
    "TBD": 'Not Yet Indicated',
    "NOR": 'Normal',
    "SBA": 'Structural Brain Anomaly',
    "EDB": "Exceeds Baseline Drinking Criteria",
    "AUD": "Alcohol Use Disorder",
}

subject_map = {
    "subjectkey": "get_subjectkey(subject)",
    "src_subject_id": 'lambda subject, *_: subject.demographics["subject"]',
    "interview_date": 'lambda subject, *_: ncanda_funcs.convert_ncanda_interview_date(str(subject.demographics["visit_date"]))',
    "interview_age":  'lambda subject, *_: int(float(subject.demographics["visit_age"]))',
    "sex": 'lambda subject, *_: subject.demographics["sex"]',
    "race": 'get_race(sys_values, int(subject.demographics["race"]))',
    "phenotype": 'lambda subject, *_: ncanda_funcs.get_ncanda_pheno(subject.demographics, "pheno")',
    "phenotype_description": 'lambda subject, *_: ncanda_funcs.get_ncanda_pheno(subject.demographics, "desc")',
    "twins_study": "No",
    "sibling_study": "No",
    "family_study": "No",
    "sample_taken": "No",
    "family_user_def_id": 'lambda subject, *_: subject.demographics["family_id"]',
    "ethnic_group": 'lambda subject, *_: ncanda_funcs.get_ncanda_ethn(subject.demographics["hispanic"])',
}

image_map = {
    "subjectkey": 'get_subjectkey',
    "src_subject_id": 'lambda subject, *_: subject.demographics["subject"]',
    "interview_date": 'lambda subject, *_: ncanda_funcs.convert_ncanda_interview_date(str(subject.demographics["visit_date"]))',
    "interview_age":  'lambda subject, *_: int(float(subject.demographics["visit_age"]))',
    "sex": 'lambda subject, *_: subject.demographics["sex"]',
    "image_description": 'get_image_description',
    "image_modality": "MRI",
    "scan_type": 'get_scan_type',
    "scan_object": "Live",
    "image_file_format": "NIFTI",
    "scanner_manufacturer_pd": 'get_dicom_value("device", "Manufacturer")',
    "scanner_type_pd": 'get_dicom_value("device", "ManufacturerModel")',
    "scanner_software_versions_pd": 'get_dicom_value("device", "SoftwareVersions")',
    "magnetic_field_strength": 'get_dicom_value("mr", "MagneticFieldStrength")',
    "transformation_performed": "No",
    "image_unit1": 'get_image_units',
    "image_unit2": 'get_image_units',
    "image_unit3": 'get_image_units',
    "manifest": "dataset_description.json",
    "mri_repetition_time_pd": 'get_dicom_value("mr", "RepetitionTime")',
    "mri_echo_time_pd": 'get_dicom_value("mr", "EchoTime")',
    "flip_angle": 'get_dicom_value("mr", "FlipAngle")',
    "acquisition_matrix": 'get_dicom_value("mr", "AcquisitionMatrix")',
    "mri_field_of_view_pd": 'get_field_of_view_pd',
    "patient_position": 'get_patient_position',
    "photomet_interpret": 'get_dicom_value("mr", "PhotometricInterpretation")',
    "image_num_dimensions": 3,
    "image_extent1": 'get_nifti_value("XFOV")',
    "image_extent2": 'get_nifti_value("YFOV")',
    "image_extent3": 'get_nifti_value("ZFOV")',
    "image_resolution1": 'get_nifti_value("XPIX")',
    "image_resolution2": 'get_nifti_value("YPIX")',
    "image_resolution3": 'get_nifti_value("ZPIX")',
    "image_slice_thickness": 'get_dicom_value("mr", "SliceThickness")',
    "image_orientation": 'get_image_orientation',
    "bvek_bval_files": 'has_bvek_bval_files',
}

#
# Ndar individual csv creation functions
#
def convert_ncanda_interview_date(date):
    try:
        interview_date = datetime.strptime(date, "%m/%Y").strftime("%m/01/%Y")
    except:
        # no interview_date from demographics file
        interview_date = "XX/XXXX"
    return interview_date

def anomoly_scan(demo_data, current_visit) -> bool:
    """Helper function to return if current scan has an anomoly"""
    if pd.isnull(demo_data['ndar_guid_anomaly_visit']) or demo_data['ndar_guid_anomaly_visit'] == '':
        logging.error(f"Anomaly scan indicated. Could not find anomaly visit value for {demo_data['subject']}.")
        return False
    if current_visit >= int(demo_data['ndar_guid_anomaly_visit']):
        # current scan is an anomoly, add this to phenotype
        return True
    
def aud_scan(demo_data, current_visit) -> bool:
    """Helper function to return if current scan is AUD"""
    if pd.isnull(demo_data['ndar_guid_aud_dx_initial']) or demo_data['ndar_guid_aud_dx_initial'] == '':
        logging.error(f"AUD scan indicated. Could not find AUD visit value for {demo_data['subject']}.")
        return False
    if current_visit >= int(demo_data['ndar_guid_aud_dx_initial']):
        return True
    
def get_ncanda_pheno(demo_data, desired):
    """Return the desired phenotype component (either phenotype or its description)"""
    phenotype, phenotype_description = get_ncanda_phenotype_all(demo_data)
    if desired == "pheno":
        return phenotype
    else:
        return phenotype_description

def get_ncanda_phenotype_all(demo_data) -> str:
    """
    Get the phenotype and phenotype description
    possible diagnosis + description:
    normal, structural brain anomaly, Exceeds Baseline Drinking Criteria, 
    Exceeds Baseline Drinking Criteria & structural brain anomaly, AUD, 
    AUD & structural brain anomaly
    # SBA&EDB&AUD -- order to report
    """
    phenotype = None
    phenotype_description = None
    try:
        anomaly_flag = int(demo_data['ndar_guid_anomaly']) # SBA 
        aud_flag = int(demo_data['ndar_guid_aud_dx_followup']) # AUD
        exceed_flag = int(demo_data['exceeds_bl_drinking_2']) # EDB
    except:
        # demographics files are not yet updated
        phenotype = "TBD"
        phenotype_description = "Not Yet Indicated"
        return phenotype, phenotype_description

    flag_list = [anomaly_flag, aud_flag, exceed_flag]
    for flag in flag_list:
        if pd.isnull(flag) or flag == '':
            phenotype = "TBD"
            phenotype_description = "Not Yet Indicated"
            return phenotype, phenotype_description

    current_visit = int(re.sub("[^0-9]", "", demo_data['visit'])) 
    # if subject is normal, return phenotype, phenotype_description:
    if anomaly_flag == 0 and aud_flag == 0 and exceed_flag == 0:
        phenotype = "NOR"
        phenotype_description = "Normal"
        return phenotype, phenotype_description
    # check for anomaly first
    if anomaly_flag != 0 and anomoly_scan(demo_data, current_visit):
        phenotype = "SBA"
        phenotype_description = "Structural Brain Anomaly"
    # check for exceeds baseline drinking
    if exceed_flag != 0:
        if phenotype is not None:
            phenotype += "&EDB"
            phenotype_description += " & Exceeds Baseline Drinking Criteria"
        else:
            phenotype = "EDB"
            phenotype_description = "Exceeds Baseline Drinking Criteria"
    # check for aud 
    if aud_flag != 0 and aud_scan(demo_data, current_visit):
        if phenotype is not None:
            phenotype += "&AUD"
            phenotype_description += " & Alcohol Use Disorder"
        else:
            phenotype = "AUD"
            phenotype_description = "Alcohol Use Disorder"

def get_ncanda_ethn(ethn):
    """Input whether hispanic or not"""
    if ethn == "Y":
        ethnic_group = "Hispanic/Latino"
    else:
        ethnic_group = "Non-Hispanic/Latino"
    return ethnic_group


#
# Ndar summary csv creation functions
#
@dataclass(frozen=True)
class StagingPaths:
    staging: pathlib.Path = pathlib.Path("staging")
    validation_errors: pathlib.Path = staging / "validation_errors"
    waiting_for_consent: pathlib.Path = staging / "waiting_for_consent"

def set_visit_path(args, staging_path, data_path):
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
        logging.info(f"Ncanda project doesn't have study designations. Directing to check_new.")
        path_to_visits = data_path
    else:  # if args.check_validation
        path_to_visits = staging_path / StagingPaths.validation_errors
    
    return path_to_visits

def get_visit_path(args, path_to_visits, visit):
    visit_path = (path_to_visits / visit)
    return visit_path

def set_consent_path(args, config):
    """Returns path to demographics csv template. Subject needs to be dynamically replaced"""
    # 
    cases_base = pathlib.Path(config.get('cases_directory'))
    followup_year = "followup_" + args.followup_year + "y"
    base_release = cases_base / followup_year
    snaps_dir_sub_ver = "NCANDA_SNAPS_" + args.followup_year + "Y_REDCAP" + "_"
    latest_dir = sorted(base_release.glob(snaps_dir_sub_ver + "*"))[-1]
    consent_path = base_release / latest_dir / "cases" / "SUBJECT" / "standard" / followup_year / "measures" / "demographics.csv"
    return consent_path

def get_paths_from_config(args, config):
    '''
    This returns a tuple:
        - staging_path: the base dir for the ndar staging workflow
        - data_path: the base dir where ndar csv files and imagery that are to be located live
        - consent_path: the base dir where consent data can be located
        - files_to_validate: a list of filenames in ndar csv format that should be validated
    '''
    try:
        staging_path = pathlib.Path(
            config.get("staging_directory").replace('*', args.followup_year)
        )  # e.g. /fs/neurosci01/ncanda/releases/public/followup_*y/ndar
    except:
        raise ValueError(f"No staging_directory in {args.sibis_general_config}")

    try:
        data_path = pathlib.Path(
            config.get("data_directory")
        )  # e.g. /fs/neurosci01/ncanda/releases/public/upload2ndar
    except:
        raise ValueError(f"No data_directory in {args.sibis_general_config}")

    try:
        # e.g. /fs/neurosci01/ncanda/releases/internal/followup_*y/ \
        # NCANDA_SNAPS_*Y_REDCAP_V**/cases/SUBJECT/standard/followup_*y/measures/demographics.csv
        consent_path = set_consent_path(args, config)
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

    try: #TODO: may have to update for ncanda container
        data_dict_path = pathlib.Path(
        config.get("data_dict_directory")
        ) # e.g. '/fs/share/datadict/ndar'
    except:
        raise ValueError(f"No data_dict_directory in {args.sibis_general_config}")

    return staging_path, data_path, consent_path, files_to_validate, data_dict_path

def check_study_designation(included_visits, path_to_visits, consent_path, staging_path, args) -> list:
    """
    Not a relevant need in ncanda. There is only one study and all subjects are included.
    """
    return [included_visits]

def get_consent(visit_path: pathlib.Path, consent_path: pathlib.Path):
    subject_id = visit_path.name
    demographics_path = pathlib.Path(str(consent_path).replace("SUBJECT", subject_id))
    try:
        # keep_default_na turns blank fields to empty string
        demographics = pd.read_csv(demographics_path, keep_default_na=False)
        consent = demographics["ndar_consent"].item()
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

def restrict_to_existing_files(args, path_to_visits, visits, files_to_validate):
    existing_visits, existing_files = [], []
    for visit, file_to_validate in zip(visits, files_to_validate):
        visit = visit / pathlib.Path(("followup_" + args.followup_year + "y"))
        path = path_to_visits / visit / file_to_validate
        if path.exists():
            existing_visits.append(visit)
            existing_files.append(file_to_validate)
        else:
            if file_to_validate == "ndar_subject01.csv":
                raise ValueError("ndar_subject01.csv file not found for {visit}")
    return existing_visits, existing_files
