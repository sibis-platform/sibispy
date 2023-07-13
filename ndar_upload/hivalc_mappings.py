#!/usr/bin/env python3
import re
import logging
import pathlib
from dataclasses import dataclass
import pandas as pd
import shutil


logger = logging.getLogger("ndar_create_csv")

#
# Ndar individual csv creation mappings
#
race_map = {
    1: "American Indian/Alaska Native",
    2: "Asian",
    3: "Black or African American",
    4: "Hawaiian or Pacific Islander",
    5: "White",
    7: "More than one race"
}
        
phenotype_map = {
    "C": "Control",
    "H": "HIV-positive",
    "E": "Alcoholic",
    "HE": "HIV-positive and alcoholic",
    "MCI": "Mild Cognitive Impairment (MCI)",
    "MCI_Alcoholic": "Mild Cognitive Impairment (MCI) and Alcoholic",
    "AD": "Alzheimer's Disease",
    "Adol": "Adolescent",
    "Prkd": "Parkinson's Disease",
    "H_Prkd": "HIV-positive with Parkinson's",
    "PMDD": "Premenstrual Dysphoric Disorder (PMDD)",
    "KS": "Korsakoff's",
    "SZ": "Schizophrenia",
    "Smok": "Smoker (Colrain study)",
    "Twin": "Twin",
    "Phantom": "Phantom scan development control",
    "Twin_unknown": "Twin (maybe)",
}

subject_map = {
    "subjectkey": "get_subjectkey(subject)",
    "src_subject_id": 'lambda subject, *_: subject.demographics["subject"]',
    "interview_date": 'lambda subject, *_: hivalc_funcs.convert_interview_date(str(subject.demographics["visit"]))',
    "interview_age":  'lambda subject, *_: hivalc_funcs.get_interview_age(str(subject.demographics["visit"]), str(subject.demographics["demo_dob"]))',
    "sex": 'lambda subject, *_: subject.demographics["sex"]',
    "race": 'get_race(sys_values, int(subject.demographics["demo_race"]))',
    "phenotype": 'lambda subject, *_: hivalc_funcs.get_phenotype(subject.demographics["demo_diag"], subject.demographics["demo_diag_new"], subject.demographics["demo_diag_new_dx"])',
    "phenotype_description": 'hivalc_funcs.get_phenotype_description(sys_values, subject.demographics["demo_diag"], subject.demographics["demo_diag_new"], subject.demographics["demo_diag_new_dx"])',
    "twins_study": "No",
    "sibling_study": "No",
    "family_study": "No",
    "sample_taken": "No",
}

image_map = {
    "subjectkey": 'get_subjectkey',
    "src_subject_id": 'lambda subject, *_: subject.demographics["subject"]',
    "interview_date": 'lambda subject, *_: hivalc_funcs.convert_interview_date(str(subject.demographics["visit"]))',
    "interview_age":  'lambda subject, *_: hivalc_funcs.get_interview_age(str(subject.demographics["visit"]), str(subject.demographics["demo_dob"]))',
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
def get_phenotype(diag, diag_new, diag_new_dx):
    if diag_new != "" and int(diag_new) == 1:
        return str(diag_new_dx)
    else:
        return str(diag)

def get_phenotype_description(sys_values, diag, diag_new, diag_new_dx):
    phenotype = get_phenotype(diag, diag_new, diag_new_dx)
    PHENOTYPE_MAP = sys_values.phenotype_map
    try:
        phenotype_desc = PHENOTYPE_MAP[phenotype]
    except KeyError:
        logger.error(f'No mapping for {phenotype}')
        phenotype_desc = ""
    return phenotype_desc

def convert_interview_date(date):
    mmddyyyy = date.split('_')[2]
    month = mmddyyyy[:2]
    year = mmddyyyy[4:]
    return month+"/01/"+year

def get_interview_age(visit_date, dob):
    interview_date = convert_interview_date(visit_date)
    interview_month = int(interview_date.split('/')[0])
    interview_year = int(interview_date.split('/')[2])

    dob_month = int(dob.split('-')[1])
    dob_year = int(dob.split('-')[0])

    return str(12*(interview_year-dob_year) + (interview_month - dob_month))

#
# Ndar summary csv creation functions
#
def set_visit_path(args, staging_path, data_path):
    """
    Set the path to the visit directories based upon whether we are:
    1. checking new - this should pull data from the data_path
    2. rechecking consents = this should pull data from the waiting for consent path
    3. rechecking study = this should pull data from the waiting for study path
    4. checking validation - this should pull data only from validation_errors path
    """
    if args.project == 'ncanda':
        if args.check_new:
            path_to_visits = ad

    else:
        if args.check_new:
            path_to_visits = data_path
        elif args.recheck_consent:
            path_to_visits = staging_path / StagingPaths.waiting_for_consent
        elif args.recheck_study:
            path_to_visits = staging_path / StagingPaths.waiting_for_study
        else:  # if args.check_validation
            path_to_visits = staging_path / StagingPaths.validation_errors
        
    return path_to_visits

def get_visit_path(args, path_to_visits, visit):
    visit_path = (path_to_visits / visit)
    return visit_path

@dataclass(frozen=True)
class StagingPaths:
    staging: pathlib.Path = pathlib.Path("staging")
    waiting_for_consent: pathlib.Path = staging / "waiting_for_consent"
    waiting_for_study: pathlib.Path = staging / "waiting_for_study"
    validation_errors: pathlib.Path = staging / "validation_errors"
    exempt_from_release: pathlib.Path = staging / "exempt_from_release"

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

    try:    #TODO: update for ncanda, because it has to be more complex
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

