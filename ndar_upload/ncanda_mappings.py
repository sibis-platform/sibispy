#!/usr/bin/env python3
import logging
import pandas as pd
from datetime import datetime
import re

logger = logging.getLogger("ndar_create_csv")

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
        #FIXME: test if this error works
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