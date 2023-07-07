#!/usr/bin/env python3
import logging


logger = logging.getLogger("ndar_create_csv")

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