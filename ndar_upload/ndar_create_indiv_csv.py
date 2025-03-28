#!/usr/bin/env python3
import argparse
import logging
from dataclasses import dataclass
from datetime import datetime
import enum
import inspect
from math import ceil
from queue import Empty
import re
from subprocess import PIPE, STDOUT, Popen, TimeoutExpired
from typing import Dict, Generator, List, Optional, Sequence
from pathlib import Path

import csv
import shutil
import sys
import os
from arrow import ParserError

import yaml
import pandas as pd
import xml.etree.ElementTree as ET

# Hack so that we don't have to rely on the package compilation
scripts_dir = Path(__file__).parent / '..'
sys.path.append(str(scripts_dir.resolve()))

EmptyString = ""
BVEC_FILE_NAME = "bvec"
BVAL_FILE_NAME = "bval"

logger = logging.getLogger("ndar_create_csv")

def describe_nifti(nifti_gz: Path) -> dict:
    p = Popen(["describe", "-m", nifti_gz.as_posix()], stdout=PIPE, stderr=PIPE, text=True)
    nifti_meta = {}
    try:
        std_out, std_err  = p.communicate(timeout=30)
        if std_err in [None, ""]:

            for line in std_out.splitlines():
                param, val = line.split(maxsplit=1)
                nifti_meta[param] = val
    except TimeoutExpired:
        p.kill()
        std_out, std_err = p.communicate()
        logger.error(f'Problems running `describe`:', std_out, std_err)
    
    return nifti_meta

def find_diffusion_nifti_xml(args, visit_dir: Path):
    if args.source == 'ncanda':
        visit_dir = mappings.set_ncanda_visit_dir(args, 'diffusion')
    diffusion_scans = {}
    diffusion_native = visit_dir / "diffusion" / "native"
    if not diffusion_native.exists() :
        return diffusion_scans
    if args.source == 'hivalc':
        diffusion_dirs =  [x.resolve() for x in diffusion_native.iterdir() if x.is_dir() and x.is_symlink()]
        symlink_dirs = [x for x in diffusion_native.iterdir() if x.is_dir() and x.is_symlink()]
        for symlink, diffusion_dir in zip(symlink_dirs, diffusion_dirs):
            nifti_xml = sorted(diffusion_dir.rglob("*.nii.xml"))
            diffusion_scans[symlink.name] = nifti_xml
    else:
        diffusion_dirs = [x for x in diffusion_native.iterdir() if x.is_dir()]
        for diffusion_dir in diffusion_dirs:
            nifti_xml = sorted(diffusion_dir.rglob("*.nii.xml"))
            diffusion_scans[diffusion_dir.name] = nifti_xml
    return diffusion_scans

def find_first_diffusion_nifti(args, visit_dir: Path) -> Generator[Path, None, None]:
    nifti_files = []
    if args.source == 'ncanda':
        visit_dir = mappings.set_ncanda_visit_dir(args, 'diffusion')
    diffusion_native = visit_dir / "diffusion" / "native"
    if not diffusion_native.exists() :
        return nifti_files
    diffusion_dirs =  [x.resolve() for x in diffusion_native.iterdir() if x.is_dir() and x.is_symlink()]
    if args.source == 'ncanda':
        diffusion_dirs = [x for x in diffusion_native.iterdir() if x.is_dir()]
    for diffusion_dir in diffusion_dirs:
        possible_nifti = sorted(diffusion_dir.rglob("*.nii.gz"))
        if len(possible_nifti) > 0:
            nifti_files.append(possible_nifti[0])
    return nifti_files

def find_first_rsfmri_nifti(args, visit_dir: Path):
    nifti_files = []
    if args.source == 'ncanda':
        visit_dir = mappings.set_ncanda_visit_dir(args, 'restingstate')
    rsfmri_native = visit_dir / "restingstate" / "native" 
    if not rsfmri_native.exists():
        return nifti_files
    rsfmri_dirs = [x for x in rsfmri_native.iterdir() if x.is_dir()]
    for rsfmri_dir in rsfmri_dirs:
        possible_nifti = sorted(rsfmri_dir.rglob("*.nii.gz"))
        if len(possible_nifti) > 0:
            nifti_files.append(possible_nifti[0])
    return nifti_files

def find_first_swan_nifti(args, visit_dir: Path):
    nifti_files = []
    swan_native = visit_dir / "iron" / "native" 
    if not swan_native.exists():
        return nifti_files
    possible_nifti = sorted(swan_native.rglob("*.nii.gz"))
    if len(possible_nifti) > 0:
        nifti_files.append(possible_nifti[0])
    return nifti_files

def find_structural_nifti(args, visit_dir: Path) -> Generator[Path, None, None]:
    if args.source == 'ncanda':
        visit_dir = mappings.set_ncanda_visit_dir(args, 'structural')
    structural_native = visit_dir / "structural" / "native"
    if not structural_native.exists() :
        return [] 

    nifti_files = sorted([x.resolve() for x in structural_native.rglob("*.nii.gz")])
    return nifti_files

def find_nifti_xml(nifti_gz: Path) -> Path:
    nifti_gz = nifti_gz.resolve()
    nifti_xml = nifti_gz.parent / f"{nifti_gz.stem}.xml"
    return nifti_xml

def get_element_for_cmtk_nifti_xml(nifti_xml: Path) -> ET.Element:
    with nifti_xml.open('r') as fh:
        # Okay... if I could find the person who generated these XML files,
        # I really want to take them into a back alley and beat the *&^%$#!
        # out of them. I digress.
        # dcm2image doesn't produce valid XML
        # 1. uses namespaces but no namespaces are declared
        # 2. some of the namespaces use illegal characters
        # 3. has no root node
        # So the next few lines basically is a hack to make it parsable.
        try:
            xml_lines = fh.readlines()
            meta_root = ET.fromstringlist(xml_lines)
        except ET.ParseError as pe:
            fh.seek(0)
            bad_xml_lines = fh.readlines()
            for idx, line in enumerate(bad_xml_lines):
                bad_xml_lines[idx] = line.replace("dicom:GE", "GE")
            bad_xml_lines.insert(1, '''<cmtk xmlns:dicom="https://sibis.sri.com/dicom" xmlns:GE="https://sibis.sri.com/dicom/ge">''')
            bad_xml_lines.append("</cmtk>")
            meta_root = ET.fromstringlist(bad_xml_lines)
    return meta_root

def get_nifti_metadata(args):
    visit_dir = args.scan_dir
    nifti_metadata = {}

    structural_nifti_files = find_structural_nifti(args, visit_dir)
    diffusion_nifti_files = find_first_diffusion_nifti(args, visit_dir)
    rsfmri_nifti_files = find_first_rsfmri_nifti(args, visit_dir)
    swan_nifti_files = find_first_swan_nifti(args, visit_dir)
    nifti_files = structural_nifti_files + diffusion_nifti_files + rsfmri_nifti_files + swan_nifti_files
    for nifti_gz in nifti_files:
        modality = NDARImageType.get_modality(nifti_gz)
        nifti_metadata[modality] = describe_nifti(nifti_gz)
    
    return nifti_metadata

@dataclass
class DiffusionMeta:
    bvalue: pd.DataFrame
    bvector: pd.DataFrame

def as_num(value: str):
    try:
        return int(value)
    except ValueError:
        return float(value)

def get_bvector_bvalue(nii_xml_files: Sequence[Path]):
    b_value_df = []
    b_vector_df = []
    for nii_xml in nii_xml_files:
        nii_root = get_element_for_cmtk_nifti_xml(nii_xml)
        b_value = nii_root.findall('.//mr/dwi/bValue')
        if len(b_value) == 1:
            b_value_df.append(as_num(b_value[0].text))
        b_vector = nii_root.findall('.//mr/dwi/bVector')
        if len(b_vector) == 1:
            b_vector_df.append(map(as_num, b_vector[0].text.split(' ')))

    b_value_df = pd.DataFrame(b_value_df)
    b_vector_df = pd.DataFrame(b_vector_df)
    return DiffusionMeta(b_value_df, b_vector_df)

def get_dicom_diffusion_metadata(args) -> Dict[str, DiffusionMeta]:
    visit_dir = args.scan_dir
    all_nii_xml = find_diffusion_nifti_xml(args, visit_dir)
    diffusion_meta = {}
    for dti_kind, xml_files in all_nii_xml.items():
        diffusion_meta[dti_kind] = get_bvector_bvalue(xml_files)
    return diffusion_meta

def fill_modality_obj(meta_root: ET.Element, modality_obj: dict, section: str):
    for meta_elt in meta_root.findall(f".//{section}/"):
        tag_name = meta_elt.tag[meta_elt.tag.find("}")+1:]
        if tag_name in ["image", "dwi"] and len(list(meta_elt)) > 0:
            child_obj = {}
            for img_meta_elt in meta_elt:
                child_tag_name = img_meta_elt.tag[img_meta_elt.tag.find("}")+1:]
                child_tag_value = img_meta_elt.text
                child_obj.update({child_tag_name: {"value": child_tag_value}})
                
                child_attribs = img_meta_elt.attrib
                if child_attribs != {}:
                    child_obj[child_tag_name].update({"attribs": child_attribs})
                
            if len(child_obj.keys()) > 0:
                if tag_name not in modality_obj[section]:
                    modality_obj[section][tag_name] = []
                modality_obj[section][tag_name].append(child_obj)
        else:
            attribs = meta_elt.attrib
            tag_value = meta_elt.text
            modality_obj[section][tag_name] = {
                "value": tag_value
            }
            if attribs != {}:
                modality_obj[section][tag_name].update({"attribs": attribs})

def get_dicom_structural_metadata(args):
    """
    Gathers all structural nifti's and the first niftis of diffusion
    
    """
    visit_dir = args.scan_dir
    dicom_metadata = {}

    structural_nifti_files = find_structural_nifti(args, visit_dir)
    diffusion_nifti_files = find_first_diffusion_nifti(args, visit_dir)
    rsfmri_nifti_files = find_first_rsfmri_nifti(args, visit_dir)
    swan_nifti_files = find_first_swan_nifti(args, visit_dir)
    nifti_files = structural_nifti_files + diffusion_nifti_files + rsfmri_nifti_files + swan_nifti_files
    for nifti_gz in nifti_files:
        nifti_xml = find_nifti_xml(nifti_gz)
        if nifti_xml.exists():
            modality = NDARImageType.get_modality(nifti_gz)
            dicom_metadata[modality] = { "device": {}, "mr": {}, "stack": {}}
            
            meta_root = get_element_for_cmtk_nifti_xml(nifti_xml)

            fill_modality_obj(meta_root, dicom_metadata[modality], "device")
            fill_modality_obj(meta_root, dicom_metadata[modality], "mr")
            fill_modality_obj(meta_root, dicom_metadata[modality], "stack")
            
    if logger.isEnabledFor(logging.DEBUG): logger.debug(str(dicom_metadata))

    return dicom_metadata

@dataclass
class SubjectData:
    dicom: dict = None
    demographics: dict = None
    nifti: dict = None
    diffusion: Dict[str, DiffusionMeta] = None
    measurements: dict = None

def get_stack_value(stack_key: str):

    def get_stack_value_for_subject(subject: SubjectData, image_type: str):
        stack_data = subject.dicom[image_type]["stack"]
        try:
            stack_obj = stack_data[stack_key]
            stack_value = stack_obj["value"]
            if "attribs" in stack_obj and "units" in stack_obj["attribs"]:
                stack_value = " ".join([stack_value, stack_obj["attribs"]["units"]])
        except KeyError:
            stack_value = EmptyString
        return stack_value
    
    return get_stack_value_for_subject

def get_dicom_value(section: str, key: str):
    def get_dicom_value_for_subject(subject: SubjectData, image_type: str):
        mr_data = subject.dicom[image_type][section]
        try:
            mr_obj = mr_data[key]
            mr_value = mr_obj["value"]
            if "attribs" in mr_obj and "units" in mr_obj["attribs"]:
                mr_value = " ".join([mr_value, mr_obj["attribs"]["units"]])
        except KeyError:
            mr_value = EmptyString
        return mr_value
    
    return get_dicom_value_for_subject

def get_nifti_value(nifti_key: str):

    def get_nifti_value_for_subject(subject: SubjectData, image_type: str):
        try:
            n_data = subject.nifti[image_type][nifti_key]
        except KeyError:
            n_data = EmptyString
        return n_data
    
    return get_nifti_value_for_subject

def get_aquisition_matrix(subject: SubjectData, image_type: str):

    matrix_row_keys = ["I2PMAT0", "I2PMAT1", "I2PMAT2", "I2PMAT3"]
    matrix_rows = []
    for matrix_key in matrix_row_keys:
        matrix_row = filter(lambda idx, val : idx == 3,
                         enumerate(get_nifti_value(matrix_key)(subject, image_type).split(maxsplit=3)))
        matrix_rows.append(matrix_row[1])
    matrix = "/".join(matrix_rows)
    return matrix

def get_field_of_view_pd(subject: SubjectData, image_type: str):
    # import pdb; pdb.set_trace()
    #TODO: add a default null value for this one, baseline test doesn't have the value
    fov_pd = []
    phase_encoding_direction = get_dicom_value('mr', 'phaseEncodeDirection')(subject, image_type)
    col_row_pixel_spacing = get_dicom_value('mr', 'PixelSpacing')(subject, image_type).split('\\')
    
    col_pixel_spacing = float(col_row_pixel_spacing[0])
    col_pixels = int(get_dicom_value('mr', 'Columns')(subject, image_type))
    row_pixel_spacing = float(col_row_pixel_spacing[1])
    row_pixels = int(get_dicom_value('mr', 'Rows')(subject, image_type))
    
    if phase_encoding_direction.upper() == "COL":
        fov_pd = [col_pixels*col_pixel_spacing, row_pixels*row_pixel_spacing]
    else:
        fov_pd = [row_pixels*row_pixel_spacing, col_pixels*col_pixel_spacing]
    
    return f"{fov_pd[0]} x {fov_pd[1]} Millimeters"

    
image_orientation_map = {
    tuple([1, 0, 0, 0, 0, -1]): 'Coronal',
    tuple([0, 1, 0, 0, 0, -1]): 'Sagittal',
    tuple([1, 0, 0, 0, 1,  0]): 'Axial'
}
def get_image_orientation(subject: SubjectData, image_type:str):
    image_orientation_patient = [round(float(x)) for x in get_dicom_value('stack', 'ImageOrientationPatient')(subject, image_type).split('\\')]
    try:
        iop = image_orientation_map[tuple(image_orientation_patient)]
    except KeyError:
        logger.error(f'Unknown image_orientation for {repr(image_orientation_patient)}')
        iop="Unknown"
    return iop

def get_patient_position(subject: SubjectData, image_type: str):
    pp_long = get_stack_value("ImageOrientationPatient")(subject, image_type)

    max_len = 50
    pp_len=len(pp_long)
    if  pp_len > max_len:
        logger.warning("Truncating ImageOrientationPatient so it is less than 51 characters as required by data dictionary!")
        pp_parts = pp_long.split('\\')
        if  len(pp_parts) != 6:
            logger.error("ImageOrientationPatient should define 6 numbers but defined" + str(len(pp_parts)) + "!"  )
            return pp_long  
        
        # first check if int and float of each value is the same
        for idx, part in enumerate(pp_parts):
            if int(float(part)) == float(part):
                pp_parts[idx] = str(int(float(part)))

        new_len = sum(len(part) for part in pp_parts)
        if new_len > max_len:
            # values should be 0 or 1 - all others need to be truncated
            incorrectNum=0
            for idx, part in enumerate(pp_parts):
                if float(part) != 0  and float(part) != 1  and float(part) != -1  : 
                    incorrectNum+=1

            # over the quota has to be fixed by those that are not defined according to format 
            over_by = len(pp_long) - max_len 
            trunc_by = ceil(over_by / incorrectNum)

            # truncate everything that is not 0 or -1
            for idx, part in enumerate(pp_parts):
                if float(part) == 0  or float(part) == 1  or float(part) == -1  :
                    continue
                
                pp_parts[idx] = part[:-trunc_by]
        
        pp_long = '\\'.join(pp_parts)

    return pp_long

@dataclass(frozen=True)
class NDARImageType:
    t1 = "t1"
    t2 = "t2"
    dti30b400 = "dti30b400"
    dti60b1000 = "dti60b1000"
    dti6b500pepolar = "dti6b500pepolar"
    rs_fMRI = "rs-fMRI"
    swan = "swan"

    @classmethod
    def get_modality(self, nii_path:Path) -> str:
        members = inspect.getmembers(self)
        for m, v in members:
            if not m.startswith('_'):
                m = m.replace('_', '-')
                if nii_path.as_posix().find(m) > 0:
                    return m
        return None

@dataclass(frozen=True)
class NDARNonImageType:
    asr01 = "asr01"
    grooved_peg02 = "grooved_peg02"
    tipi01 = "tipi01"
    uclals01 = "uclals01"
    wrat401 = "wrat401"
    upps01 = "upps01"
    fgatb01 = "fgatb01"
    macses01 = "macses01"
    sre01 = "sre01"

@dataclass
class TargetCSVMeta():
    output_file: Path
    data_dictionary: Path
    image_type: Optional[str] = None

    def __str__(self) -> str:
        return f"output_file: {self.output_file}, data_dictionary: {self.data_dictionary}, image_type: {self.image_type}"

class NDARFileVariant(): 
    headers = {
        'image': ['image', '03'],
        'subject': ['ndar_subject', '01'],
        'asr01': ['asr', '01'],
        'grooved_peg02': ['grooved_peg', '02'],
        'tipi01': ['tipi', '01'],
        'uclals01': ['uclals', '01'],
        'wrat401': ['wrat4', '01'],
        'upps01': ['upps', '01'],
        'fgatb01': ['fgatb', '01'],
        'macses01': ['macses', '01'],
        'sre01': ['sre', '01'],
    }

    @classmethod
    def is_image_type(clazz, file_type: str) -> bool:
        if file_type in [NDARImageType.t1, NDARImageType.t2, NDARImageType.dti30b400, NDARImageType.dti60b1000, NDARImageType.dti6b500pepolar, NDARImageType.rs_fMRI, NDARImageType.swan]:
            return True
        return False

    @classmethod
    def is_measurements_type(clazz, file_type: str) -> bool:
        if file_type in mappings.measurements_file_list:
            return True
        return False

    @classmethod
    def get_version_header(claszz, file_type: str):
        if NDARFileVariant.is_image_type(file_type):
            return NDARFileVariant.headers['image']
        elif NDARFileVariant.is_measurements_type(file_type):
            return NDARFileVariant.headers[file_type]
        else:
            return NDARFileVariant.headers['subject']

def get_demo_dict(args) -> dict:
    """
    Convert the demographics.csv file into a dictionary where key=col name, value=value of col
    """
    demo_csv_file = args.visit_demographics
    if args.source == 'ncanda':
        demo_csv_file = mappings.set_ncanda_visit_dir(args, 'redcap') / 'measures' / 'demographics.csv'
    with demo_csv_file.open() as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        cols_dict = {}
        demo_dict = {}
        for row in csv_reader:
            col_no = 0
            if line_count == 0:
                for col in row:
                    cols_dict[col_no] = col
                    col_no = col_no+1    
            else:
                for val in row:
                    demo_dict[cols_dict[col_no]] = val
                    col_no = col_no+1
            line_count += 1

        return demo_dict

def get_race(race: int) -> str:
    # RACE_MAP = sys_values['maps']['race_map']
    RACE_MAP = mappings.race_map
    try:
        race = RACE_MAP[race]
    except KeyError:
        race = "Unknown or not reported"
    return race

@dataclass(frozen=True)
class DefinitionHeader:
    ElementName = "ElementName"
    DataType = "DataType"
    Size = "Size"
    Required = "Required"
    Condition = "Condition"
    ElementDescription = "ElementDescription"
    ValueRange = "ValueRange"
    Notes = "Notes"
    Aliases = "Aliases"

def get_subjectkey(subject: SubjectData, *_) -> str:
    """Get ndar guid of the subject"""
    if "demo_ndar_guid" in subject.demographics and len(subject.demographics["demo_ndar_guid"].strip()):
        field_value =  subject.demographics["demo_ndar_guid"]
    elif "ndar_guid_id" in subject.demographics and len(subject.demographics["ndar_guid_id"].strip()):
        field_value = subject.demographics["ndar_guid_id"]
    else:
        field_value =  "NDARXXXXXXXX"
    return field_value

def get_image_description(subject: SubjectData, image_type: str) -> str:
    if image_type == NDARImageType.t1:
        field_value =  "SPGR"
    elif image_type == NDARImageType.rs_fMRI:
        field_value = "fMRI"
    elif image_type in [NDARImageType.dti30b400, NDARImageType.dti60b1000, NDARImageType.dti6b500pepolar]:
        field_value =  "DTI"
    elif image_type == NDARImageType.swan:
        field_value = "SWAN-QSM"
    else:
        field_value =  "FSE"
    return field_value

SCAN_TYPE_MAP = {
    NDARImageType.t1: "MR structural (T1)",
    NDARImageType.t2: "MR structural (T2)",
    NDARImageType.dti30b400: "single-shell DTI",
    NDARImageType.dti60b1000: "single-shell DTI",
    NDARImageType.dti6b500pepolar: "single-shell DTI",
    NDARImageType.rs_fMRI: "fMRI",
    NDARImageType.swan: "T2* Weighted Angiography (GE SWAN)",
}
def get_scan_type(subject: SubjectData, image_type: str) -> str:
    try:
        scan_type = SCAN_TYPE_MAP[image_type]
    except KeyError:
        scan_type = EmptyString
    return scan_type

def conform_field_specs_datatype(field_value, field_spec:dict, subject:SubjectData):
    field = field_spec[DefinitionHeader.ElementName]
    datatype = field_spec[DefinitionHeader.DataType]
    if datatype == "String":
        if not isinstance(field_value, str):
            logger.warning(f"{field} [{field_value}] is not of type {datatype}")
            field_value = str(field_value)
    
    if datatype == "GUID":
        if not(isinstance(field_value, str) and re.match("^NDAR.*$", str(field_value))):
            logger.warning(f"{field} [{field_value}] is not of type {datatype} ")
            field_value = str(field_value)

    if datatype == "Float":
        try:
            if field_value == "":
                field_value = ""
            elif str(field):
                field_value = float(re.sub(r'[^0-9\.]+', '', field_value))
            
        except:
            logger.error(f"cannot convert {field} [{field_value}] to float")

    if datatype == "Integer":
        try:
            if field_value == "":
                field_value = ""
            else:
                field_value = int(round(float(field_value)))
        except:
            logger.error(f"cannot convert {field} [{field_value}] to int")
    
    if datatype == "Date":
        try:
            pd.to_datetime(str(field_value), errors='raise')
        except ParserError or ValueError:
            logger.error(f"cannot parse {field} [{field_value}]as a date")

    return field_value

def check_field_specs(field_value, field_spec:dict, subject: SubjectData):

    field =  field_spec[DefinitionHeader.ElementName]
    if field_spec[DefinitionHeader.Required] in ['Required'] and field_value in [EmptyString, None]:
            logger.warning(f"{field_spec[DefinitionHeader.Required]} Field, {field}, is missing a value!")

    if field_spec[DefinitionHeader.Size] not in [None, EmptyString]:
        if len(str(field_value)) > int(field_spec[DefinitionHeader.Size]):
            logger.warning(f"Field, {field} is {len(field_value)} and exceeds the maximum size {field_spec[DefinitionHeader.Size]}!")

DIFFUSION_MODALITIES = [NDARImageType.dti30b400, NDARImageType.dti60b1000, NDARImageType.dti6b500pepolar]
def has_bvek_bval_files(subject: SubjectData, image_type: str):
    if (image_type in DIFFUSION_MODALITIES and image_type in subject.diffusion.keys()):
        return "Yes"
    else:
        return ""

unit_map = {
    "in": "Inches",
    "cm": "Centimeters",
    "ang": "Angstroms", 
    "nm": "Nanometers", 
    "um": "Micrometers",
    "mm": "Millimeters",
    "m": "Meters",
    "km": "Kilometers", 
    "mi": "Miles",
    "ns": "Nanoseconds",
    "us": "Microseconds",
    "ms": "Milliseconds",
    "s": "Seconds",
    "min": "Minutes",
    "hr": "Hours",
    "hz": "Hertz",
    "fn": "frame number"
}
def get_image_units(subject: SubjectData, image_type: str):
    try:
        units = unit_map[get_nifti_value("UNITS")(subject, image_type)]
    except KeyError:
        units = unit_map["mm"]
    return units

def handle_image_field(image_type: str, field_spec: dict, subject: SubjectData):
    IMAGE_MAP = mappings.image_map
    
    field =  field_spec[DefinitionHeader.ElementName]
    field_value = EmptyString
    try:
        field_value = IMAGE_MAP[field]
        if isinstance(field_value, str):
            if field_value.startswith('lambda'):
                func = eval(field_value)
                field_value = func(subject, image_type)
            elif field_value.startswith(('get', 'has', 'mappings')):
                func = field_value + '(subject, "' + image_type + '")'
                field_value = eval(func)

        field_value = conform_field_specs_datatype(field_value, field_spec, subject)
    except KeyError:
        field_value = EmptyString
    except Exception as e:
        logger.warning("Exception for %s: %s\n" % (field, e))
        field_value =  EmptyString

    check_field_specs(field_value, field_spec, subject)
    
    return field_value

def get_add_measurements_metadata(args):
    """Get the additional subject data that comes from summaries file (mncanda, asr)"""
    #NOTE: for now only going to store asr data
    summaries_path = mappings.set_ncanda_visit_dir(args, 'additional')
    summaries_files = list(summaries_path.glob('*.csv'))
    follow_yr = "followup_" + args.followup_year + "y"

    summaries_dfs = {}

    # extract individual subject data
    for f in summaries_files:
        if f.name == "asr.csv" and f.exists():
            try:
                df = pd.read_csv(f)
                subject_df = df.loc[ (df['subject'] == args.subject) & (df['visit'] == follow_yr)]
                summaries_dfs[f.name.split('.')[0]] = subject_df.reset_index(inplace=True, drop=True)
            except Exception as e:
                logger.warning(f"Error getting additional summary values from file: {f}")
            
    return summaries_dfs

def get_measurements_metadata(args):
    
    """Store all of the csv files under the redcap release as a dictionary of dataframes"""
    meta = {}

    if args.source == 'hivalc':
        return meta

    # get the path to all of the redcap csv files
    redcap_path = mappings.set_ncanda_visit_dir(args, 'redcap')

    # for every file in the redcap directory store its content as a title:dataframe pair
    files = list(redcap_path.glob('measures/*'))
    for f in files:
        key = f.name.split('.', 1)[0]
        val = pd.read_csv(f)
        meta[key] = val

    # add the specific subject values from additional summary files (mncanda, asr)
    add_meta = get_add_measurements_metadata(args)
    
    # add additional meta data
    meta.update(add_meta)

    return meta

def get_empty_string():
    """
    Returns empty string. Allows for forcing immediate population of empty string
    in a field via the ncanda mappings
    """
    return EmptyString

def recode_missing(field_spec):
    """Some variable types have specific codes for missing values, replace as needed"""
    miss_list = ['lr_rawscore', 'wr_rawscore', 'wr_totalrawscore', 'wr_standardscore']
    # format for asr missingness value == '88=Missing'
    if field_spec['ElementName'].startswith('asr'):
        miss_value_idx = field_spec['Notes'].find('Missing') - 3
        miss_value = field_spec['Notes'][miss_value_idx:(miss_value_idx+2)]
        return miss_value
    elif field_spec['ElementName'] in miss_list:
        miss_value = int(field_spec['ValueRange'].split(';')[-1])
        return miss_value

    return EmptyString

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

def reverse_val(value, field_spec, map_row):
    """Reverse value to match ndar value range meaning (assuming range is same, just reversed meaning)"""
    try:
        # convert ranges to possible value arrays
        ndar_range_str = field_spec['ValueRange'].split(';')[0] # ex. '1::7'
        ndar_range_ends = list(map(int, re.findall(r'\d+', ndar_range_str)))
        ndar_range = list(range(ndar_range_ends[0], ndar_range_ends[1]+1))
        ndar_range.reverse()

        ncanda_range_str = map_row['ncanda_value_range'].values[0]
        ncanda_range = list(map(int, re.findall(r'\d+', ncanda_range_str)))
        
        # get the inverse position of the value
        ncanda_idx = ncanda_range.index(value)
        new_value = ndar_range[ncanda_idx]
        return new_value
    except KeyError:
        logger.warning(f"Failed to reverse value of {field_spec['ElementName']}")
        return value

def get_measurements_source_val(ndar_csv_meta, field_spec, subject: SubjectData):
    mapping_file = Path(str(ndar_csv_meta.data_dictionary).replace("definitions", "mappings"))
    if not mapping_file.exists():
        logger.error(f'Cannot find the mapping file for {ndar_csv_meta.image_type}. Tried {mapping_file}')

    mapping_df = pd.read_csv(mapping_file)

    # get row from map for given ndar variable
    ndar_element_name = field_spec.get('ElementName')
    map_row = mapping_df.loc[mapping_df['NDA_ElementName'] == ndar_element_name]

    try:
        csv_source_name = map_row.get('ncanda_csv').iloc[0]
        csv_source_df = subject.measurements.get(csv_source_name)
        ncanda_element_name = map_row.get('ncanda_variable').iloc[0]

        # if mapping exists, then pull the value
        if not pd.isna([ncanda_element_name, csv_source_df, csv_source_name]).any():
            value = csv_source_df[ncanda_element_name].iloc[0]
            # check if there is a specific value to indicate missingness
            if pd.isna(value):
                value = recode_missing(field_spec)
            # check if there needs to be a reversal of value based on ndar definitions
            elif value != '' and test_reverse(map_row):
                value = reverse_val(value, field_spec, map_row)
            return str(value)
    except IndexError:
        # if trying to get elements from results give index error, then mapping doesn't exist.
        raise KeyError

    # if map doesn't exist but value is required, fill in w/ missing value
    if field_spec['Required'] == 'Required':
        value = recode_missing(field_spec)
        return str(value)

    raise KeyError

def get_measurement_timept(subject):
    """Return visit number/name for measurements required fields"""
    try:
        timept = int(re.search(r"\d+", subject.demographics["visit"]).group())
    except AttributeError:
        # visit is baseline, return 0 to indicate baseline visit
        timept = 0
    return timept

def handle_measurements_field(ndar_csv_meta, field_spec, subject: SubjectData):
    """Using the mappings file convert our measurements data to ndar expected file format"""
    #TODO: ndar_create_csv [WARNING] Exception for timept: 'NoneType' object has no attribute 'group'
    field = field_spec[DefinitionHeader.ElementName]
    try:
        field_value = mappings.MEASUREMENTS_MAP[field]

        if field_value.startswith('lambda'):
            func = eval(field_value)
            field_value = func(subject)
        elif field_value.startswith('get'):
            field_value = eval(field_value)

        field_value = conform_field_specs_datatype(field_value, field_spec, subject)
    except KeyError:
        # variable is not in map dictionary, see if it has corresponding ndar csv value
        try:
            field_value = get_measurements_source_val(ndar_csv_meta, field_spec, subject)
            field_value = conform_field_specs_datatype(field_value, field_spec, subject)

        except KeyError:
            field_value = EmptyString
    except Exception as e:
        logger.warning("Exception for %s: %s\n" % (field, e))
        field_value = EmptyString
    
    check_field_specs(field_value, field_spec, subject)
    
    return field_value

def handle_field(field_spec: dict, subject: SubjectData):
    field = field_spec[DefinitionHeader.ElementName]
    SUBJECT_MAP = mappings.subject_map
    try:
        field_value = SUBJECT_MAP[field]

        if field_value.startswith('lambda'):
            func = eval(field_value)
            field_value = func(subject)
        elif field_value.startswith('get'):
            field_value = eval(field_value)

        field_value = conform_field_specs_datatype(field_value, field_spec, subject)
    except KeyError:
        field_value = EmptyString
    except Exception as e:
        logger.warning("Exception for %s: %s\n" % (field, e))
        field_value = EmptyString
    
    check_field_specs(field_value, field_spec, subject)
    
    return field_value

def write_ndar_csv(subject_data: SubjectData, ndar_csv_meta: TargetCSVMeta):
    """
    Create ndar files with the given metadata
    """
    if ndar_csv_meta.image_type:
        if ndar_csv_meta.image_type not in subject_data.nifti.keys() and not NDARFileVariant.is_measurements_type(ndar_csv_meta.image_type):
            logger.info(f"Skipping {ndar_csv_meta.image_type}")
            return 
    
    if not ndar_csv_meta.output_file.parent.exists():
        ndar_csv_meta.output_file.parent.mkdir(parents=True, exist_ok=True)
        
    with ndar_csv_meta.output_file.open('w') as ndar_file: 
        val_row = []
        header_row = []
        csvwriter = csv.writer(ndar_file, quoting=csv.QUOTE_NONNUMERIC) 
        with open(ndar_csv_meta.data_dictionary) as csv_file:
            # csv_reader = csv.reader(csv_file, delimiter=',')
            csv_reader = csv.DictReader(csv_file, delimiter=',')
            for field_spec in csv_reader:
                if NDARFileVariant.is_image_type(ndar_csv_meta.image_type):
                    val = handle_image_field(ndar_csv_meta.image_type, field_spec, subject_data)
                elif NDARFileVariant.is_measurements_type(ndar_csv_meta.image_type):
                    val = handle_measurements_field(ndar_csv_meta, field_spec, subject_data)
                else:
                    val = handle_field(field_spec, subject_data)
                header_row.append(field_spec[DefinitionHeader.ElementName])
                val_row.append(val)
        csvwriter.writerow(NDARFileVariant.get_version_header(ndar_csv_meta.image_type))
        csvwriter.writerow(header_row)
        csvwriter.writerow(val_row)

def is_dir(arg_name: str = "Path", mode: int = os.R_OK | os.W_OK | os.X_OK , create_if_missing: bool =  False):

    def is_dir_path(dir_path: str) -> Path:
        maybe_path = Path(dir_path)
        if (maybe_path.exists() and maybe_path.is_dir()):
            if (os.access(maybe_path.as_posix(), mode)):
                return maybe_path
            return argparse.ArgumentTypeError(f"{arg_name}: {maybe_path} has incorrect access permissions")
        elif (create_if_missing and not maybe_path.exists()):
            try:
                maybe_path.mkdir(mode=0o775, parents=True, exist_ok=True)
                return maybe_path
            except Exception as e:
                return argparse.ArgumentTypeError(f"{arg_name}: {maybe_path} had a problem creating directory")

        return argparse.ArgumentTypeError(f"{arg_name}: {maybe_path} is not a valid path")
    
    return is_dir_path

def is_file(arg_name: str = "Path", mode: int = os.R_OK ):

    def is_file_path(file_path: str) -> Path:
        maybe_path = Path(file_path).expanduser()
        if (maybe_path.exists() and maybe_path.is_file()):
            return maybe_path

        return argparse.ArgumentTypeError(f"{arg_name}: {maybe_path} is not a valid path")
    
    return is_file_path

@dataclass
class ConfigError(Exception):
    msg: str
    
def _parse_args(input_args: List[str] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser()

    # Standard config args independent of source
    config_args = p.add_argument_group('Config', 'Config args regardless of source (enter first before source)')
    config_args.add_argument(
        '--config', help="SIBIS General Configuration file",
        type=is_file("config", os.X_OK), default="/fs/storage/share/operations/secrets/.sibis/.sibis-general-config.yml"
    )
    config_args.add_argument(
        '--sys_config', help="SIBIS System Configuration file",
        type=is_file("config", os.X_OK)
    )
    config_args.add_argument(
        '--ndar_dir', help="Base output directory for NDAR directory and CSV files to be written",
        type=is_dir("ndar_dir", os.X_OK | os.R_OK | os.W_OK, create_if_missing=True)
    )
    config_args.add_argument(
        '--verbose', '-v', action='count', default=0
    )

    # HIVALC Specific args
    subparsers = p.add_subparsers(title='Data Souce', dest='source', help='Define the data source for csv creation')
    hivalc_parser = subparsers.add_parser('hivalc', help='Hivalc CSV Creation')
    hivalc_parser.add_argument(
        '--subject', help="The Subject ID, typ: LAB_SXXXXX",
        required=True, type=str
    )
    hivalc_parser.add_argument(
       '--visit', help="The Visit ID, usually <visit>_<scan_id>",
        type=str, required=True,
    )
    hivalc_parser.add_argument(
        '--arm', help='Arm location of scan',
        type=str, required=True,
    )

    # NCANDA Specific args
    ncanda_parser = subparsers.add_parser('ncanda', help='Ncanda CSV Creation')
    ncanda_parser.add_argument(
        '--subject', help="The Subject ID, typ: NCANDA_SXXXXX",
        required=True, type=str
    )
    ncanda_parser.add_argument(
        "--release_year",
        help="Release year (digit only, ex: 8) of the data (parent dir of desired NCANDA_SNAPS* dir)",
        type=str, required=True,
    )
    ncanda_parser.add_argument(
        "--followup_year",
        help="Followup year (digit only, ex: 8) of the data in release (parent dir of desired measures/imaging dirs in cases), default is release year",
        type=str, required=False,
    )

    ns =  p.parse_args(input_args)

    if ns.verbose > 0:
        change_log_level = ns.verbose * 10
        current_root_level = logging.getLogger().getEffectiveLevel()
        new_level = current_root_level - change_log_level
        if new_level < 0:
            new_level = 0
        logging.getLogger().setLevel(new_level)

    # set followup year default if not specified
    if ns.source == 'ncanda' and ns.followup_year is None:
        ns.followup_year = ns.release_year

    with ns.config.open("r") as fh:
        cfg = yaml.safe_load(fh)
        try:
            gen_cfg = cfg['ndar']['create_csv'][ns.source]

            if ns.source == 'hivalc':
                fmt_env = {
                    "subject": ns.subject,
                    "arm": ns.arm,
                    "visit": ns.visit
                }
                fmt_env.update(gen_cfg)
                
                ns.scan_dir = Path(gen_cfg['visit_dir'].format(**fmt_env))
                if not ns.scan_dir.exists():
                    raise ConfigError(f"The `visit_dir`, {ns.scan_dir}, does not exist.")

                ns.visit_demographics = ns.scan_dir / gen_cfg['visit_demographics']
                if not ns.visit_demographics.exists():
                    raise ConfigError(f"The `visit_demographics`, {gen_cfg['visit_demographics']}, does not exist in {ns.scan_dir}")
                # set measurements data dict path to None
                ns.measurements_definitions = None
            else:
                # ncanda scan dir and demographics endpoint
                cfg_paths = cfg['ndar']['create_csv'][ns.source]
                ns.scan_dir = Path(cfg_paths['visit_dir'])
                ns.visit_demographics = ns.scan_dir

                ns.measurements_definitions = Path(gen_cfg['definition_dir']) / 'measurements'
                if not ns.measurements_definitions.exists():
                    raise ConfigError(f"The measurements definitions dir is missing from {ns.datadict_dir}")

            if  ns.ndar_dir is None:
                ns.ndar_dir = Path(gen_cfg['output_dir'])

            if not ns.ndar_dir.exists():
                ns.ndar_dir.mkdir(0o775, parents=True, exist_ok=True)

            ns.datadict_dir = Path(gen_cfg['definition_dir'])
            if not ns.datadict_dir.exists():
                raise ConfigError(f'The `definition_dir`, {ns.datadict_dir} does not exist.')

            ns.subject_definition = ns.datadict_dir / gen_cfg['subject_definition']
            if not ns.subject_definition.exists():
                raise ConfigError(f"The `subject_definition`, {gen_cfg['subject_definition']} is missing from {ns.datadict_dir}")

            ns.image_definition = ns.datadict_dir / gen_cfg['image_definition']
            if not ns.image_definition.exists():
                raise ConfigError(f"The `image_definition`, {gen_cfg['image_definition']} is missing from {ns.datadict_dir}")

            ns.mappings_dir = gen_cfg['mappings_dir']
        except KeyError:
            p.exit(10, f"Could not find `ndar.create_csv` in {p.config.as_posix()}")
        except ConfigError as ce:
            p.exit(1, f"Configuration Error: {ce.msg}")

    return ns

def write_bvec_bval_files(subject: SubjectData, ndar_meta: TargetCSVMeta):
    b_path = ndar_meta.output_file.parent
    if ndar_meta.image_type in DIFFUSION_MODALITIES and  ndar_meta.image_type in subject.diffusion:
        diffusion_meta = subject.diffusion[ndar_meta.image_type]
        if len(diffusion_meta.bvalue) > 0:
            diffusion_meta.bvalue.T.to_csv(b_path/BVAL_FILE_NAME, sep=' ', quoting=None, index=False, header=False)
        if len(diffusion_meta.bvector) > 0:
            diffusion_meta.bvector.T.to_csv(b_path/BVEC_FILE_NAME, 
                                            sep=' ', quoting=None, float_format='%.5f',
                                            index=False, header=False)

def set_output_dir(args):
    """Set the path for the output directory aka ndar_dir"""
    if args.source == 'hivalc':
        ndar_dir: Path = args.ndar_dir / f"{args.subject}_{args.visit}" # /tmp/ndarupload/LAB_S01669_20220517_6909_05172022
    else:
        # /tmp/ncanda-ndarupload/NCANDA_SXXXXX/followup_yr
        if args.followup_year == '0':
            ndar_dir: Path = args.ndar_dir / args.subject / "baseline"
        else:
            ndar_dir: Path = args.ndar_dir / args.subject / f"followup_{args.followup_year}y"

    return ndar_dir

def set_dir_paths(args):
    """
    Set path to scan dir (input) and ndar dir (output)
    Scan dir ex: /fs/neurosci01/ncanda/releases/internal
    Ndar dir ex: /tmp/sibispy_ndar/NCANDA_S00735/baseline
    """
    scan_dir: Path = args.scan_dir
    ndar_dir = set_output_dir(args)
    return scan_dir, ndar_dir

def main(input_args: List[str] = None):
    logging.basicConfig(level=logging.WARNING,
                        format=r"%(asctime)s > %(name)s [%(levelname)s] %(message)s",
                        datefmt=r"%Y%m%dT%H:%M:%S")
    log_config = Path("logging.conf")
    if log_config.exists():
        logging.config.fileConfig(log_config.absolute().as_posix())

    args = _parse_args(input_args)
    
    # add mappings files to the system path so it can be imported
    sys.path.append(args.mappings_dir)

    # set input and output base paths
    scan_dir, ndar_dir = set_dir_paths(args)
    
    # import respective mappings file as a global import
    if args.source == 'hivalc':
        globals()['mappings'] = __import__('hivalc_mappings')
    else:
        globals()['mappings'] = __import__('ncanda_mappings')

    # Create output directory if it doesn't exist.
    if not ndar_dir.exists():
        ndar_dir.mkdir(mode=0o775, parents=True, exist_ok=True)
    
    # Read in all relevant NDAR file definitions
    subject_definitions_csv = args.subject_definition
    image_definitions_csv = args.image_definition
    measurements_definitions = args.measurements_definitions

    # Create full list of files to generate of TargetCSVMeta class
    files_to_generate = mappings.files_to_generate
    ndar_csv_meta_files = []

    for file_name in files_to_generate:
        file_type = file_name.split('/', 1)[0]
        if file_type == 'ndar_subject01':
            definition = subject_definitions_csv
            meta = TargetCSVMeta(ndar_dir / "ndar_subject01.csv", definition)
        elif NDARFileVariant.is_image_type(file_type):
            definition = image_definitions_csv
            meta = TargetCSVMeta(ndar_dir / file_type / "image03.csv", definition, file_type)
        else:
            measurements_file_type = file_name.split('/', 1)[1]
            measurements_type = measurements_file_type.split('.')[0]
            definition = measurements_definitions / str(measurements_type + "_definitions.csv")
            meta = TargetCSVMeta(ndar_dir / file_type / measurements_file_type, definition, measurements_type)

        ndar_csv_meta_files.append(meta)

    # Get the imaging and measurements data from source
    dicom_metadata = get_dicom_structural_metadata(args)
    nifti_metadata = get_nifti_metadata(args)
    dti_metadata = get_dicom_diffusion_metadata(args)
    measurements_metadata = get_measurements_metadata(args)

    # Get user demographic data which is needed for all ndar csv files
    demo_dict = get_demo_dict(args)
    logger.debug('demo_dict: %s\n' % (demo_dict))

    subj_data = SubjectData(dicom_metadata, demo_dict, nifti_metadata, dti_metadata, measurements_metadata)

    # Create each CSV file
    for some_ndar_meta in ndar_csv_meta_files:
        logger.info(f'Starting  {scan_dir} ({some_ndar_meta})')
        write_ndar_csv(subj_data, some_ndar_meta)
        write_bvec_bval_files(subj_data, some_ndar_meta)


if __name__ == '__main__':
    main()

