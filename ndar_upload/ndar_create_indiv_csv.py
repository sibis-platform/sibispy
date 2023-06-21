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
from sibispy import config_file_parser as cfg_parser

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

def find_diffusion_nifti_xml(visit_dir: Path):
    diffusion_scans = {}
    diffusion_native = visit_dir / "diffusion" / "native"
    if not diffusion_native.exists() :
        return diffusion_scans
    
    diffusion_dirs =  [x.resolve() for x in diffusion_native.iterdir() if x.is_dir() and x.is_symlink()]
    symlink_dirs = [x for x in diffusion_native.iterdir() if x.is_dir() and x.is_symlink()]
    for symlink, diffusion_dir in zip(symlink_dirs, diffusion_dirs):
        nifti_xml = sorted(diffusion_dir.rglob("*.nii.xml"))
        diffusion_scans[symlink.name] = nifti_xml
    return diffusion_scans

def find_first_diffusion_nifti(visit_dir: Path) -> Generator[Path, None, None]:
    nifti_files = []
    diffusion_native = visit_dir / "diffusion" / "native"
    if not diffusion_native.exists() :
        return nifti_files
    diffusion_dirs =  [x.resolve() for x in diffusion_native.iterdir() if x.is_dir() and x.is_symlink()]
    for diffusion_dir in diffusion_dirs:
        possible_nifti = sorted(diffusion_dir.rglob("*.nii.gz"))
        if len(possible_nifti) > 0:
            nifti_files.append(possible_nifti[0])
    return nifti_files

def find_structural_nifti(visit_dir: Path) -> Generator[Path, None, None]:
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

def get_nifti_metadata(visit_dir: Path):
    nifti_metadata = {}

    structural_nifti_files = find_structural_nifti(visit_dir)
    diffusion_nifti_files = find_first_diffusion_nifti(visit_dir)
    nifti_files = structural_nifti_files + diffusion_nifti_files
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

def get_dicom_diffusion_metadata(visit_dir: Path) -> Dict[str, DiffusionMeta]:
    all_nii_xml = find_diffusion_nifti_xml(visit_dir)
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

def get_dicom_structural_metadata(visit_dir: Path):
    dicom_metadata = {}

    structural_nifti_files = find_structural_nifti(visit_dir)
    diffusion_nifti_files = find_first_diffusion_nifti(visit_dir)
    nifti_files = structural_nifti_files + diffusion_nifti_files
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
    image_orientation_patient = [int(float(x)) for x in get_dicom_value('stack', 'ImageOrientationPatient')(subject, image_type).split('\\')]
    try:
        iop = image_orientation_map[tuple(image_orientation_patient)]
    except KeyError:
        if logger.isEnabledFor(logging.WARNING): logger.warning(f'Unknown image_orientation for {repr(image_orientation_patient)}')
        iop = 'Unknown'
    return iop

def get_patient_position(subject: SubjectData, image_type: str):
    pp_long = get_stack_value("ImageOrientationPatient")(subject, image_type)
    max_len = 50
    if len(pp_long) > max_len:
        pp_parts = pp_long.split('\\')
        over_by = len(pp_long) - max_len
        trunc_by = ceil(over_by / len(pp_parts))
        for idx, part in enumerate(pp_parts):
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

    @classmethod
    def get_modality(self, nii_path:Path) -> str:
        members = inspect.getmembers(self)
        for m, v in members:
            if not m.startswith('_') and nii_path.as_posix().find(m) > 0:
                return m
        return None


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
        'subject': ['ndar_subject', '01']
    }

    @classmethod
    def is_image_type(clazz, file_type: str) -> bool:
        if file_type in [NDARImageType.t1, NDARImageType.t2, NDARImageType.dti30b400, NDARImageType.dti60b1000, NDARImageType.dti6b500pepolar]:
            return True
        return False

    @classmethod
    def get_version_header(claszz, file_type: str):
        if NDARFileVariant.is_image_type(file_type):
            return NDARFileVariant.headers['image']
        else:
            return NDARFileVariant.headers['subject']


def get_demo_dict(demo_csv_file: Path) -> dict:
    """
    Convert the demographics.csv file into a dictionary where key=col name, value=value of col
    """
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

def get_race(sys_values, race: int) -> str:
    RACE_MAP = sys_values['maps']['race_map']
    try:
        race = RACE_MAP[race]
    except KeyError:
        race = "Unknown or not reported"
    return race

def get_phenotype(diag, diag_new, diag_new_dx):
    if diag_new != "" and int(diag_new) == 1:
        return str(diag_new_dx)
    else:
        return str(diag)

def get_phenotype_description(sys_values, diag, diag_new, diag_new_dx):
    phenotype = get_phenotype(diag, diag_new, diag_new_dx)
    PHENOTYPE_MAP = sys_values['maps']['phenotype_map']
    try:
        phenotype_desc = PHENOTYPE_MAP[phenotype]
    except KeyError:
        logger.error(f'No mapping for {phenotype}')
        phenotype_desc = EmptyString
    return phenotype_desc


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
    if "demo_ndar_guid" in subject.demographics and len(subject.demographics["demo_ndar_guid"].strip()):
        field_value =  subject.demographics["demo_ndar_guid"]
    else:
        field_value =  "NDARXXXXXXXX"
    return field_value

def get_image_description(subject: SubjectData, image_type: str) -> str:
    if image_type == NDARImageType.t1:
        field_value =  "SPGR"
    elif image_type in [NDARImageType.dti30b400, NDARImageType.dti60b1000, NDARImageType.dti6b500pepolar]:
        field_value =  "DTI"
    else:
        field_value =  "FSE"
    return field_value


SCAN_TYPE_MAP = {
    NDARImageType.t1: "MR structural (T1)",
    NDARImageType.t2: "MR structural (T2)",
    NDARImageType.dti30b400: "single-shell DTI",
    NDARImageType.dti60b1000: "single-shell DTI",
    NDARImageType.dti6b500pepolar: "single-shell DTI",
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
            field_value = float(re.sub(r'[^0-9\.]+', '', field_value))
        except:
            logger.error(f"cannot convert {field} [{field_value}] to float")

    if datatype == "Integer":
        try:
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

def handle_image_field(image_type: str, field_spec: dict, subject: SubjectData, sys_values: dict):
    IMAGE_MAP = sys_values['maps']['image_map']
    
    field =  field_spec[DefinitionHeader.ElementName]
    field_value = EmptyString
    try:
        field_value = IMAGE_MAP[field]
        if isinstance(field_value, str):
            if field_value.startswith('lambda'):
                func = eval(field_value)
                field_value = func(subject, image_type)
            elif field_value.startswith(('get', 'has')):
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


def handle_field(field_spec: dict, subject: SubjectData, sys_values: dict):
    field = field_spec[DefinitionHeader.ElementName]
    SUBJECT_MAP = sys_values['maps']['subject_map']
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


def write_ndar_csv(subject_data: SubjectData, ndar_csv_meta: TargetCSVMeta, sys_values):
    """
    Create ndar_subject01.csv using the demographics dictionary
    """
    if ndar_csv_meta.image_type:
        if ndar_csv_meta.image_type not in subject_data.nifti.keys() :
            logger.info("Skipping",ndar_csv_meta.image_type)
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
                    val = handle_image_field(ndar_csv_meta.image_type, field_spec, subject_data, sys_values)
                else:
                    val = handle_field(field_spec, subject_data, sys_values)
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

    p.add_argument(
        '--config', help="SIBIS General Configuration file",
        type=is_file("config", os.X_OK), default="~/.sibis-general-config.yml"
    )
    p.add_argument(
        '--sys_config', help="SIBIS System Configuration file",
        type=is_file("config", os.X_OK)
    )
    p.add_argument(
        '--source', help="Data source (hivalc or ncanda)",
        required=True, type=str
    )
    p.add_argument(
        '--subject', help="The Subject ID, typ: LAB_SXXXXX",
        required=True, type=str
    )
    p.add_argument(
        '--visit', help="The Visit ID, usually <visit>_<scan_id>",
        required=True, type=str
    )

    p.add_argument(
        '--ndar_dir', help="Base output directory for NDAR directory and CSV files to be written",
        type=is_dir("ndar_dir", os.X_OK | os.R_OK | os.W_OK, create_if_missing=True)
    )

    p.add_argument(
        '--verbose', '-v', action='count', default=0
    )

    ns =  p.parse_args(input_args)

    if ns.verbose > 0:
        change_log_level = ns.verbose * 10
        current_root_level = logging.getLogger().getEffectiveLevel()
        new_level = current_root_level - change_log_level
        if new_level < 0:
            new_level = 0
        logging.getLogger().setLevel(new_level)

    with ns.config.open("r") as fh:
        cfg = yaml.safe_load(fh)
        try:
            gen_cfg = cfg['ndar']['create_csv'][ns.source]

            fmt_env = {
                "subject": ns.subject,
                "visit": ns.visit
            }
            fmt_env.update(gen_cfg)
            
            ns.scan_dir = Path(gen_cfg['visit_dir'].format(**fmt_env))
            if not ns.scan_dir.exists():
                raise ConfigError(f"The `visit_dir`, {ns.scan_dir}, does not exist.")

            ns.visit_demographics = ns.scan_dir / gen_cfg['visit_demographics']
            if not ns.visit_demographics.exists():
                raise ConfigError(f"The `visit_demographics`, {gen_cfg['visit_demographics']}, does not exist in {ns.scan_dir}")

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
            
        

        except KeyError:
            p.exit(10, f"Could not find `ndar.create_csv` in {p.config.as_posix()}")
        except ConfigError as ce:
            p.exit(1, f"Configuration Error: {ce.msg}")

    return ns

def get_sys_config_values(args):
    """Returns the source specific demographics mappings"""
    if not args.sys_config:
        if args.source == "hivalc":
            sys_file = "/fs/share/operations/sibis_sys_config.yml"
        else:
            sys_file = "/fs/ncanda-share/operations/sibis_sys_config.yml"
    else:
        sys_file = args.sys_config

    sys_file_parser = cfg_parser.config_file_parser()
    err_msg = sys_file_parser.configure(sys_file)
    if err_msg:
        raise ConfigError(f"Could not get sys config file from {args}")

    if sys_file_parser.has_category("ndar_upload"):
        sys_vals = sys_file_parser.get_category('ndar_upload')[args.source]
    
    return sys_vals

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


def main(input_args: List[str] = None):
    logging.basicConfig(level=logging.WARNING,
                        format=r"%(asctime)s > %(name)s [%(levelname)s] %(message)s",
                        datefmt=r"%Y%m%dT%H:%M:%S")
    log_config = Path("logging.conf")
    if log_config.exists():
        logging.config.fileConfig(log_config.absolute().as_posix())

    args = _parse_args(input_args)
    scan_dir: Path = args.scan_dir # /fs/process/LAB_S01669/standard/20220517_6909_05172022
    ndar_dir: Path = args.ndar_dir / f"{args.subject}_{args.visit}" # /tmp/ndarupload/LAB_S01669_20220517_6909_05172022

    # Get the sibis_sys_config file
    sys_values = get_sys_config_values(args)

    # Create output directory if it doesn't exist.
    if not ndar_dir.exists():
        ndar_dir.mkdir(mode=0o775, parents=True, exist_ok=True)
    
    subject_definitions_csv = args.subject_definition
    image_definitions_csv = args.image_definition

    # Define the files we want to generate
    ndar_csv_meta_files = [
        TargetCSVMeta(
            ndar_dir / "ndar_subject01.csv",
            subject_definitions_csv),
        
        TargetCSVMeta(
            ndar_dir  / NDARImageType.t1 / "image03.csv",
            image_definitions_csv,
            NDARImageType.t1),

        TargetCSVMeta(
            ndar_dir  / NDARImageType.t2 / "image03.csv",
            image_definitions_csv,
            NDARImageType.t2),

        TargetCSVMeta(
            ndar_dir  / NDARImageType.dti30b400 / "image03.csv",
            image_definitions_csv,
            NDARImageType.dti30b400),

        TargetCSVMeta(
            ndar_dir  / NDARImageType.dti60b1000 / "image03.csv",
            image_definitions_csv,
            NDARImageType.dti60b1000),

        TargetCSVMeta(
            ndar_dir  / NDARImageType.dti6b500pepolar / "image03.csv",
            image_definitions_csv,
            NDARImageType.dti6b500pepolar),
    ]

    # find_diffusion_nifti(args.scan_dir)

    dicom_metadata = get_dicom_structural_metadata(args.scan_dir)
    nifti_metadata = get_nifti_metadata(args.scan_dir)
    dti_metadata = get_dicom_diffusion_metadata(args.scan_dir)

    # Get user demographic data which is needed for all ndar csv files
    demo_dict = get_demo_dict(args.visit_demographics)
    logger.debug('demo_dict: %s\n' % (demo_dict))

    subj_data = SubjectData(dicom_metadata, demo_dict, nifti_metadata, dti_metadata)

    # Create each CSV file
    for some_ndar_meta in ndar_csv_meta_files:
        logger.info(f'Starting  {scan_dir} ({some_ndar_meta})')
        write_ndar_csv(subj_data, some_ndar_meta, sys_values)
        write_bvec_bval_files(subj_data, some_ndar_meta)


if __name__ == '__main__':
    main()

