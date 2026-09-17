#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import logging
from dataclasses import dataclass
from datetime import datetime
import inspect
from math import ceil
from math import isclose
from pathlib import Path
from queue import Empty
from typing import Dict, Generator, List, Optional, Sequence

import csv
import os
import re
import shutil
import sys
import xml.etree.ElementTree as ET

import pandas as pd
from arrow import ParserError
import yaml

# --------- bootstrap path so helpers import cleanly ----------
scripts_dir = Path(__file__).parent / '..'
sys.path.append(str(scripts_dir.resolve()))

# ---------------- constants & logger ----------------
logger = logging.getLogger("ndar_create_csv")

EmptyString = ""
BVEC_FILE_NAME = "bvec"
BVAL_FILE_NAME = "bval"

# restrict builtins for legacy-eval only (HIVALC strings)
SAFE_BUILTINS = {
    "int": int, "str": str, "float": float, "len": len,
    "round": round, "max": max, "min": min,
}

# ---------------- data classes ----------------
@dataclass
class DiffusionMeta:
    bvalue: pd.DataFrame
    bvector: pd.DataFrame

@dataclass
class SubjectData:
    dicom: dict = None
    demographics: dict = None
    nifti: dict = None
    diffusion: Dict[str, DiffusionMeta] = None
    measurements: dict = None

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
    def get_modality(self, nii_path: Path) -> str:
        members = inspect.getmembers(self)
        for m, v in members:
            if not m.startswith('_'):
                m2 = m.replace('_', '-')
                if nii_path.as_posix().find(m2) > 0:
                    return m2
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

class NDARFileVariant:
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
    def is_image_type(cls, file_type: str) -> bool:
        return file_type in [
            NDARImageType.t1, NDARImageType.t2,
            NDARImageType.dti30b400, NDARImageType.dti60b1000, NDARImageType.dti6b500pepolar,
            NDARImageType.rs_fMRI, NDARImageType.swan
        ]

    @classmethod
    def is_measurements_type(cls, file_type: str) -> bool:
        return file_type in getattr(mappings, "measurements_file_list", [])

    @classmethod
    def get_version_header(cls, file_type: Optional[str]):
        if file_type and NDARFileVariant.is_image_type(file_type):
            return NDARFileVariant.headers['image']
        if file_type and NDARFileVariant.is_measurements_type(file_type):
            return NDARFileVariant.headers[file_type]
        return NDARFileVariant.headers['subject']

# ---------------- NIfTI / DICOM helpers ----------------
def describe_nifti(nifti_gz: Path) -> dict:
    meta = {}
    try:
        import nibabel as nib
    except ImportError:
        logger.error("nibabel is not installed. Please install it.")
        return meta

    try:
        img = nib.load(str(nifti_gz))
        hdr = img.header
        aff = img.affine

        space_unit, _ = hdr.get_xyzt_units()
        meta["UNITS"] = space_unit or "mm"

        for i in range(4):
            meta[f"I2PMAT{i}"] = " ".join(f"{x:g}" for x in aff[i])

        # extras (safe)
        meta["DIMS"] = "x".join(str(d) for d in img.shape)
        try:
            meta["ZOOMS"] = " ".join(f"{z:g}" for z in hdr.get_zooms())
        except Exception:
            pass
        try:
            meta["DATATYPE"] = str(hdr.get_data_dtype())
        except Exception:
            pass
        try:
            meta["QFORM_CODE"] = str(int(hdr["qform_code"]))
            meta["SFORM_CODE"] = str(int(hdr["sform_code"]))
        except Exception:
            pass

    except Exception as e:
        logger.error(f"Failed to read NIfTI metadata from {nifti_gz}: {e}")

    return meta

def find_nifti_xml(nifti_gz: Path) -> Path:
    nifti_gz = nifti_gz.resolve()
    return nifti_gz.parent / f"{nifti_gz.stem}.xml"

def get_element_for_cmtk_nifti_xml(nifti_xml: Path) -> ET.Element:
    with nifti_xml.open('r') as fh:
        try:
            xml_lines = fh.readlines()
            return ET.fromstringlist(xml_lines)
        except ET.ParseError:
            fh.seek(0)
            bad_xml_lines = fh.readlines()
            for idx, line in enumerate(bad_xml_lines):
                bad_xml_lines[idx] = line.replace("dicom:GE", "GE")
            bad_xml_lines.insert(1, '''<cmtk xmlns:dicom="https://sibis.sri.com/dicom" xmlns:GE="https://sibis.sri.com/dicom/ge">''')
            bad_xml_lines.append("</cmtk>")
            return ET.fromstringlist(bad_xml_lines)

def fill_modality_obj(meta_root: ET.Element, modality_obj: dict, section: str):
    for meta_elt in meta_root.findall(f".//{section}/"):
        tag_name = meta_elt.tag[meta_elt.tag.find("}")+1:]
        if tag_name in ["image", "dwi"] and len(list(meta_elt)) > 0:
            child_obj = {}
            for img_meta_elt in meta_elt:
                child_tag_name = img_meta_elt.tag[img_meta_elt.tag.find("}")+1:]
                child_tag_value = img_meta_elt.text
                child_obj.update({child_tag_name: {"value": child_tag_value}})
                if img_meta_elt.attrib:
                    child_obj[child_tag_name].update({"attribs": img_meta_elt.attrib})
            if child_obj:
                modality_obj[section].setdefault(tag_name, []).append(child_obj)
        else:
            modality_obj[section][tag_name] = {"value": meta_elt.text}
            if meta_elt.attrib:
                modality_obj[section][tag_name].update({"attribs": meta_elt.attrib})

def find_structural_nifti(args, visit_dir: Path) -> List[Path]:
    if args.source == 'ncanda':
        visit_dir = mappings.set_ncanda_visit_dir(args, 'structural')
    native = visit_dir / "structural" / "native"
    if not native.exists():
        return []
    return sorted(x.resolve() for x in native.rglob("*.nii.gz"))

def find_first_diffusion_nifti(args, visit_dir: Path) -> List[Path]:
    if args.source == 'ncanda':
        visit_dir = mappings.set_ncanda_visit_dir(args, 'diffusion')
    native = visit_dir / "diffusion" / "native"
    if not native.exists():
        return []
    if args.source == 'ncanda':
        ddirs = [x for x in native.iterdir() if x.is_dir()]
    else:
        ddirs = [x.resolve() for x in native.iterdir() if x.is_dir() and x.is_symlink()]
    files = []
    for d in ddirs:
        cand = sorted(d.rglob("*.nii.gz"))
        if cand:
            files.append(cand[0])
    return files

def find_first_rsfmri_nifti(args, visit_dir: Path) -> List[Path]:
    if args.source == 'ncanda':
        visit_dir = mappings.set_ncanda_visit_dir(args, 'restingstate')
    native = visit_dir / "restingstate" / "native"
    if not native.exists():
        return []
    files = []
    for d in [x for x in native.iterdir() if x.is_dir()]:
        cand = sorted(d.rglob("*.nii.gz"))
        if cand:
            files.append(cand[0])
    return files

def find_first_swan_nifti(args, visit_dir: Path) -> List[Path]:
    native = visit_dir / "iron" / "native"
    if not native.exists():
        return []
    cand = sorted(native.rglob("*.nii.gz"))
    return [cand[0]] if cand else []

def get_dicom_structural_metadata(args):
    visit_dir = args.scan_dir
    meta = {}
    nifti_files = \
        find_structural_nifti(args, visit_dir) + \
        find_first_diffusion_nifti(args, visit_dir) + \
        find_first_rsfmri_nifti(args, visit_dir) + \
        find_first_swan_nifti(args, visit_dir)
    for nifti_gz in nifti_files:
        xml = find_nifti_xml(nifti_gz)
        if not xml.exists():
            continue
        modality = NDARImageType.get_modality(nifti_gz)
        meta[modality] = {"device": {}, "mr": {}, "stack": {}}
        root = get_element_for_cmtk_nifti_xml(xml)
        fill_modality_obj(root, meta[modality], "device")
        fill_modality_obj(root, meta[modality], "mr")
        fill_modality_obj(root, meta[modality], "stack")
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(str(meta))
    return meta

def get_nifti_metadata(args):
    visit_dir = args.scan_dir
    meta = {}
    nifti_files = \
        find_structural_nifti(args, visit_dir) + \
        find_first_diffusion_nifti(args, visit_dir) + \
        find_first_rsfmri_nifti(args, visit_dir) + \
        find_first_swan_nifti(args, visit_dir)
    for nifti_gz in nifti_files:
        modality = NDARImageType.get_modality(nifti_gz)
        meta[modality] = describe_nifti(nifti_gz)
    return meta

def as_num(v: str):
    try:
        return int(v)
    except ValueError:
        return float(v)

def get_bvector_bvalue(nii_xml_files: Sequence[Path]) -> DiffusionMeta:
    bval, bvec = [], []
    for p in nii_xml_files:
        root = get_element_for_cmtk_nifti_xml(p)
        b_value = root.findall('.//mr/dwi/bValue')
        if len(b_value) == 1:
            bval.append(as_num(b_value[0].text))
        b_vector = root.findall('.//mr/dwi/bVector')
        if len(b_vector) == 1:
            bvec.append(map(as_num, b_vector[0].text.split(' ')))
    return DiffusionMeta(pd.DataFrame(bval), pd.DataFrame(bvec))

def find_diffusion_nifti_xml(args, visit_dir: Path):
    if args.source == 'ncanda':
        visit_dir = mappings.set_ncanda_visit_dir(args, 'diffusion')
    native = visit_dir / "diffusion" / "native"
    scans = {}
    if not native.exists():
        return scans
    if args.source == 'hivalc':
        ddirs = [x.resolve() for x in native.iterdir() if x.is_dir() and x.is_symlink()]
        syms = [x for x in native.iterdir() if x.is_dir() and x.is_symlink()]
        for symlink, ddir in zip(syms, ddirs):
            scans[symlink.name] = sorted(ddir.rglob("*.nii.xml"))
    else:
        ddirs = [x for x in native.iterdir() if x.is_dir()]
        for d in ddirs:
            scans[d.name] = sorted(d.rglob("*.nii.xml"))
    return scans

def get_dicom_diffusion_metadata(args) -> Dict[str, DiffusionMeta]:
    all_xml = find_diffusion_nifti_xml(args, args.scan_dir)
    out = {}
    for kind, xmls in all_xml.items():
        out[kind] = get_bvector_bvalue(xmls)
    return out

# ---------------- value helpers used by mappings ----------------
def get_stack_value(stack_key: str):
    def fn(subject: SubjectData, image_type: str):
        try:
            obj = subject.dicom[image_type]["stack"][stack_key]
            val = obj["value"]
            if "attribs" in obj and "units" in obj["attribs"]:
                val = " ".join([val, obj["attribs"]["units"]])
            return val
        except KeyError:
            return EmptyString
    return fn

def get_dicom_value(section: str, key: str):
    def fn(subject: SubjectData, image_type: str):
        try:
            obj = subject.dicom[image_type][section][key]
            val = obj["value"]
            if "attribs" in obj and "units" in obj["attribs"]:
                val = " ".join([val, obj["attribs"]["units"]])
            return val
        except KeyError:
            return EmptyString
    return fn

def get_nifti_value(key: str):
    def fn(subject: SubjectData, image_type: str):
        try:
            return subject.nifti[image_type][key]
        except KeyError:
            return EmptyString
    return fn

def get_field_of_view_pd(subject: SubjectData, image_type: str):
    ped = get_dicom_value('mr', 'phaseEncodeDirection')(subject, image_type)
    px = get_dicom_value('mr', 'PixelSpacing')(subject, image_type).split('\\')
    if len(px) < 2:
        return EmptyString
    col_ps = float(px[0])
    row_ps = float(px[1])
    cols = int(get_dicom_value('mr', 'Columns')(subject, image_type))
    rows = int(get_dicom_value('mr', 'Rows')(subject, image_type))
    if str(ped).upper() == "COL":
        fov = [cols * col_ps, rows * row_ps]
    else:
        fov = [rows * row_ps, cols * col_ps]
    return f"{fov[0]} x {fov[1]} Millimeters"

image_orientation_map = {
    (1,0,0,0,0,-1): 'Coronal',
    (0,1,0,0,0,-1): 'Sagittal',
    (1,0,0,0,1,0):  'Axial',
}

def get_image_orientation(subject: SubjectData, image_type:str):
    try:
        parts = [round(float(x)) for x in get_dicom_value('stack', 'ImageOrientationPatient')(subject, image_type).split('\\')]
        return image_orientation_map.get(tuple(parts), "Unknown")
    except Exception:
        logger.error('Unknown image_orientation')
        return "Unknown"

def _normalize_orient_number(x: float) -> str:
    # Snap near-zeros and near-ones first
    if isclose(x, 0.0, abs_tol=1e-6):
        return "0"
    if isclose(x, 1.0, abs_tol=1e-6):
        return "1"
    if isclose(x, -1.0, abs_tol=1e-6):
        return "-1"
    # Otherwise: round to 2 decimals, strip trailing zeros/decimal
    s = f"{x:.2f}".rstrip('0').rstrip('.')
    return "0" if s in {"-0", "+0"} else s

def get_patient_position(subject: SubjectData, image_type: str):
    """
    Returns ImageOrientationPatient as 6 backslash-separated numbers,
    with each number reduced to a very short string:
      - 0, 1, or -1 when close to those values
      - else up to 2 decimal places (e.g., 0.33, -0.5, 0.12)
    """
    pp_long = get_stack_value("ImageOrientationPatient")(subject, image_type)
    if not isinstance(pp_long, str) or not pp_long.strip():
        return pp_long  # nothing to do

    parts = [p.strip() for p in pp_long.split('\\') if p.strip() != ""]

    if len(parts) != 6:
        logger.warning("ImageOrientationPatient expected 6 numbers, got %d: %r", len(parts), pp_long)
        # We'll still try to normalize whatever is present

    normalized = []
    for i, p in enumerate(parts):
        try:
            x = float(p)
            normalized.append(_normalize_orient_number(x))
        except Exception:
            # Keep the original token (shortened if it's absurdly long) and log it
            if len(p) > 16:
                logger.warning("Non-numeric ImageOrientationPatient token too long at idx %d: %r", i, p)
                p = p[:16]
            normalized.append(p)

    # If we had fewer/more than 6 parts, just join what we have;
    # most DICOMs should have 6 and the above will preserve that.
    result = '\\'.join(normalized)

    # Optional: enforce an overall length guard (rarely needed now, keeping out for now)
    # max_len = 50
    # if len(result) > max_len:
    #     logger.warning("ImageOrientationPatient normalized length %d exceeds %d, trimming minimally.", len(result), max_len)
    #     # As a last resort, truncate each component from the right but keep structure
    #     tokens = result.split('\\')
    #     # ensure we keep at least 2 characters per token (handles "-1", "0")
    #     budget = max_len - (len(tokens) - 1)  # account for backslashes
    #     per = max(2, budget // len(tokens))
    #     tokens = [t[:per] for t in tokens]
    #     result = '\\'.join(tokens)

    return result

# race / subjectkey / scan-type helpers
GUID_RE = re.compile(r'^NDAR_?[A-Z0-9]{8,12}$')

def get_race(race: int) -> str:
    RACE_MAP = getattr(mappings, "race_map", {})
    return RACE_MAP.get(race, "Unknown or not reported")

def get_subjectkey(subject: SubjectData, *_):
    val = (subject.demographics.get("demo_ndar_guid")
           or subject.demographics.get("ndar_guid_id")
           or "").strip().upper()
    if not GUID_RE.match(val):
        logger.warning("subjectkey [%s] is not of type GUID", val)
    return val

def get_image_description(subject: SubjectData, image_type: str) -> str:
    if image_type == NDARImageType.t1:
        return "SPGR"
    if image_type == NDARImageType.rs_fMRI:
        return "fMRI"
    if image_type in [NDARImageType.dti30b400, NDARImageType.dti60b1000, NDARImageType.dti6b500pepolar]:
        return "DTI"
    if image_type == NDARImageType.swan:
        return "SWAN-QSM"
    return "FSE"

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
    return SCAN_TYPE_MAP.get(image_type, EmptyString)

DIFFUSION_MODALITIES = [NDARImageType.dti30b400, NDARImageType.dti60b1000, NDARImageType.dti6b500pepolar]

def has_bvek_bval_files(subject: SubjectData, image_type: str):
    return "Yes" if (image_type in DIFFUSION_MODALITIES and image_type in subject.diffusion.keys()) else ""

unit_map = {
    "in": "Inches", "cm": "Centimeters", "ang": "Angstroms", "nm": "Nanometers",
    "um": "Micrometers", "mm": "Millimeters", "m": "Meters", "km": "Kilometers",
    "mi": "Miles", "ns": "Nanoseconds", "us": "Microseconds", "ms": "Milliseconds",
    "s": "Seconds", "min": "Minutes", "hr": "Hours", "hz": "Hertz", "fn": "frame number"
}
def get_image_units(subject: SubjectData, image_type: str):
    try:
        return unit_map[get_nifti_value("UNITS")(subject, image_type)]
    except KeyError:
        return unit_map["mm"]

# ---------------- field conformance & checks ----------------
def conform_field_specs_datatype(field_value, field_spec: dict, subject: SubjectData):
    field = field_spec[DefinitionHeader.ElementName]
    dtype = field_spec[DefinitionHeader.DataType]

    if dtype == "String":
        if not isinstance(field_value, str):
            logger.warning(f"{field} [{field_value}] is not of type {dtype}")
            field_value = str(field_value)

    elif dtype == "GUID":
        # Normalize to uppercase string
        val = ("" if field_value is None else str(field_value)).strip().upper()

        # Warn ONLY for the primary subjectkey; never warn for subjectkey_* relatives
        if field.lower() == "subjectkey":
            if val and not GUID_RE.match(val):
                logger.warning(f"{field} [{field_value}] is not of type {dtype}")

        # Always return the normalized value; no warnings for other GUID fields
        return val

    elif dtype == "Float":
        try:
            if field_value != "":
                field_value = float(re.sub(r'[^0-9\.]+', '', str(field_value)))
        except Exception:
            logger.error(f"cannot convert {field} [{field_value}] to float")

    elif dtype == "Integer":
        try:
            if field_value != "":
                field_value = int(round(float(field_value)))
        except Exception:
            logger.error(f"cannot convert {field} [{field_value}] to int")

    elif dtype == "Date":
        try:
            pd.to_datetime(str(field_value), errors='raise')
        except (ParserError, ValueError):
            logger.error(f"cannot parse {field} [{field_value}] as a date")

    return field_value

def check_field_specs(value, field_spec: dict, subject: SubjectData):
    field = field_spec[DefinitionHeader.ElementName]
    if field_spec[DefinitionHeader.Required] == 'Required' and value in (EmptyString, None):
        logger.warning(f"Required Field, {field}, is missing a value!")

    sz = field_spec.get(DefinitionHeader.Size)
    if sz not in (None, EmptyString):
        if len(str(value)) > int(sz):
            logger.warning(f"Field, {field} is {len(str(value))} and exceeds the maximum size {sz}!")

# ---------------- measurements helpers ----------------
def get_add_measurements_metadata(args):
    # currently only ASR
    try:
        summaries_path = mappings.set_ncanda_visit_dir(args, 'additional')
    except Exception:
        return {}
    files = list(summaries_path.glob('*.csv'))
    follow_yr = f"followup_{args.followup_year}y"
    out = {}
    for f in files:
        if f.name == "asr.csv" and f.exists():
            try:
                df = pd.read_csv(f)
                s_df = df.loc[(df['subject'] == args.subject) & (df['visit'] == follow_yr)]
                out[f.name.split('.')[0]] = s_df.reset_index(drop=True)
            except Exception:
                logger.warning(f"Error getting additional summary values from file: {f}")
    return out

def get_measurements_metadata(args):
    if args.source == 'hivalc':
        return {}
    redcap_path = mappings.set_ncanda_visit_dir(args, 'redcap')
    meta = {}
    for f in (redcap_path / 'measures').glob('*'):
        key = f.name.split('.', 1)[0]
        try:
            meta[key] = pd.read_csv(f)
        except Exception:
            logger.warning(f"Could not read {f}")
    meta.update(get_add_measurements_metadata(args))
    return meta

def recode_missing(field_spec):
    miss_list = ['lr_rawscore', 'wr_rawscore', 'wr_totalrawscore', 'wr_standardscore']
    el = field_spec['ElementName']
    if el.startswith('asr'):
        idx = field_spec['Notes'].find('Missing') - 3
        return field_spec['Notes'][idx:(idx+2)]
    if el in miss_list:
        return int(field_spec['ValueRange'].split(';')[-1])
    return EmptyString

def test_reverse(map_row):
    reverse_str = "reverse scored"
    notes = map_row['Notes'].values[0]
    return isinstance(notes, str) and reverse_str in notes.lower()

def reverse_val(value, field_spec, map_row):
    try:
        ndar_range_str = field_spec['ValueRange'].split(';')[0]
        ends = list(map(int, re.findall(r'\d+', ndar_range_str)))
        ndar_range = list(range(ends[0], ends[1]+1)); ndar_range.reverse()

        ncanda_range_str = map_row['ncanda_value_range'].values[0]
        ncanda_range = list(map(int, re.findall(r'\d+', ncanda_range_str)))

        idx = ncanda_range.index(value)
        return ndar_range[idx]
    except Exception:
        logger.warning(f"Failed to reverse value of {field_spec['ElementName']}")
        return value

def get_measurements_source_val(ndar_csv_meta, field_spec, subject: SubjectData):
    mapping_file = Path(str(ndar_csv_meta.data_dictionary).replace("definitions", "mappings"))
    if not mapping_file.exists():
        logger.error(f'Cannot find the mapping file for {ndar_csv_meta.image_type}. Tried {mapping_file}')
        raise KeyError

    mapping_df = pd.read_csv(mapping_file)
    el = field_spec.get('ElementName')
    map_row = mapping_df.loc[mapping_df['NDA_ElementName'] == el]

    try:
        csv_name = map_row.get('ncanda_csv').iloc[0]
        ncanda_var = map_row.get('ncanda_variable').iloc[0]
        df = subject.measurements.get(csv_name)
        if not pd.isna([ncanda_var, df, csv_name]).any():
            val = df[ncanda_var].iloc[0]
            if pd.isna(val):
                val = recode_missing(field_spec)
            elif val != '' and test_reverse(map_row):
                val = reverse_val(val, field_spec, map_row)
            return str(val)
    except IndexError:
        raise KeyError

    if field_spec['Required'] == 'Required':
        return str(recode_missing(field_spec))
    raise KeyError

# ---------------- legacy-safe mapping resolution ----------------
def _eval_ctx(subject, image_type=None):
    g = {"__builtins__": SAFE_BUILTINS, "mappings": mappings}
    # expose helper names old (HIVALC) strings may reference
    g.update({
        "get_subjectkey": get_subjectkey,
        "get_race": get_race,
        "get_dicom_value": get_dicom_value,
        "get_stack_value": get_stack_value,
        "get_nifti_value": get_nifti_value,
        "get_image_units": get_image_units,
        "get_image_orientation": get_image_orientation,
        "get_patient_position": get_patient_position,
        "get_field_of_view_pd": get_field_of_view_pd,
        "has_bvek_bval_files": has_bvek_bval_files,
        "get_image_description": get_image_description,
        "get_scan_type": get_scan_type,
    })
    l = {
        "subject": subject,
        "src_dfs": {"demographics": subject.demographics},
    }
    if image_type is not None:
        l["image_type"] = image_type
    return g, l

def _call_maybe(fn, subject, image_type=None):
    try:
        if image_type is None:
            return fn(subject)
        return fn(subject, image_type)
    except TypeError:
        try:
            return fn(subject)
        except TypeError:
            return fn()

def _eval_legacy_spec(spec_str: str, subject, image_type=None):
    s = spec_str.strip()
    g, l = _eval_ctx(subject, image_type)

    if s.startswith("lambda"):
        func = eval(s, g, l)
        return _call_maybe(func, subject, image_type)

    if s.startswith(("get", "has", "mappings.")):
        obj = eval(s, g, l)
        if callable(obj):
            return _call_maybe(obj, subject, image_type)
        return obj

    return s  # literal like "No", "NIFTI", etc.

def _resolve_spec(spec, subject, *, image_type=None, legacy_ok=True):
    if callable(spec):
        return _call_maybe(spec, subject, image_type)
    if isinstance(spec, str) and legacy_ok:
        return _eval_legacy_spec(spec, subject, image_type)
    return spec

def _render_field(field_spec: dict, subject: SubjectData, *, image_type: Optional[str], ndar_csv_meta=None):
    field = field_spec[DefinitionHeader.ElementName]
    # choose map
    if image_type and NDARFileVariant.is_image_type(image_type):
        mapping_dict = getattr(mappings, "image_map", {})
    elif image_type and NDARFileVariant.is_measurements_type(image_type):
        mapping_dict = getattr(mappings, "MEASUREMENTS_MAP", {})
    else:
        mapping_dict = getattr(mappings, "subject_map", None) or getattr(mappings, "src_to_ndar_map", {})

    try:
        if field in mapping_dict:
            value = _resolve_spec(mapping_dict[field], subject, image_type=image_type, legacy_ok=True)
        elif image_type and NDARFileVariant.is_measurements_type(image_type):
            value = get_measurements_source_val(ndar_csv_meta, field_spec, subject)
        else:
            value = EmptyString

        value = conform_field_specs_datatype(value, field_spec, subject)

    except KeyError:
        value = EmptyString
    except Exception as e:
        logger.warning("Exception for %s: %s", field, e)
        value = EmptyString

    check_field_specs(value, field_spec, subject)
    return value

# ---------------- CSV writing ----------------
def write_ndar_csv(subject_data: SubjectData, ndar_csv_meta: 'TargetCSVMeta'):
    if ndar_csv_meta.image_type:
        if (ndar_csv_meta.image_type not in subject_data.nifti.keys()
            and not NDARFileVariant.is_measurements_type(ndar_csv_meta.image_type)):
            logger.info(f"Skipping {ndar_csv_meta.image_type}")
            return

    ndar_csv_meta.output_file.parent.mkdir(parents=True, exist_ok=True)

    with ndar_csv_meta.output_file.open('w') as ndar_file:
        header_row, val_row = [], []
        csvwriter = csv.writer(ndar_file, quoting=csv.QUOTE_NONNUMERIC)
        with open(ndar_csv_meta.data_dictionary) as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=',')
            for field_spec in csv_reader:
                val = _render_field(field_spec, subject_data, image_type=ndar_csv_meta.image_type, ndar_csv_meta=ndar_csv_meta)
                header_row.append(field_spec[DefinitionHeader.ElementName])
                val_row.append(val)
        csvwriter.writerow(NDARFileVariant.get_version_header(ndar_csv_meta.image_type))
        csvwriter.writerow(header_row)
        csvwriter.writerow(val_row)

def write_bvec_bval_files(subject: SubjectData, ndar_meta: 'TargetCSVMeta'):
    b_path = ndar_meta.output_file.parent
    if ndar_meta.image_type in DIFFUSION_MODALITIES and ndar_meta.image_type in subject.diffusion:
        meta = subject.diffusion[ndar_meta.image_type]
        if len(meta.bvalue) > 0:
            meta.bvalue.T.to_csv(b_path / BVAL_FILE_NAME, sep=' ', quoting=None, index=False, header=False)
        if len(meta.bvector) > 0:
            meta.bvector.T.to_csv(b_path / BVEC_FILE_NAME, sep=' ', quoting=None, float_format='%.5f', index=False, header=False)

# ---------------- demo & CLI ----------------
def get_demo_dict(args) -> dict:
    demo_csv = args.visit_demographics
    if args.source == 'ncanda':
        # CURRENTLY SET TO READ DEMO DATA FROM CASES RATHER THAN INTERNAL RELEASE
        demo_csv = mappings.set_ncanda_visit_dir(args, 'redcap', use_cases_override=True, cases_base_override="/fs/ncanda-share") / 'measures' / 'demographics.csv'
    with demo_csv.open() as f:
        reader = csv.reader(f, delimiter=',')
        cols = {}
        demo = {}
        for i, row in enumerate(reader):
            if i == 0:
                for j, col in enumerate(row):
                    cols[j] = col
            else:
                for j, val in enumerate(row):
                    demo[cols[j]] = val
        return demo

@dataclass
class TargetCSVMeta:
    output_file: Path
    data_dictionary: Path
    image_type: Optional[str] = None
    def __str__(self) -> str:
        return f"output_file: {self.output_file}, data_dictionary: {self.data_dictionary}, image_type: {self.image_type}"

@dataclass
class ConfigError(Exception):
    msg: str

def is_dir(arg_name: str = "Path", mode: int = os.R_OK | os.W_OK | os.X_OK , create_if_missing: bool =  False):
    def is_dir_path(dir_path: str) -> Path:
        maybe_path = Path(dir_path)
        if maybe_path.exists() and maybe_path.is_dir():
            if os.access(maybe_path.as_posix(), mode):
                return maybe_path
            raise argparse.ArgumentTypeError(f"{arg_name}: {maybe_path} has incorrect access permissions")
        if create_if_missing and not maybe_path.exists():
            try:
                maybe_path.mkdir(mode=0o775, parents=True, exist_ok=True)
                return maybe_path
            except Exception:
                raise argparse.ArgumentTypeError(f"{arg_name}: {maybe_path} had a problem creating directory")
        raise argparse.ArgumentTypeError(f"{arg_name}: {maybe_path} is not a valid path")
    return is_dir_path

def is_file(arg_name: str = "Path", mode: int = os.R_OK):
    def is_file_path(file_path: str) -> Path:
        maybe_path = Path(file_path).expanduser()
        if maybe_path.exists() and maybe_path.is_file():
            return maybe_path
        raise argparse.ArgumentTypeError(f"{arg_name}: {maybe_path} is not a valid path")
    return is_file_path

def _parse_args(input_args: List[str] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser()

    cfg_args = p.add_argument_group('Config')
    cfg_args.add_argument('--config', type=is_file("config", os.X_OK),
                          default="/fs/storage/share/operations/secrets/.sibis/.sibis-general-config.yml",
                          help="SIBIS General Configuration file")
    cfg_args.add_argument('--sys_config', type=is_file("config", os.X_OK), help="SIBIS System Configuration file")
    cfg_args.add_argument('--ndar_dir', type=is_dir("ndar_dir", os.X_OK | os.R_OK | os.W_OK, create_if_missing=True),
                          help="Base output directory for NDAR directory and CSV files to be written")
    cfg_args.add_argument('--mappings-dir', dest='mappings_dir_cli', type=is_dir("mappings_dir", os.X_OK | os.R_OK),
                          help='Override the mappings directory (directory containing *_mappings.py)')
    cfg_args.add_argument('--verbose', '-v', action='count', default=0)
    cfg_args.add_argument('--mappings_dir', type=is_dir("mappings_dir", os.R_OK | os.X_OK),
                          help="Override path to dir containing mappings module (e.g., hivalc_mappings.py or ncanda_mappings.py).")

    sub = p.add_subparsers(title='Data Source', dest='source')

    hivalc = sub.add_parser('hivalc', help='Hivalc CSV Creation')
    hivalc.add_argument('--subject', required=True, type=str)
    hivalc.add_argument('--visit', required=True, type=str)
    hivalc.add_argument('--arm', required=True, type=str)

    ncanda = sub.add_parser('ncanda', help='Ncanda CSV Creation')
    ncanda.add_argument('--subject', required=True, type=str)
    ncanda.add_argument('--release_year', required=True, type=str)
    ncanda.add_argument('--followup_year', required=False, type=str)

    ns = p.parse_args(input_args)

    if ns.verbose > 0:
        change_log_level = ns.verbose * 10
        current_root_level = logging.getLogger().getEffectiveLevel()
        new_level = max(0, current_root_level - change_log_level)
        logging.getLogger().setLevel(new_level)

    if ns.source == 'ncanda' and ns.followup_year is None:
        ns.followup_year = ns.release_year

    with ns.config.open("r") as fh:
        cfg = yaml.safe_load(fh)
        try:
            gen_cfg = cfg['ndar']['create_csv'][ns.source]

            if ns.source == 'hivalc':
                fmt_env = {"subject": ns.subject, "arm": ns.arm, "visit": ns.visit}
                fmt_env.update(gen_cfg)
                ns.scan_dir = Path(gen_cfg['visit_dir'].format(**fmt_env))
                if not ns.scan_dir.exists():
                    raise ConfigError(f"The `visit_dir`, {ns.scan_dir}, does not exist.")
                ns.visit_demographics = ns.scan_dir / gen_cfg['visit_demographics']
                if not ns.visit_demographics.exists():
                    raise ConfigError(f"The `visit_demographics`, {gen_cfg['visit_demographics']}, does not exist in {ns.scan_dir}")
                ns.measurements_definitions = None
            else:
                cfg_paths = cfg['ndar']['create_csv'][ns.source]
                ns.scan_dir = Path(cfg_paths['visit_dir'])
                ns.visit_demographics = ns.scan_dir
                ns.measurements_definitions = Path(gen_cfg['definition_dir']) / 'measurements'
                if not ns.measurements_definitions.exists():
                    raise ConfigError(f"The measurements definitions dir is missing from {ns.measurements_definitions}")

            if ns.ndar_dir is None:
                ns.ndar_dir = Path(gen_cfg['output_dir'])
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

            cfg_map_dir = Path(gen_cfg['mappings_dir']).expanduser()
            ns.mappings_dir = Path(ns.mappings_dir_cli).expanduser() if getattr(ns, 'mappings_dir_cli', None) else cfg_map_dir
            if not ns.mappings_dir.exists() or not ns.mappings_dir.is_dir():
                raise ConfigError(f"The mappings dir does not exist or is not a directory: {ns.mappings_dir}")

        except KeyError:
            p.exit(10, f"Could not find `ndar.create_csv` in {ns.config.as_posix()}")
        except ConfigError as ce:
            p.exit(1, f"Configuration Error: {ce.msg}")

    return ns

def set_output_dir(args):
    if args.source == 'hivalc':
        return args.ndar_dir / f"{args.subject}_{args.visit}"
    if args.followup_year == '0':
        return args.ndar_dir / args.subject / "baseline"
    return args.ndar_dir / args.subject / f"followup_{args.followup_year}y"

def set_dir_paths(args):
    return args.scan_dir, set_output_dir(args)

# ---------------- main ----------------
def main(input_args: List[str] = None):
    logging.basicConfig(level=logging.WARNING,
                        format=r"%(asctime)s > %(name)s [%(levelname)s] %(message)s",
                        datefmt=r"%Y%m%dT%H:%M:%S")

    args = _parse_args(input_args)

    # ensure mappings module importable
    sys.path.insert(0, str(Path(args.mappings_dir).resolve()))
    if args.source == 'hivalc':
        globals()['mappings'] = __import__('hivalc_mappings')
    else:
        globals()['mappings'] = __import__('ncanda_mappings')

    scan_dir, ndar_dir = set_dir_paths(args)
    ndar_dir.mkdir(mode=0o775, parents=True, exist_ok=True)

    subject_definitions_csv = args.subject_definition
    image_definitions_csv = args.image_definition
    measurements_definitions = args.measurements_definitions

    files_to_generate = mappings.files_to_generate
    ndar_csv_meta_files: List[TargetCSVMeta] = []

    for file_name in files_to_generate:
        file_type = file_name.split('/', 1)[0]
        if file_type == 'ndar_subject01':
            meta = TargetCSVMeta(ndar_dir / "ndar_subject01.csv", subject_definitions_csv)
        elif NDARFileVariant.is_image_type(file_type):
            meta = TargetCSVMeta(ndar_dir / file_type / "image03.csv", image_definitions_csv, file_type)
        else:
            measurements_file_type = file_name.split('/', 1)[1]
            measurements_type = measurements_file_type.split('.')[0]
            definition = measurements_definitions / f"{measurements_type}_definitions.csv"
            meta = TargetCSVMeta(ndar_dir / file_type / measurements_file_type, definition, measurements_type)
        ndar_csv_meta_files.append(meta)

    dicom_metadata = get_dicom_structural_metadata(args)
    nifti_metadata = get_nifti_metadata(args)
    dti_metadata = get_dicom_diffusion_metadata(args)
    measurements_metadata = get_measurements_metadata(args)

    demo_dict = get_demo_dict(args)
    logger.debug('demo_dict: %s', demo_dict)

    subject_data = SubjectData(dicom_metadata, demo_dict, nifti_metadata, dti_metadata, measurements_metadata)

    for meta in ndar_csv_meta_files:
        logger.info(f"Starting  {scan_dir} ({meta})")
        write_ndar_csv(subject_data, meta)
        write_bvec_bval_files(subject_data, meta)

if __name__ == '__main__':
    main()
