from __future__ import print_function
# General Util functions
from builtins import str
from sibispy import sibislogger as slog
import subprocess
import re
import tempfile
import shutil
import os.path
import pandas
import hashlib

date_format_ymd = '%Y-%m-%d'

# "Safe" CSV export - this will catch IO errors from trying to write to a file
# that is currently being read and will retry a number of times before giving
# up. This function will also confirm whether the newly created file is
# different from an already existing file of the same name. Only changed files
# will be updated.
def safe_dataframe_to_csv(df, fname, verbose=False):
    import pandas
    import time
    import filecmp
    import os

    success = False
    retries = 10
    last_e = None

    while (not success) and (retries > 0):
        try:
            df.to_csv(fname + '.new', index=False)
            success = True
        except IOError as e:
            last_e = e
            if e.errno == 11:
                if verbose : 
                    print("Failed to write to csv ! Retrying in 5s...")
                time.sleep(5)
                retries -= 1
            else:
                retries = 0

    if not success:
        slog.info("safe_dataframe_to_csv",
                  f"ERROR: failed to write file {fname} with errno {last_e.errno}")
        return False

    # Check if new file is equal to old file
    if os.path.exists(fname) and filecmp.cmp(fname, fname + '.new',shallow=False):
        # Equal - remove new file
        os.remove(fname + '.new')
    else:
        # Not equal or no old file: put new file in its final place
        os.rename(fname + '.new', fname)
        if verbose:
            print("Updated", fname)

    return True

def dicom2bxh(dicom_path, bhx_file) :
    cmd = "dicom2bxh " 
    if dicom_path and bhx_file :
        cmd += "%s/* %s >& /dev/null" % ( dicom_path, bxh_file )
    else :
        cmd += "--help" 
        
    # Everything but 0 indicates an error occured 
    return call_shell_program(cmd)

def htmldoc(args) :
    return call_shell_program("htmldoc " + args)

dcm2image_cmd = 'cmtk dcm2image '
def dcm2image(args, verbose = False) :
    cmd = dcm2image_cmd + args
    if verbose : 
        print(cmd) 
    return call_shell_program(cmd)
 
def detect_adni_phantom(args) :
    return call_shell_program('cmtk detect_adni_phantom ' + args)

def gzip(args) :
    return  call_shell_program('gzip ' + args)

def zip(baseDir,zipFile,fileNames): 
    # if file already exists then first delete it otherwise zip returns code other than 0 ! 
    absZipFile = os.path.join(baseDir,zipFile)
    if os.path.exists(absZipFile):
        os.remove(absZipFile)
 
    cmd = 'cd %s; /usr/bin/zip -rqu %s %s' % (baseDir, zipFile, fileNames)
    return  call_shell_program(cmd)

def tar(args) :
    cmd = 'tar ' + args
    return call_shell_program(cmd)

def untar(tarfile, out_dir):
    args = "-xzf " + tarfile + " --directory=" + out_dir
    return tar(args)

def make_nifti(args):
    cmd = "makenifti " + args
    return call_shell_program(cmd)

# called by makenifti - we just have it hear for testing that makenifti runs 
def sprlioadd(args):
    cmd = "sprlioadd " + args
    return call_shell_program(cmd)

def mdb_export(args):
    cmd = "mdb-export " + args
    return call_shell_program(cmd)




def make_nifti_from_spiral(spiral_file, outfile):
    errcode, stdout, stderr =  make_nifti("-s 0 %s %s" % (spiral_file, outfile[:-7]))
    if os.path.exists(outfile[:-3]):
        gzip('-9 ' +  outfile[:-3])

    return errcode, stdout, stderr


def Rscript(args) : 
    return call_shell_program('/usr/bin/Rscript ' + args)
    

def call_shell_program(cmd):
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (out, err) = process.communicate()
    return (process.returncode, out, err)


#
# Formarly in Rwrapper.py 
#

# Label translation function - LimeSurvey to SRI/old REDCap style
def limesurvey_label_in_redcap( prefix, ls_label ):
    return "%s_%s" % (prefix, re.sub( '_$', '', re.sub( '[_\W]+', '_', re.sub( 'subjid', 'subject_id', ls_label.lower() ) ) ) )

# Map labels in a list according to a dictionary
def map_labels_to_dict( labels, ldict ):
    new_labels = list()
    for label in labels:
        if label in list(ldict.keys()):
            new_labels.append( ldict[label] )
        else:
            new_labels.append( label )
    return new_labels

# Score one record by running R script
def run_rscript( row, script, scores_key = None):
    tmpdir = tempfile.mkdtemp()

    data_csv = os.path.join( tmpdir, 'data.csv' )
    scores_csv = os.path.join( tmpdir, 'scores.csv' )

    pandas.DataFrame( row ).T.to_csv( data_csv )

    args = script + " " +  data_csv + " " + scores_csv 
    (errcode, stdout, stderr) = Rscript(args)
    if errcode :
        # because it is run by apply we need to raise error 
        raise slog.sibisExecutionError('utils.run_rscript.' + hashlib.sha1(str(stderr).encode()).hexdigest()[0:6], 'Error: Rscript failed !', err_msg= str(stderr), args= args)

    if scores_key : 
        scores = pandas.read_csv( scores_csv, index_col=0 )
        shutil.rmtree( tmpdir )
        return pandas.Series( name = row.name, data = scores.to_dict()[scores_key] )

    scores = pandas.read_csv( scores_csv, index_col=None )
    shutil.rmtree( tmpdir )
    return scores.ix[0]


"""
https://github.com/ActiveState/code/blob/master/recipes/Python/577982_Recursively_walk_Python_objects/recipe-577982.py
"""
from collections import Mapping, Set, Sequence 

# dual python 2/3 compatability, inspired by the "six" library
string_types = (str, str) if str is bytes else (str, bytes)
iteritems = lambda mapping: getattr(mapping, 'iteritems', mapping.items)()

def objwalk(obj, path=(), memo=None):
    if memo is None:
        memo = set()
    iterator = None
    if isinstance(obj, Mapping):
        iterator = iteritems
    elif isinstance(obj, (Sequence, Set)) and not isinstance(obj, string_types):
        iterator = enumerate
    if iterator:
        if id(obj) not in memo:
            memo.add(id(obj))
            for path_component, value in iterator(obj):
                for result in objwalk(value, path + (path_component,), memo):
                    yield result
            memo.remove(id(obj))
    else:
        yield path, obj
