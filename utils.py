# General Util functions
from sibispy import sibislogger as slog

import subprocess

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

    while (not success) and (retries > 0):
        try:
            df.to_csv(fname + '.new', index=False)
            success = True
        except IOError as e:
            if e.errno == 11:
                if verbose : 
                    print "Failed to write to csv ! Retrying in 5s..."
                time.sleep(5)
                retries -= 1
            else:
                retries = 0

    if not success:
        slog.info("safe_dataframe_to_csv","ERROR: failed to write file" + str(fname) + "with errno" + str(e.errno))
        return False

    # Check if new file is equal to old file
    if os.path.exists(fname) and filecmp.cmp(fname, fname + '.new',shallow=False):
        # Equal - remove new file
        os.remove(fname + '.new')
    else:
        # Not equal or no old file: put new file in its final place
        os.rename(fname + '.new', fname)
        if verbose:
            print "Updated", fname

    return True

def dicom2bxh(dicom_path, bhx_file) :
    cmd = "dicom2bxh " 
    if dicom_path and bhx_file :
        cmd += "%s/* %s >& /dev/null" % ( dicom_path, bxh_file )
        
    # Everything but 0 indicates an error occured 
    return not subprocess.call(cmd, shell=True)

def htmldoc(args) :
    return not subprocess.call("htmldoc " + args, shell=True)

dcm2image_cmd = 'cmtk dcm2image '
def dcm2image(args, verbose = False) :
    cmd = dcm2image_cmd + args
    if verbose : 
        print cmd 
    return not subprocess.call(cmd, shell=True)
 
def detect_adni_phantom(args) : 
    return not subprocess.call('cmtk detect_adni_phantom ' + args, shell=True)

def gzip(opt,file_path) :
    return not subprocess.call( [ 'gzip', opt, file_path ] )

def zip(baseDir,zipFile,fileNames): 
    cmd = 'cd %s; /usr/bin/zip -rqu %s %s' % (baseDir, zipFile, fileNames)
    return not subprocess.call(cmd, shell=True)

def tar(args) :
    return call_shell_program('tar ' + args)

def untar(tarfile, out_dir):
    args = "-xzf %(tarfile)s --directory=%(out_dir)s"
    args % {'tarfile':tarfile,
                 'out_dir':out_dir
                 }
    return tar(args)

def make_nifti(args):
    return call_shell_program("makenifti " + args)

def make_nifti_from_spiral(spiral_file, outfile):
    errcode, stdout, stderr =  make_nifti("-s 0 %s %s" % (spiral_file, outfile[:-7]))
    if os.path.exists(outfile[:-3]):
        gzip('-9',outfile[:-3])

    return errcode, stdout, stderr


def Rscript(args) : 
    return call_shell_program('/usr/bin/Rscript ' + args)
    

def sas(sas_script) :
    sas_path = os.path.join( os.path.expanduser("~"), '.wine', 'drive_c', 'Program Files', 'SAS', 'SAS 9.1', 'sas.exe' )
    if not sas_script : 
        return not subprocess.call( ['wine', sas_path, '-h'], stderr=devnull )

    sas_script_path = 'S:\\%s' % sas_script
    return not subprocess.call( ['wine', sas_path, '-SYSIN', sas_script_path, '-NOSPLASH'], stderr=devnull )


def manipula(man_script) :
    return subprocess.call( ['wine', os.path.join( os.path.expanduser("~"), 'src', 'manipula', 'Manipula.exe' ), man_script ], stderr=devnull )


def call_shell_program(cmd):
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = process.communicate()
    return process.returncode, out, err
