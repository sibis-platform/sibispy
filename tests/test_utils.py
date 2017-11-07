#!/usr/bin/env python

##
##  Copyright 2017 SRI International
##  See COPYING file distributed along with the package for the copyright and license terms
##

import os
import sys
from sibispy import sibislogger as slog
from sibispy import utils as sutils
from sibispy import session as sess

#if sys.argv.__len__() > 1 : 
#    config_file = sys.argv[1]
#else :
#    config_file = os.path.join(os.path.dirname(sys.argv[0]), 'data', '.sibis-general-config.yml')

slog.init_log(False, False,'test_sibis_utils', 'test_sibis_utils',None)

(errcode, stdout, stderr) = sutils.dcm2image("--man")
if errcode : 
    print "Error: dcm2image: ", stderr

(errcode, stdout, stderr) = sutils.gzip("-h")
if errcode : 
    print "Error: gzip: ", stderr

(errcode, stdout, stderr) = sutils.zip(".","--help","")
if errcode : 
    print "Error: zip: ", stderr

(errcode, stdout, stderr) = sutils.tar("--help")
if errcode : 
    print "Error: tar: ", stderr

(errcode, stdout, stderr) = sutils.make_nifti("-h")
if errcode : 
    print "Error: make_nifti: ", stderr
else : 
    # required by make_nifti 
    (errcode, stdout, stderr) = sutils.sprlioadd("")
    if errcode : 
        print "Error: sprlioadd: ", stderr



# Front end right now 
print "=== Front End " 
(errcode, stdout, stderr) = sutils.Rscript("--help")
if errcode : 
    print "Error: Rscript: ", stderr

(ecode,sout,eout) = sutils.htmldoc("--help")
if ecode > 1 : 
    print "Error: htmldoc: ", eout

(ecode,sout,eout) = sutils.dicom2bxh(None,None)
if ecode != 255 : 
    print "Error: dicom2bxh: ", eout

(ecode,sout,eout) = sutils.detect_adni_phantom("--man")
if ecode : 
    print "Error: detect_adni_phantom: ", eout

# assert(sutils.sas(None))
# assert(sutils.manipula('-h'))
