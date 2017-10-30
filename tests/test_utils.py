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
    print "Error; dcm2image: ", stderr
    sys.exit(1) 


(errcode, stdout, stderr) = sutils.gzip("-h")
if errcode : 
    print "Error; gzip: ", stderr
    sys.exit(1) 

(errcode, stdout, stderr) = sutils.zip(".","--help","")
if errcode : 
    print "Error; zip: ", stderr
    sys.exit(1) 

(errcode, stdout, stderr) = sutils.tar("--help")
if errcode : 
    print "Error; tar: ", stderr
    sys.exit(1) 

(errcode, stdout, stderr) = sutils.Rscript("-h")
if errcode : 
    print "Error; Rscript: ", stderr
    sys.exit(1) 

(errcode, stdout, stderr) = sutils.make_nifti("-h")
if errcode : 
    print "Error; make_nifti: ", stderr
    sys.exit(1) 

# Front end right now 
print "=== Front End " 
(ecode,sout,eout) = sutils.htmldoc("--help")
if ecode > 1 : 
    print "Error; htmldoc: ", eout
    sys.exit(1) 

assert(sutils.dicom2bxh(None,None))
assert(sutils.detect_adni_phantom("-man"))
# assert(sutils.sas(None))
# assert(sutils.manipula('-h'))
