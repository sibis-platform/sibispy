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

assert(sutils.dcm2image("--man"))
assert(sutils.gzip("-h",""))
assert(sutils.zip(".","-h",""))
(errcode, stdout, stderr) = sutils.tar("-h")
assert(errcode)
(errcode, stdout, stderr) = sutils.make_nifti("-h")
assert(errcode)
(errcode, stdout, stderr) = sutils.Rscript("-h")
assert(errcode)

# Front end right now 
print "=== Front End " 
assert(sutils.htmldoc("-h"))
assert(sutils.dicom2bxh(None,None))
assert(sutils.detect_adni_phantom("-man"))
# assert(sutils.sas(None))
# assert(sutils.manipula('-h'))
