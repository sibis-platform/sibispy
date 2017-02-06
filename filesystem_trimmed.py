#!/usr/bin/env python

import os
import pyxnat
import subprocess

ifc = pyxnat.Interface(config=os.path.join(os.path.expanduser("~"),'.server_config/ncanda.cfg'))

ids_to_keep = ['NCANDA_S00005',
 'NCANDA_S00016',
 'NCANDA_S00026',
 'NCANDA_S00039',
 'NCANDA_S00048',
 'NCANDA_S00006',
 'NCANDA_S00017',
 'NCANDA_S00027',
 'NCANDA_S00038',
 'NCANDA_S00049',
 'NCANDA_S00061',
 'NCANDA_S00076',
 'NCANDA_S00077',
 'NCANDA_S00093',
 'NCANDA_S00094',
 'NCANDA_S00042',
 'NCANDA_S00057',
 'NCANDA_S00078',
 'NCANDA_S00082',
 'NCANDA_S00092',
 'NCANDA_S00067',
 'NCANDA_S00081',
 'NCANDA_S00083',
 'NCANDA_S00084',
 'NCANDA_S00086',
 'NCANDA_S00073',
 'NCANDA_S00095',
 'NCANDA_S00098',
 'NCANDA_S00105',
 'NCANDA_S00112',
 'NCANDA_S00033',
 'NCANDA_S00034',
 'NCANDA_S00043',
 'NCANDA_S00051',
 'NCANDA_S00052']

map_subjects = {}

for subj in ids_to_keep:
    items = ifc.select( 'xnat:mrSessionData', ['xnat:mrSessionData/LABEL'] ) \
			   .where( [('xnat:mrSessionData/SUBJECT_ID','=', subj)] ).items()
    project_name = ifc.select( 'xnat:mrSessionData', ['xnat:mrSessionData/PROJECT'] ).where( [ ('xnat:mrSessionData/SUBJECT_ID','=',subj) ]  ).items()[0][0]
    map_subjects[subj] = [[x[0] for x in items], project_name]
     
 
files_to_copy = []
path_to_new_file = []

for key in map_subjects:
    for session in map_subjects.get(key)[0]:
        files_to_copy.append(('/fs/ncanda-xnat/archive/{1}/arc001/{0}').format(session, map_subjects.get(key)[1]))
        path_to_new_file.append(('/tmp/filesystem_trimmed/{1}/arc001').format(session, map_subjects.get(key)[1]))

for e in range(len(files_to_copy)): 
    make_dir_bash = "mkdir -p {0}".format(path_to_new_file[e])
    copy_files_bash = "rsync -a -r {0} {1}".format(files_to_copy[e], path_to_new_file[e])
    subprocess.call(make_dir_bash, shell=True)
    subprocess.call(copy_files_bash, shell=True)
