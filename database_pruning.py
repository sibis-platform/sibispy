#!/usr/bin/env python

import os
import pyxnat

ifc = pyxnat.Interface(config=os.path.join(os.path.expanduser("~"),'.server_config/ncanda.cfg'))

#get phantom subject ids
criteria = [('xnat:subjectData/SUBJECT_LABEL', 'LIKE', '%-00000-P-0')]
fbirn_ids_search = ifc.select('xnat:subjectData', ['xnat:subjectData/SUBJECT_ID']).where(criteria)

fbirn_ids = fbirn_ids_search.get('subject_id')

criteria = [('xnat:subjectData/SUBJECT_LABEL', 'LIKE', '%-99999-P-9')]
adni_ids_search = ifc.select('xnat:subjectData',
['xnat:subjectData/SUBJECT_ID']).where(criteria)
adni_ids = adni_ids_search.get('subject_id')

project_subjects = ['NCANDA_S00061',
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

ids_to_keep = fbirn_ids + adni_ids + project_subjects

total_ids_search = ifc.select('xnat:subjectData',['xnat:subjectData/SUBJECT_ID'])
total_ids = total_ids_search.get('subject_id')

#ids_to_delete = set(total_ids).difference(set(ids_to_keep))

print "fbirn_ids: ", fbirn_ids
print "adni_ids: ", adni_ids
#print "ids_to_delete: ", ids_to_delete

fields_per_session = ['xnat:mrSessionData/PROJECT']

#for subj in ids_to_delete:
#    project_name = ifc.select( 'xnat:mrSessionData', fields_per_session ).where( [ ('xnat:mrSessionData/SUBJECT_ID','=',subj.strip()) ]  ).items()[0][0]
#    project_object=ifc.select.project(project_name)
#    subject_object = project_object.subject(subj)
#    subject_object.delete()

