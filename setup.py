from setuptools import setup
import setuptools

setup(
	name='sibispy',
	version='1',
	packages=setuptools.find_packages(where='./sibispy'),
	package_dir={'':'sibispy'},
	entry_points={
	    'console_scripts': [
	      'bulk_mark = cmds.bulk_mark:main',
	      'change_complete_field_in_entry_and_import = cmds.change_complete_field_in_entry_and_import:main',
	      'change_status_of_complete_field = cmds.change_status_of_complete_field:main',
	      'download_dti_groundtruth = cmds.download_dti_groundtruth:main',
	      'exec_check_dti_gradients = cmds.exec_check_dti_gradients:main',
	      'exec_redcap_locking_data = cmds.exec_redcap_locking_data:main',
	      'recompute_autocalc = cmds.recompute_autocalc:main',
	      'redcap_update_summary_scores = cmds.redcap_update_summary_scores:main',
	    ]
	 },
	 scripts=[
	 	'sibispy/cmds/run_all_tests'
	 ],
	install_requires=[
	    'pandas>=1.0.1',
	    'numpy',
	    'PyYAML',
	    'future',
	    'pygithub',
# 	    'redcap'
	],
)

