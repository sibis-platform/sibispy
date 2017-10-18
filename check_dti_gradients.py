import os
import glob
import re
import hashlib
import numpy as np
from lxml import objectify

import sibispy
from sibispy import sibislogger as slog
from sibispy import config_file_parser as cfg_parser

class check_dti_gradients(object):
    """
    SIBIS Check DTI Gradient Object
    ====================
    Main object that provides functionality for checking gradient tables of DTI Sequence 
    
    assumes 
      from sibispy import sibislogger as slog
      slog.init_log() 
      is called before 
      and operations/sibis_config.yml is properly defined 

    """
    def __init__(self):
        self.__decimals = -1
        self.__cases_dir = None
        self.__sibis_defs = dict() 
        self.__gt_gradients_dict = dict()


    def configure(self, sessionObj=None, check_decimals=1):
        """
        Configures object by first checking for an
        environment variable, then in the home directory.
        """

        self.__cases_dir = sessionObj.get_cases_dir() 
        self.__decimals = check_decimals
        
        #
        # Define sibis_defs
        #
        (cfgParser,err_msg) = sessionObj.get_config_sys_parser()
        if err_msg:
            slog.info('check_dti_gradients.configure',str(err_msg))
            return False

        self.__sibis_defs= cfgParser.get_category('check_dti_gradients')

        #
        # Define gt_gradients_dict
        #
        if not len(self.__sibis_defs['ground_truth']):
            slog.info('check_dti_gradients.configure',"Error: No ground truth for DTI defined !")
            return False

        # First define path so that later we can use that function to easily install the ground truth on a new system
        gt_path_dict = self.get_ground_truth_gradient_path_dict()
        if not len(gt_path_dict) :
            slog.info('check_dti_gradients.configure','Error: Failed to determine path to ground truth cases')
            return False

        # Load in all ground truth gradients
        sequence_map  = self.__sibis_defs.get('sequence')
        errorFlag = False
        for SCANNER in gt_path_dict.iterkeys():
            scanner_dict = dict()
            scanner_map = gt_path_dict.get(SCANNER)
            for MODEL in scanner_map.iterkeys():
                model_dict = dict()
                model_map = scanner_map.get(MODEL)
                for SEQUENCE in sequence_map.iterkeys(): 
                    model_dict[SEQUENCE] = self.__load_ground_truth_gradients(model_map[SEQUENCE])
                    if not len(model_dict[SEQUENCE]) : 
                       errorFlag = True 

                scanner_dict[MODEL] = model_dict
                
            self.__gt_gradients_dict[SCANNER] = scanner_dict

        if errorFlag :
            self.__gt_gradients_dict = dict() 
            slog.info('check_dti_gradients.configure','Error: Failed to load all gradients of ground truth cases')
            return False

        return True
        
    def __read_xml_sidecar(self,filepath):
        """
        Read a CMTK xml sidecar file.
        Returns
        =======
        lxml.objectify
        """
        abs_path = os.path.abspath(filepath)
        with open(abs_path, 'rb') as fi:
            lines = fi.readlines()
            lines.insert(1, '<root>')
            lines.append('</root>')
        string = ''.join(lines)
        strip_ge = string.replace('dicom:GE:', '')
        strip_dicom = strip_ge.replace('dicom:', '')
        result = objectify.fromstring(strip_dicom)
        return result


    def __get_array(self,array_string):
        """
        Parse an array from XML string
        
        Returns
        =======
        np.array
        """
        l = array_string.text.split(' ')
        return np.fromiter(l, np.float)


    def __get_gradient_table(self,parsed_sidecar):
        """
        Get the bvector table for a single image
        Returns
        =======
        np.array (rounded to 1 decimal)
        """
        b_vector = self.__get_array(parsed_sidecar.mr.dwi.bVector)
        b_vector_image = self.__get_array(parsed_sidecar.mr.dwi.bVectorImage)
        b_vector_standard = self.__get_array(parsed_sidecar.mr.dwi.bVectorStandard)
        return np.around([b_vector,
                          b_vector_image,
                          b_vector_standard],
                         decimals=self.__decimals)

    #----------------------------------------------------
    def __get_all_gradients(self, session_label, eid, scan_id, dti_stack):
        """
        Parses a list of dti sidecar files for subject.

        Returns
        =======
        list of np.array
        """
        gradients_per_frame = list()
        gradients_as_array = np.asanyarray([])

        error_xml_path_list=[] 
        error_msg=[]

        for xml_path in dti_stack:
            xml_sidecar = self.__read_xml_sidecar(xml_path)
            try:
                gradient_table = self.__get_gradient_table(xml_sidecar)

            except Exception as e:
                error_xml_path_list.append(xml_path)
                error_msg.append(str(e))
                gradient_table = np.zeros((3, 3))

            gradients_per_frame.append(gradient_table)

        gradients_as_array = np.asanyarray(gradients_per_frame)

        if error_xml_path_list != [] :
            slog.info(session_label + "-" + hashlib.sha1(str(error_msg)).hexdigest()[0:6],
                      'ERROR: Could not get gradient table from xml sidecar',
                      script='xnat/check_gradient_tables.py',
                      sidecar=str(xml_sidecar),
                      error_xml_path_list=str(error_xml_path_list),
                      error_msg=str(error_msg),
                      eid = eid,
                      scan = scan_id)
            errorFlag = True 
        else:
            errorFlag = False
 
            return (gradients_as_array, errorFlag)

    #----------------------------------------------------
    def _get_ground_truth_gradients_(self,session_label,scanner,scanner_model,sequence_label):
        scanner_u = scanner.upper()
        if not scanner_u in self.__gt_gradients_dict.iterkeys():
            slog.info(session_label,'ERROR: _get_ground_truth_gradients_: No ground truth defined for ' + scanner_u + '!')
            return []
        
        gt_scanner_map = self.__gt_gradients_dict[scanner_u]
        scanner_model_u = scanner_model.split('_',1)[0].upper()
        if  scanner_model_u in gt_scanner_map.iterkeys():
            gt_model_map =  gt_scanner_map.get(scanner_model_u)
        else : 
            gt_model_map = gt_scanner_map.get('default')

        if not sequence_label in gt_model_map.iterkeys():
            slog.info(session_label,'ERROR: _get_ground_truth_gradients_: No ground truth defined for ' + sequence_label + '!')
            return []

        return gt_model_map[sequence_label]
 
    def get_ground_truth_gradient_path_dict(self):
        """
        Return a dictionary for scanner:gratient
        """
        # Choose arbitrary cases for ground truth
        # Get ground truth for standard baseline
        ground_truth_map  = self.__sibis_defs.get('ground_truth')
        sequence_map  = self.__sibis_defs.get('sequence')

        path_dict = dict()
        for SCANNER in ground_truth_map.iterkeys(): 
            scanner_dict = dict()
            scanner_map = ground_truth_map.get(SCANNER)
            for MODEL in scanner_map.iterkeys():
                model_map = scanner_map.get(MODEL)
                model_event =  model_map.get('event') 
                model_subject =  model_map.get('subject')
                model_subject_dir = os.path.join(self.__cases_dir, model_subject) 
                model_dict = dict()
                for SEQUENCE in sequence_map.iterkeys(): 
                    model_dict[SEQUENCE] = self.get_dti_stack_path(SEQUENCE,model_subject_dir, arm='standard', event=model_event)

                scanner_dict[MODEL] = model_dict

            path_dict[SCANNER] = scanner_dict

        return path_dict
        
    def __load_ground_truth_gradients(self,gt_dti_path):
        dti_stack = glob.glob(gt_dti_path)
        if not len(dti_stack): 
            slog.info("__load_ground_truth_gradients","Error: Cannot find " + gt_dti_path)
            return []

        dti_stack.sort()
        # Parse the xml files to get scanner specific gradients per frame
        (gradients, errorFlag) = self.__get_all_gradients(gt_dti_path, "", "",dti_stack) 
        return np.array(gradients)


    def get_dti_stack_path(self, sequence_label, case, arm=None, event=None):
        if arm:
            path = os.path.join(case, arm)
        else:
            path = os.path.join(case, '*')
        if event:
            path = os.path.join(path, event)
        else:
            path = os.path.join(path, '*')

        return os.path.join(path, 'diffusion/native',sequence_label,'*.xml')

    #----------------------------------------------------
    def get_site_scanner(self,site):
        """
        Returns the scanner for a site 
        """
        return  self.__sibis_defs.get('site_scanner').get(site) 


    def check_diffusion(self,session_label,eid,xml_file_list,manufacturer,scanner_model,scan_id,sequence_label):
        if len(xml_file_list) == 0 : 
            slog.info(session_label,
                      "Error: check_diffusion : xml_file_list is empty ",
                      eid = eid,
                      scan = scan_id)

            return False

        truth_gradient = self._get_ground_truth_gradients_(self.__cases_dir, manufacturer,scanner_model,sequence_label)
        if len(truth_gradient) == 0 :
            slog.info(session_label,
                      'ERROR: check_diffusion: Failed to check ' + sequence_label + " due to missing ground truth!", 
                      manufacturer = manufacturer,
                      model = scanner_model,
                      sequence = sequence_label,
                      eid = eid,
                      scan = scan_id)
            return False

        xml_file_list.sort()

        errorsFrame = list()
        errorsExpected = list()
        errorsActual = list()
        errorsAbsDiff = list()

        errorFlag = False

        try:
            (evaluated_gradients,errorFlag) = self.__get_all_gradients(session_label,eid,scan_id,xml_file_list)

            if len(evaluated_gradients) == len(truth_gradient):
                for idx, frame in enumerate(evaluated_gradients):
                    # if there is a frame that doesn't match,
                    # report it.
                    gtf = truth_gradient[idx]
                    if not ( gtf == frame).all():
                        errorsFrame.append(idx)
                        errorsActual.append(frame)
                        errorsExpected.append(gtf)
                        errorsAbsDiff.append('%.3f' % np.sum(np.sum(np.absolute(gtf - frame)[:])))
                        
            else:
                slog.info(session_label +"-"+ sequence_label,"ERROR: Incorrect number of frames.",
                          number_of_frames=str(len(evaluated_gradients)),
                          expected=str(len(truth_gradient)),
                          sequence = sequence_label,
                          eid = eid,
                          scan = scan_id)
                errorFlag = True

        except AttributeError as error:
            slog.info(session_label, "Error: parsing XML files failed.",
                      xml_file_list=str(xml_file_list),
                      error_msg=str(error),
                      eid = eid,
                      scan = scan_id)
            return False

        if errorsFrame:
            slog.info(session_label,
                      "Errors in gradients of " + sequence_label + " after comparing with ground_truth.",
                      frames=str(errorsFrame),
                      actualGradients=str(errorsActual),
                      expectedGradients=str(errorsExpected),
                      sumError=str(errorsAbsDiff),
                      sequence = sequence_label,
                      eid = eid, scan = scan_id)
            errorFlag = True

        # Check phase encoding
        xml_file = open(xml_file_list[0], 'r')
        try:        
            for line in xml_file:
                match = re.match('.*<>(.+)'
                                 '</phaseEncodeDirectionSign>.*',line)

                # KP: only Siemens scans include the directional sign tag
                if match :
                    sequence_map = self.__sibis_defs.get('sequence')
                    if not sequence_label in sequence_map.iterkeys():
                        slog.info(session_label, 
                                  "Check for sequence " +  sequence_label  + " not defined !", 
                                  eid = eid,                   
                                  scan = scan_id)
                        errorFlag = True
                        break

                    sequence_sign = sequence_map.get(sequence_label)
                    pe_sign = match.group(1).upper()
                    if pe_sign != sequence_sign:
                            slog.info(session_label, 
                                      sequence_label + " has wrong PE sign.",
                                      actual_sign=str(pe_sign),
                                      expect_sign=str(sequence_sign),
                                      eid = eid,
                                      scan = scan_id)
                            errorFlag = True

        # Only siemens scans include the tag 
        #if not matchedFlag : 
        #    slog.info(session_label, 
        #              "tag 'phaseEncodeDirectionSign' missing in dicom hearder",
        #              xml_file = xml_file_list[0],
        #              )
        #    errorFlag = True

 
        except AttributeError as error:
                slog.info(session_label, "Error: parsing XML files failed.",
                          xml_file=xml_file_list[0],
                          error=str(error),
                          eid = eid,
                          scan = scan_id)
                errorFlag = True
        finally:
            xml_file.close()

        return not errorFlag


