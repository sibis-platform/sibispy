#!/bin/bash
set -euo pipefail

# Helper function to check whether element exists in array
containsElement () {
  local e match="$1"
  shift
  for e; do [[ "$e" == "$match" ]] && return 0; done
  return 1
}

makeImage03 () {
    in_csv_file=$1
    JSON=$2 
    out_csv_file=$3

    if [ ! -e ${in_csv_file} ] ; then 
	echo "ERROR:$in_csv_file missing !"
	return 
    fi 


    head -n2 ${in_csv_file} > ${out_csv_file}
    csv_data=`tail -n1 ${in_csv_file}`

    new_csv_data=`echo $csv_data | cut -d, -f1-78`",\"${JSON}\","`echo $csv_data | cut -d, -f80-`
    echo $new_csv_data >> ${out_csv_file}
} 


# arguments 
# 1: tmp directory of csv file , e.g. /tmp/ndarupload/LAB_S01669_20220517_6909_05172022
# 2: cases dir (optional)
# 3: upload dir (optional)
upload_to_ndar() {
    ndar_csv_temp_dir=$1 
    SUBJECT_VISIT=`echo $ndar_csv_temp_dir | rev | cut -d'/' -f1 | rev`
    subject_dir=`echo $SUBJECT_VISIT | cut -d'_' -f1-2`
    arm_dir=standard
    visit_dir=`echo $SUBJECT_VISIT | cut -d'_' -f3-`
    year_month=${visit_dir:0:6}

    cases_dir=${2:-/fs/cases}
    ndar_dir=${3:-/fs/upload2ndar}

    complete_dir=$cases_dir/$subject_dir/$arm_dir/$visit_dir  
    if [ ! -e $complete_dir/redcap ]; then 
        echo "ERROR:$ndar_csv_temp_dir is incorrectly defined as $complete_dir/redcap does not exist!"
        exit 1
    fi 
    
    #
    # Move ndar_subject01.csv
    #
    ndar_csv_file="$ndar_csv_temp_dir"/ndar_subject01.csv
    if [ ! -e "$ndar_csv_file" ]; then
	echo "ERROR:$ndar_csv_temp_dir does not contain ndar_subject01.csv !"
	exit 1
    fi 

    GUID=`tail -n1 $ndar_csv_file | cut -d, -f1 | tr -d \"` 
     if [ "$GUID" != "" ]; then 
	echo "INFO:GUID is $GUID"
    else 
	GUID="NDARXXXXXXXX"
	echo "INFO:$ndar_csv_temp_dir:GUID is not defined - setting it to $GUID!"
    fi

    subject_and_visit="$subject_dir"_"$visit_dir"
 
    #
    # mci_cb 
    #
    BASE_DIR=${ndar_dir}/mci_cb/${subject_and_visit}
    echo "INFO:Create $BASE_DIR"
    mkdir -p $BASE_DIR
    cp $ndar_csv_file $BASE_DIR

    #
    # finalize image_03.csv and structural files
    #
    structural_dir=${complete_dir}/structural/native
    if [ -d "${structural_dir}" ]; then
	for TYPE in t1 t2; do
            FILE_NAME=${TYPE}.nii.gz 	
 	    if [ ! -e ${structural_dir}/$FILE_NAME ]; then 
		echo "ERROR:${structural_dir}/$FILE_NAME missing - removing all structural imaging files!" 
		[ -e ${BASE_DIR}/t1 ] && rm -rf ${BASE_DIR}/t1
		exit 1
	    fi 

	    TYPE_DIR=${BASE_DIR}/$TYPE
	    mkdir -p ${TYPE_DIR} 

	    JSON=${GUID}-${year_month}-structural-${TYPE}.json

	    # mv image03.csv
	    makeImage03 ${ndar_csv_temp_dir}/$TYPE/image03.csv $JSON ${TYPE_DIR}/image03.csv
	    if [ ! -e ${TYPE_DIR}/image03.csv ]; then 
		echo "ERROR:${TYPE_DIR}/image03.csv was not created - removing all structural imaging files!" 
		rm -rf ${BASE_DIR}/t1
		[ -e ${BASE_DIR}/t2 ] && rm -rf ${BASE_DIR}/t1
		exit 1
	    fi 

	    
	    # copy nii.gz 
	    REL_IMG_DIR=${GUID}/${year_month}/structural
	    IMG_DIR=${TYPE_DIR}/${REL_IMG_DIR}
            mkdir -p $IMG_DIR
	    echo "INFO:Copying ${structural_dir}/$FILE_NAME"  
            rsync -m -r -og --copy-links --include="*/" --include="${FILE_NAME}" --exclude="*" ${structural_dir}/ $IMG_DIR

	    FILE=${IMG_DIR}/${FILE_NAME}
            FILESIZE=`wc -c $FILE | awk '{print $1}'`
            md5=($(md5sum $FILE))

            FILEPATH=${REL_IMG_DIR}/${FILE_NAME}
            DATA="{\"files\": [{\"path\": \"$FILEPATH\", \"name\": \"$FILE_NAME\", \"size\": \"$FILESIZE\", \"md5sum\": \"$md5\"}]}"
            echo $DATA | tee $TYPE_DIR/${JSON}
        done
    fi

    #
    # cns_deficit - still need to implement b-values and b-vectors 
    #
    BASE_DIR=${ndar_dir}/cns_deficit/${subject_and_visit}
    echo "INFO:Create $BASE_DIR"
    mkdir -p $BASE_DIR
    cp $ndar_csv_file $BASE_DIR

    #
    # finalize image_03.csv and structural files
    #
    diffusion_dir=${complete_dir}/diffusion/native
    if [ -d "${diffusion_dir}" ]; then
	for TYPE in dti6b500pepolar dti30b400 dti60b1000; do
	    if [ ! -e $diffusion_dir/$TYPE ]; then 
		echo "Warning:$diffusion_dir is missing $TYPE!"
		continue 
	    fi 

	    TYPE_DIR=${BASE_DIR}/$TYPE
	    mkdir -p ${TYPE_DIR} 

	    JSON=${GUID}-${year_month}-diffusion-${TYPE}.json

	    # mv image03.csv
	    makeImage03 ${ndar_csv_temp_dir}/$TYPE/image03.csv $JSON ${TYPE_DIR}/image03.csv
	    
	    # copy nii.gz 
	    REL_IMG_DIR=${GUID}/${year_month}/diffusion/$TYPE
	    IMG_DIR=${TYPE_DIR}/${REL_IMG_DIR}
            mkdir -p $IMG_DIR
	    rsync -v -m -r -og --copy-links --include="*nii.gz" --exclude="*" $diffusion_dir/$TYPE/ $IMG_DIR

	    echo "{\"files\": [" > $TYPE_DIR/${JSON}
	    for FILE in $IMG_DIR/*.nii.gz; do
		FILE_NAME=`echo $FILE | rev | cut -d'/' -f1 | rev` 
		FILESIZE=`wc -c $FILE | awk '{print $1}'`
		md5=($(md5sum $FILE))
		FILEPATH=${REL_IMG_DIR}/${FILE_NAME}
		echo "{\"path\": \"$FILEPATH\", \"name\": \"$FILE_NAME\", \"size\": \"$FILESIZE\", \"md5sum\": \"$md5\"}," >> $TYPE_DIR/${JSON}
	    done

	    # Bvalue and bvec 
	    JSON_LINE=""
	    for FILE in bval bvec; do
		if [ "$JSON_LINE" != "" ]; then 
		    echo "${JSON_LINE}," >> $TYPE_DIR/${JSON}
		fi
    
		absFILE=${ndar_csv_temp_dir}/$TYPE/$FILE 
		if [ ! -e $absFILE ]; then 
		    echo "ERROR:$absFILE missing!"
		    exit 1
		fi
		cp $absFILE $IMG_DIR/$FILE

		FILESIZE=`wc -c $IMG_DIR/$FILE | awk '{print $1}'`
		md5=($(md5sum $IMG_DIR/$FILE))
		FILEPATH=${REL_IMG_DIR}/${FILE}
		JSON_LINE="{\"path\": \"$FILEPATH\", \"name\": \"$FILE\", \"size\": \"$FILESIZE\", \"md5sum\": \"$md5\"}"
	    done
            echo "${JSON_LINE}]}" >> $TYPE_DIR/${JSON}
        done
    fi
}


ncanda_upload_to_ndar() {
    ndar_csv_temp_dir=$1 

    SUBJECT_ID=`echo $ndar_csv_temp_dir | rev | cut -d'/' -f2 | rev`   
    followup_yr=`echo $ndar_csv_temp_dir | rev | cut -d'/' -f1 | rev`
    snaps_yr=`echo $followup_yr | rev | cut -c 2`
    snaps_dir_base=NCANDA_SNAPS_${snaps_yr}Y_
    arm_dir=standard

    ncanda_internal_base=/fs/neurosci01/ncanda/releases/internal
    cases_dir=${2:-${ncanda_internal_base}/${followup_yr}}
    ndar_dir=${3:-/fs/neurosci01/ncanda/releases/internal/upload2ndar}

    #
    # Move ndar_subject01.csv
    #
    ndar_csv_file="$ndar_csv_temp_dir"/ndar_subject01.csv
    if [ ! -e "$ndar_csv_file" ]; then
	echo "ERROR:$ndar_csv_temp_dir does not contain ndar_subject01.csv !"
	exit 1
    fi 

    GUID=`tail -n1 $ndar_csv_file | cut -d, -f1 | tr -d \"` 
     if [ "$GUID" != "" ]; then 
	echo "INFO:GUID is $GUID"
    else 
	GUID="NDARXXXXXXXX"
	echo "INFO:$ndar_csv_temp_dir:GUID is not defined - setting it to $GUID!"
    fi
    
    BASE_DIR=${ndar_dir}/${SUBJECT_ID}/${followup_yr}
    echo "INFO:Create $BASE_DIR"
    mkdir -p $BASE_DIR
    cp $ndar_csv_file $BASE_DIR

    #
    # t1/t2
    #
    structural_dirs=`ls ${cases_dir} | grep STRUCTURAL`
    # get latest structural dir release
    structural_dir_name=`echo ${structural_dirs} | rev | cut -d ' ' -f1 | rev`
    structural_dir=${cases_dir}/${structural_dir_name}/cases/${SUBJECT_ID}/${arm_dir}/${followup_yr}/structural/native

    if [ -d "${structural_dir}" ]; then
	for TYPE in t1 t2; do
            FILE_NAME=${TYPE}.nii.gz 	
 	    if [ ! -e ${structural_dir}/$FILE_NAME ]; then 
		echo "ERROR:${structural_dir}/$FILE_NAME missing - removing all structural imaging files!" 
		[ -e ${BASE_DIR}/t1 ] && rm -rf ${BASE_DIR}/t1
		exit 1
	    fi 

	    TYPE_DIR=${BASE_DIR}/$TYPE
	    mkdir -p ${TYPE_DIR} 

	    JSON=${GUID}-${followup_yr}-structural-${TYPE}.json

	    # mv image03.csv
	    makeImage03 ${ndar_csv_temp_dir}/$TYPE/image03.csv $JSON ${TYPE_DIR}/image03.csv
	    if [ ! -e ${TYPE_DIR}/image03.csv ]; then 
		echo "ERROR:${TYPE_DIR}/image03.csv was not created - removing all structural imaging files!" 
		rm -rf ${BASE_DIR}/t1
		[ -e ${BASE_DIR}/t2 ] && rm -rf ${BASE_DIR}/t1
		exit 1
	    fi 

	    
	    # copy nii.gz 
	    REL_IMG_DIR=${GUID}/${followup_yr}/structural
	    IMG_DIR=${TYPE_DIR}/${REL_IMG_DIR}
            mkdir -p $IMG_DIR
	    echo "INFO:Copying ${structural_dir}/$FILE_NAME"  
            rsync -m -r -og --copy-links --include="*/" --include="${FILE_NAME}" --exclude="*" ${structural_dir}/ $IMG_DIR

	    FILE=${IMG_DIR}/${FILE_NAME}
            FILESIZE=`wc -c $FILE | awk '{print $1}'`
            md5=($(md5sum $FILE))

            FILEPATH=${REL_IMG_DIR}/${FILE_NAME}
            DATA="{\"files\": [{\"path\": \"$FILEPATH\", \"name\": \"$FILE_NAME\", \"size\": \"$FILESIZE\", \"md5sum\": \"$md5\"}]}"
            echo $DATA | tee $TYPE_DIR/${JSON}
        done
    else
        echo "SKIPPING t1 t2 creation, could not find ${structural_dir}"
    fi

    #
    # DTI
    #
    diffusion_dirs=`ls ${cases_dir} | grep DIFFUSION`
    # get latest diffusion dir release
    diffusion_dir_name=`echo ${diffusion_dirs} | rev | cut -d ' ' -f1 | rev`
    diffusion_dir=${cases_dir}/${diffusion_dir_name}/cases/${SUBJECT_ID}/${arm_dir}/${followup_yr}/diffusion/native
    
    if [ -d "${diffusion_dir}" ]; then
	for TYPE in dti6b500pepolar dti30b400 dti60b1000; do
	    if [ ! -e $diffusion_dir/$TYPE ]; then 
		echo "Warning:$diffusion_dir is missing $TYPE!"
		continue 
	    fi 

	    TYPE_DIR=${BASE_DIR}/$TYPE
	    mkdir -p ${TYPE_DIR} 

	    JSON=${GUID}-${followup_yr}-diffusion-${TYPE}.json

	    # mv image03.csv
	    makeImage03 ${ndar_csv_temp_dir}/$TYPE/image03.csv $JSON ${TYPE_DIR}/image03.csv
	    
	    # copy nii.gz 
	    REL_IMG_DIR=${GUID}/${followup_yr}/diffusion/$TYPE
	    IMG_DIR=${TYPE_DIR}/${REL_IMG_DIR}
            mkdir -p $IMG_DIR
	    rsync -v -m -r -og --copy-links --include="*nii.gz" --exclude="*" $diffusion_dir/$TYPE/ $IMG_DIR

	    echo "{\"files\": [" > $TYPE_DIR/${JSON}
	    for FILE in $IMG_DIR/*.nii.gz; do
		FILE_NAME=`echo $FILE | rev | cut -d'/' -f1 | rev` 
		FILESIZE=`wc -c $FILE | awk '{print $1}'`
		md5=($(md5sum $FILE))
		FILEPATH=${REL_IMG_DIR}/${FILE_NAME}
		echo "{\"path\": \"$FILEPATH\", \"name\": \"$FILE_NAME\", \"size\": \"$FILESIZE\", \"md5sum\": \"$md5\"}," >> $TYPE_DIR/${JSON}
	    done

	    # Bvalue and bvec 
	    JSON_LINE=""
	    for FILE in bval bvec; do
		if [ "$JSON_LINE" != "" ]; then 
		    echo "${JSON_LINE}," >> $TYPE_DIR/${JSON}
		fi
    
		absFILE=${ndar_csv_temp_dir}/$TYPE/$FILE 
		if [ ! -e $absFILE ]; then 
		    echo "ERROR:$absFILE missing!"
		    exit 1
		fi
		cp $absFILE $IMG_DIR/$FILE

		FILESIZE=`wc -c $IMG_DIR/$FILE | awk '{print $1}'`
		md5=($(md5sum $IMG_DIR/$FILE))
		FILEPATH=${REL_IMG_DIR}/${FILE}
		JSON_LINE="{\"path\": \"$FILEPATH\", \"name\": \"$FILE\", \"size\": \"$FILESIZE\", \"md5sum\": \"$md5\"}"
	    done
            echo "${JSON_LINE}]}" >> $TYPE_DIR/${JSON}
        done
    else
        echo "SKIPPING dti creation, could not find ${diffusion_dir}"
    fi
}

"$@"
