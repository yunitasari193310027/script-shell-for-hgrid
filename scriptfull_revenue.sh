#!/bin/bash
BASE_DIR=/home/hdoop
LOCKFILE=$BASE_DIR/LOCK/filelock.lock
### INPUT DIR
DIR_INPUT=${BASE_DIR}/datayunita/RevenueSA
INPUT_1=$DIR_INPUT/HLR_PRE_DIM
INPUT_2=$DIR_INPUT/MERGE_REVENUE_SA

### OUTPUT DIR
DIR_OUTPUT=${BASE_DIR}/OUTPUT
OUTPUT_1=$DIR_OUTPUT/RevenueSA_HLR 
OUTPUT_2=$DIR_OUTPUT/RevenueSA_VLR
############################## INIT JAR ####################################
JAR_DIR=$BASE_DIR/WF
PACKAGE=RevenueSA
CLASS=HLR_VLR
JAR_NAME=$JAR_DIR/${PACKAGE}_spark2.jar
PACKAGE_CLASS=${PACKAGE}.${CLASS}
HGRID_LOG=$BASE_DIR/LOG/${PACKAGE}.log
#Chek path jar
	CK_JAR=`ls $JAR_DIR`
	if [ -z ${CK_JAR} ]; then
		echo "jar not found.."
	fi

	if [ -d $DIR_OUTPUT ];then
		echo "Remove output.."
		rm -r $DIR_OUTPUT
	fi

### LOCKING PROCESS
if [ -e $LOCKFILE ];then
	echo "Proses masih jalan"
	exit ####aplikasi keluar
else
	touch $LOCKFILE ##genrate filelock.lock

SPARK_COMMAND=/opt/spark/bin/spark-submit
############################## RUN SPARK ####################################
	### Part1
	echo "Start : "`date +%Y-%m-%d\ %H:%M:%S` > $HGRID_LOG
		${SPARK_COMMAND} --class ${PACKAGE_CLASS} --master local[1] ${JAR_NAME} \
		file://${INPUT_1} file://${INPUT_2} file://${OUTPUT_1} file://${OUTPUT_2}
	echo "End : "`date +%Y-%m-%d\ %H:%M:%S` >> $HGRID_LOG
	mv_log=`ls -l ${PACKAGE_CLASS}*.log |awk '{print $NF}' |tail -1`
    cat ${mv_log} |awk -F"|" '{print $(NF-1)}' |awk '{gsub(";","\n")}1' >> ${HGRID_LOG}
	
	echo "Proses Selesai pada:" `date`

############################## LOAD OUTPUT TO DATABASE ####################################
	
	cat $OUTPUT_1/h* > OUTPUT_1.csv
mysql -uyunita -p1q2q3q4q -Dfull <<EOF
	CREATE TABLE IF NOT EXISTS Revenue_SA_HLR(trx_year varchar(100),trx_month varchar(100),trx_date varchar(100),brand varchar(100),l3_cd varchar(100),area_hlr varchar(100),region_hlr varchar(100),city_hlr varchar(100),l1_name varchar(100),l2_name varchar(100),l3_name varchar(100),tot_user varchar(100),tot_dur varchar(100),tot_rev varchar(100),tot_trx varchar(100),event_date varchar(100),source_name varchar(100));
	load data local infile '/home/hdoop/OUTPUT_1.csv' into table Revenue_SA_HLR fields 
    terminated by '|' 
    enclosed by '"' 
    lines terminated by '\n' 
    (trx_year,trx_month,trx_date,brand,l3_cd,area_hlr,region_hlr,city_hlr,l1_name,l2_name,l3_name,tot_user,tot_dur,tot_rev,tot_trx,event_date,source_name);
EOF
	
	cat $OUTPUT_2/h* > OUTPUT_2.csv
mysql -uyunita -p1q2q3q4q -Dfull <<EOF
	CREATE TABLE IF NOT EXISTS Revenue_SA_VLR(file_date varchar(100),area varchar(100),region varchar(100),cluster varchar(100),brand varchar(100),l1_name varchar(100),l2_name varchar(100),l3_name varchar(100),offer_name varchar(100),tot_rev varchar(100),tot_trx varchar(100),tot_dur varchar(100),node varchar(100),cust_type varchar(100),cust_subtype varchar(100),cust_subsegment varchar(100),event_date varchar(100),source_name varchar(100));
	load data local infile '/home/hdoop/OUTPUT_2.csv' into table Revenue_SA_VLR fields 
    terminated by '|' 
    enclosed by '"' 
    lines terminated by '\n' 
    (file_date,area,region,cluster,brand,l1_name,l2_name,l3_name,offer_name,tot_rev,tot_trx,tot_dur,node,cust_type,cust_subtype,cust_subsegment,event_date,source_name);
EOF
###### REMOVE FOLDER/FILE 
	rm -r $OUTPUT_1
	rm $LOCKFILE 
	rm $mv_log
	rm OUTPUT_1.csv
	rm OUTPUT_2.csv	
fi
