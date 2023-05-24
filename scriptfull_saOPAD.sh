#!/bin/bash
BASE_DIR=/home/hdoop
LOCKFILE=$BASE_DIR/LOCK/filelock.lock
### INPUT DIR
DIR_INPUT=${BASE_DIR}/datayunita/SA_OPAD
INPUT_1=$DIR_INPUT/OPAD_JKT_Pre_20221220235959_00000534_51.1.dat
INPUT_2=$DIR_INPUT/LINEDIM
INPUT_3=$DIR_INPUT/IFRS

### OUTPUT DIR
DIR_OUTPUT=${BASE_DIR}/OUTPUT
OUTPUT_1=$DIR_OUTPUT/SA_OPAD 
OUTPUT_2=$DIR_OUTPUT/SA_OPADReject
############################## INIT JAR ####################################
JAR_DIR=$BASE_DIR/WF
PACKAGE=SAOPAD
CLASS=Part1
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
		file://${INPUT_1} file://${INPUT_2} file://${INPUT_3} file://${OUTPUT_1} file://${OUTPUT_2}
	echo "End : "`date +%Y-%m-%d\ %H:%M:%S` >> $HGRID_LOG
	mv_log=`ls -l ${PACKAGE_CLASS}*.log |awk '{print $NF}' |tail -1`
    cat ${mv_log} |awk -F"|" '{print $(NF-1)}' |awk '{gsub(";","\n")}1' >> ${HGRID_LOG}
	
	echo "Proses Selesai pada:" `date`

############################## LOAD OUTPUT TO DATABASE ####################################
cat $OUTPUT_1/h* > OUTPUT_1.csv

mysql -uhdoop -p1q2q3q4q -Dfull <<EOF
	CREATE TABLE IF NOT EXISTS SA_OPAD(timestamp_r varchar(100),trx_date varchar(100),trx_hour varchar(100),user_name varchar(100),msisdn varchar(100),order_id varchar(100),main_plan_name varchar(100),main_plan_id varchar(100),topping_plan_name varchar(100),topping_plan_id varchar(100),price varchar(100),brand varchar(100),file_id varchar(100),load_ts varchar(100),load_user varchar(100),offer_id varchar(100),pre_post_flag varchar(100),l1_payu varchar(100),l2_service_type varchar(100),l3_allowance_type varchar(100),l4_product_category varchar(100),l5_product varchar(100),l1_name varchar(100),l2_name varchar(100),l3_name varchar(100),l4_name varchar(100),payment_channel varchar(100));
	 load data local infile '/home/hdoop/HasilSA_OPAD.csv' into table SA_OPAD fields 
	 terminated by '~' 
	 enclosed by '"' 
	 lines terminated by '\n' 
	 (timestamp_r,trx_date,trx_hour,user_name,msisdn,order_id,main_plan_name,main_plan_id,topping_plan_name,topping_plan_id,price,brand,file_id,load_ts,load_user,offer_id,pre_post_flag,l1_payu,l2_service_type,l3_allowance_type,l4_product_category,l5_product,l1_name,l2_name,l3_name,l4_name,payment_channel);

EOF

	cat $OUTPUT_2/h* > OUTPUT_2.csv

mysql -uhdoop -p1q2q3q4q -Dfull <<EOF
	CREATE TABLE IF NOT EXISTS SA_OPAD_Reject(timestamp_r varchar(100),trx_date varchar(100),trx_hour varchar(100),user_name varchar(100),msisdn varchar(100),order_id varchar(100),main_plan_name varchar(100),main_plan_id varchar(100),topping_plan_name varchar(100),topping_plan_id varchar(100),price varchar(100),brand varchar(100),file_id varchar(100),load_ts varchar(100),load_user varchar(100),offer_id varchar(100),pre_post_flag varchar(100),l1_payu varchar(100),l2_service_type varchar(100),l3_allowance_type varchar(100),l4_product_category varchar(100),l5_product varchar(100),l1_name varchar(100),l2_name varchar(100),l3_name varchar(100),l4_name varchar(100),payment_channel varchar(100));
	 load data local infile '/home/hdoop/HasilSA_OPADReject.csv' into table SA_OPAD_Reject fields 
	 terminated by '~' 
	 enclosed by '"' 
	 lines terminated by '\n' 
	 (timestamp_r,trx_date,trx_hour,user_name,msisdn,order_id,main_plan_name,main_plan_id,topping_plan_name,topping_plan_id,price,brand,file_id,load_ts,load_user,offer_id,pre_post_flag,l1_payu,l2_service_type,l3_allowance_type,l4_product_category,l5_product,l1_name,l2_name,l3_name,l4_name,payment_channel);

EOF
###### REMOVE FOLDER/FILE 
	rm -r $OUTPUT_1
	rm $LOCKFILE 
	rm $mv_log
	rm OUTPUT_1.csv
	rm OUTPUT_2.csv
fi
