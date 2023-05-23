#!/bin/bash
BASE_DIR=/home/hdoop
JAR_DIR=$BASE_DIR/WF
LOCKFILE=$BASE_DIR/LOCK/filelock.lock
### INPUT DIR
DIR_INPUT=${BASE_DIR}/datayunita
INPUT_1=$DIR_INPUT/PAYR_JKT_Pre_20221219235959_00000574_51.1.dat
INPUT_2=$DIR_INPUT/SA_REJECT
INPUT_3=$DIR_INPUT/event_date=2022-12-18 ##sub_dim
INPUT_4=$DIR_INPUT/20221219 ##laccima
INPUT_5=$DIR_INPUT/sa_product
INPUT_6=$DIR_INPUT/dom_mm
INPUT_7=$DIR_INPUT/dom_mtd

### OUTPUT DIR
DIR_OUTPUT=${BASE_DIR}/OUTPUT
OUTPUT_1=$DIR_OUTPUT/HasilSUB_ID ##output part1
OUTPUT_2=$DIR_OUTPUT/HasilPayrGood 
OUTPUT_3=$DIR_OUTPUT/HasilPayrReject

############################## INIT JAR ####################################
PACKAGE=Yunita
CLASS_1=SA_PAYR_Part1
CLASS_2=SA_PAYR_End
CLASS_3=SumaryHH
JAR_NAME=$JAR_DIR/${PACKAGE}_spark2.jar
CLASS_Part1=${PACKAGE}.${CLASS_1}
CLASS_Part2=${PACKAGE}.${CLASS_2}
CLASS_Part3=${PACKAGE}.${CLASS_3}
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
		${SPARK_COMMAND} --class ${CLASS_Part1} --master local[1] ${JAR_NAME} \
		file://${INPUT_1} file://${INPUT_2} file://${INPUT_3} file://${OUTPUT_1}
	echo "End : "`date +%Y-%m-%d\ %H:%M:%S` >> $HGRID_LOG
	mv_log1=`ls -l ${CLASS_Part1}*.log |awk '{print $NF}' |tail -1`
    cat ${mv_log1} |awk -F"|" '{print $(NF-1)}' |awk '{gsub(";","\n")}1' >> ${HGRID_LOG}
	
	### Part2
	echo "Start : "`date +%Y-%m-%d\ %H:%M:%S` >> $HGRID_LOG
		${SPARK_COMMAND} --class ${CLASS_Part2} --master local[1] ${JAR_NAME} \
		file://${INPUT_4} file://${INPUT_5} file://${INPUT_6} file://${INPUT_7} file://${OUTPUT_1} file://${OUTPUT_2} file://${OUTPUT_3}
	echo "End : "`date +%Y-%m-%d\ %H:%M:%S` >> $HGRID_LOG
	mv_log2=`ls -l ${CLASS_Part2}*.log |awk '{print $NF}' |tail -1`
    cat ${mv_log2} |awk -F"|" '{print $(NF-1)}' |awk '{gsub(";","\n")}1' >> ${HGRID_LOG}

	echo "Proses Selesai pada:" `date`
###### REMOVE FORMAT HSPARK
	rm $OUTPUT_1
	rm $LOCKFILE 
	rm $mv_log1
	rm $mv_log2
fi