#!/bin/bash
BASE_DIR=/home/hdoop
JAR_DIR=$BASE_DIR/WF
LOCKFILE=$BASE_DIR/LOCK/filelock.lock
### INPUT DIR
DIR_INPUT=${BASE_DIR}/datayunita/USAGECHG
INPUT_1=$DIR_INPUT/prepMTD
INPUT_2=$DIR_INPUT/prepMM
INPUT_3=$DIR_INPUT/LACCIMA_DIM.csv 
INPUT_4=$DIR_INPUT/SUB_DIM 
INPUT_5=$DIR_INPUT/SA_PRODLINE_BONUS
INPUT_6=$DIR_INPUT/RCG
INPUT_7=$DIR_INPUT/TC_RCG
INPUT_8=$BASE_DIR/datayunita/SAUsageOCS/HH ##for HH

### OUTPUT DIR
DIR_OUTPUT=${BASE_DIR}/OUTPUT
OUTPUT_1=$DIR_OUTPUT/USAGE_SA_RCG_REJECT 
OUTPUT_2=$DIR_OUTPUT/USAGE_SA_RCG
OUTPUT_3=$DIR_OUTPUT/USAGE_SA_RCG_SUM
OUTPUT_4=$DIR_OUTPUT/MERGE_SA_RCG_GOOD
OUTPUT_5=$DIR_OUTPUT/MERGE_SA_RCG_REJECT 
OUTPUT_6=$DIR_OUTPUT/USAGE_SA_RCG_BAD
OUTPUT_7=$DIR_OUTPUT/USAGE_SA_RCG_DUP01
OUTPUT_8=$DIR_OUTPUT/USAGE_SA_RCG_DUP03
OUTPUT_9=$DIR_OUTPUT/USAGE_SA_RCG_DUP02
OUTPUT_10=$DIR_OUTPUT/USAGE_SA_RCG_HH

############################## INIT JAR ####################################
PACKAGE=SAUsageOCS
CLASS_1=Final
CLASS_2=HH
JAR_NAME=$JAR_DIR/${PACKAGE}_spark2.jar
CLASS_Part1=${PACKAGE}.${CLASS_1}
CLASS_Part2=${PACKAGE}.${CLASS_2}
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
		file://${INPUT_1} file://${INPUT_2} file://${INPUT_3} file://${INPUT_4} file://${INPUT_5} file://${INPUT_6} file://${INPUT_7} \
		file://${OUTPUT_1} file://${OUTPUT_2} file://${OUTPUT_3} file://${OUTPUT_4} file://${OUTPUT_5} file://${OUTPUT_6} file://${OUTPUT_7} file://${OUTPUT_8} file://${OUTPUT_9}
	echo "End : "`date +%Y-%m-%d\ %H:%M:%S` >> $HGRID_LOG
	mv_log1=`ls -l ${CLASS_Part1}*.log |awk '{print $NF}' |tail -1`
    cat ${mv_log1} |awk -F"|" '{print $(NF-1)}' |awk '{gsub(";","\n")}1' >> ${HGRID_LOG}

	### PartHH
	echo "Start : "`date +%Y-%m-%d\ %H:%M:%S` >> $HGRID_LOG
		${SPARK_COMMAND} --class ${CLASS_Part2} --master local[1] ${JAR_NAME} \
		file://${OUTPUT_8} file://${OUTPUT_10}
	echo "End : "`date +%Y-%m-%d\ %H:%M:%S` >> $HGRID_LOG
	mv_log2=`ls -l ${CLASS_Part2}*.log |awk '{print $NF}' |tail -1`
    cat ${mv_log2} |awk -F"|" '{print $(NF-1)}' |awk '{gsub(";","\n")}1' >> ${HGRID_LOG}
	echo "Proses Selesai pada:" `date`
###### REMOVE FORMAT HSPARK
	rm $LOCKFILE 
	rm $mv_log1
	rm $mv_log2
fi
