#!/bin/bash
BASE_DIR=/home/hdoop
JAR_DIR=$BASE_DIR/WF
LOCKFILE=$BASE_DIR/LOCK/filelock.lock
### INPUT DIR
DIR_INPUT=${BASE_DIR}/datayunita/USAGECHG
INPUT_1=$DIR_INPUT/CHG
INPUT_2=$DIR_INPUT/SUB_DIM
INPUT_3=$DIR_INPUT/STG 
INPUT_4=$DIR_INPUT/prepMTD 
INPUT_5=$DIR_INPUT/prepMM
INPUT_6=$DIR_INPUT/LACCIMA_DIM_TERBARU.csv
INPUT_7=$DIR_INPUT/SA_Product
INPUT_8=$DIR_INPUT/SA_PRODLINE_BONUS

### OUTPUT DIR
DIR_OUTPUT=${BASE_DIR}/OUTPUT
OUTPUT_1=$DIR_OUTPUT/CHGbad 
OUTPUT_2=$DIR_OUTPUT/CHGpart1 ##for input part2
OUTPUT_3=$DIR_OUTPUT/SA_USAGE_CHG_Sum
OUTPUT_4=$DIR_OUTPUT/SA_USAGE_CHG_Dup_02
OUTPUT_5=$DIR_OUTPUT/SA_USAGE_CHG_REJECT 
OUTPUT_6=$DIR_OUTPUT/SA_USAGE_CHG
OUTPUT_7=$DIR_OUTPUT/HasilCHGUSAGE_HH

############################## INIT JAR ####################################
PACKAGE=SAUsageChg
CLASS_1=CHGReguler_Part1
CLASS_2=CHGReguler_Part2
CLASS_3=HH
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
		file://${INPUT_1} file://${INPUT_2} file://${INPUT_3} file://${INPUT_4} file://${INPUT_5} file://${INPUT_6} file://${OUTPUT_1} file://${OUTPUT_2}
	echo "End : "`date +%Y-%m-%d\ %H:%M:%S` >> $HGRID_LOG
	mv_log1=`ls -l ${CLASS_Part1}*.log |awk '{print $NF}' |tail -1`
    cat ${mv_log1} |awk -F"|" '{print $(NF-1)}' |awk '{gsub(";","\n")}1' >> ${HGRID_LOG}
	
	### Part2
	echo "Start : "`date +%Y-%m-%d\ %H:%M:%S` >> $HGRID_LOG
		${SPARK_COMMAND} --class ${CLASS_Part2} --master local[1] ${JAR_NAME} \
		file://${INPUT_7} file://${OUTPUT_2} file://${INPUT_8} file://${OUTPUT_3} file://${OUTPUT_4} file://${OUTPUT_5} file://${OUTPUT_6}
	echo "End : "`date +%Y-%m-%d\ %H:%M:%S` >> $HGRID_LOG
	mv_log2=`ls -l ${CLASS_Part2}*.log |awk '{print $NF}' |tail -1`
    cat ${mv_log2} |awk -F"|" '{print $(NF-1)}' |awk '{gsub(";","\n")}1' >> ${HGRID_LOG}

	### PartHH
	echo "Start : "`date +%Y-%m-%d\ %H:%M:%S` >> $HGRID_LOG
		${SPARK_COMMAND} --class ${CLASS_Part3} --master local[1] ${JAR_NAME} \
		file://${OUTPUT_6} file://${OUTPUT_7}
	echo "End : "`date +%Y-%m-%d\ %H:%M:%S` >> $HGRID_LOG
	mv_log3=`ls -l ${CLASS_Part3}*.log |awk '{print $NF}' |tail -1`
    cat ${mv_log3} |awk -F"|" '{print $(NF-1)}' |awk '{gsub(";","\n")}1' >> ${HGRID_LOG}
	echo "Proses Selesai pada:" `date`
###### REMOVE FORMAT HSPARK
	rm -r $OUTPUT_2 ##remove output part1
	rm $LOCKFILE 
	rm $mv_log1
	rm $mv_log2	
	rm $mv_log3
fi
