#!/bin/bash

RUNDATE=`date`
JOBID=Bebas

### LOCKING PROCESS

if [ -e /home/hdoop/filelock.lock ];then
	echo "Proses masih jalan"
	exit ####aplikasi keluar
else
	touch /home/hdoop/filelock.lock ##genrate filelock.lock

	PACKAGE=Yunita
	CLASS=NGRS_Reject
	export NAMAJAR=/home/hdoop/Yunita_spark2.jar

	if [ -d /home/hdoop/HasilNGRS ];then
		echo "Folder masih ada, hapus dulu ya"
		rm -r HasilNGRS
	fi

	spark-submit --class ${PACKAGE}.${CLASS} --master yarn \
	${NAMAJAR} \
	hdfs:///user/hdoop/datayunita/dim.csv \
	hdfs:///user/hdoop/datayunita/HLR \
	hdfs:///user/hdoop/HVC2/hgrid247-00001 \
	hdfs:///user/hdoop/datayunita/reject_ngrs \
	file:///home/hdoop/HasilNGRS

	echo "Proses Selesai pada:" `date`

	LOGFILE=`ls -lrt ${PACKAGE}.${CLASS}*log | tail -1 | awk '{print $9}'`
	NAMATRANS1=TRA_TRA_READ_REJECT
 	INPUTTRA1=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $1}' | awk -F":" '{print $2}'`
  	OUTPUTTRA1=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $1}' | awk -F":" '{print $3}'`

	NAMATRANS2=TRA_TRA_READ_REJECT_2
	INPUTTRA2=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $2}' | awk -F":" '{print $2}'`
	OUTPUTTRA2=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $2}' | awk -F":" '{print $3}'`

	NAMATRANS3=TRA_TRA_Read_DIM
	INPUTTRA3=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $3}' | awk -F":" '{print $2}'`
	OUTPUTTRA3=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $3}' | awk -F":" '{print $3}'`

	NAMATRANS4=RJO_RefJoin_1
    INPUTTRA4=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $4}' | awk -F":" '{print $2}'`
    OUTPUTTRA4=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $4}' | awk -F":" '{print $3}'`

	NAMATRANS5=TRA_Join_SERVICECODE
    INPUTTRA5=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $5}' | awk -F":" '{print $2}'`
    OUTPUTTRA5=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $5}' | awk -F":" '{print $3}'`

	NAMATRANS6=TRA_TRA_READ_HVC
	INPUTTRA6=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $6}' | awk -F":" '{print $2}'`
	OUTPUTTRA6=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $6}' | awk -F":" '{print $3}'`

	NAMATRANS7=JOI_Join_1
	INPUTTRA7=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $7}' | awk -F":" '{print $2}'`
	OUTPUTTRA7=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $7}' | awk -F":" '{print $3}'`

	NAMATRANS8=TRA_Join_HVC
	INPUTTRA8=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $8}' | awk -F":" '{print $2}'`
	OUTPUTTRA8=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $8}' | awk -F":" '{print $3}'`

	NAMATRANS9=TRA_TRA_READ_HLR
	INPUTTRA9=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $9}' | awk -F":" '{print $2}'`
	OUTPUTTRA9=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $9}' | awk -F":" '{print $3}'`

	NAMATRANS10=JOI_Join_2
	INPUTTRA10=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $10}' | awk -F":" '{print $2}'`
	OUTPUTTRA10=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $10}' | awk -F":" '{print $3}'`

	NAMATRANS11=TRA_Join_HLR
	INPUTTRA11=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $11}' | awk -F":" '{print $2}'`
	OUTPUTTRA11=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $11}' | awk -F":" '{print $3}'`

	NAMATRANS12=TRA_Transformator_13
	INPUTTRA12=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $12}' | awk -F":" '{print $2}'`
	OUTPUTTRA12=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $12}' | awk -F":" '{print $3}'`

	NAMATRANS13=GRO_GroupBy_1
	INPUTTRA13=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $13}' | awk -F":" '{print $2}'`
	OUTPUTTRA13=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $13}' | awk -F":" '{print $3}'`
	
	NAMATRANS14=COM_Combiner_2
	INPUTTRA14=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $14}' | awk -F":" '{print $2}'`
	OUTPUTTRA14=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $14}' | awk -F":" '{print $3}'`
	
	NAMATRANS15=TRA_Transformator_12
	INPUTTRA15=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $15}' | awk -F":" '{print $2}'`
	OUTPUTTRA15=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $15}' | awk -F":" '{print $3}'`
	

	echo "$JOBID,$NAMATRANS1,$INPUTTRA1,$OUTPUTTRA1" > filelogngrs.csv
	echo "$JOBID,$NAMATRANS2,$INPUTTRA2,$OUTPUTTRA2" >> filelogngrs.csv
	echo "$JOBID,$NAMATRANS3,$INPUTTRA3,$OUTPUTTRA3" >> filelogngrs.csv
	echo "$JOBID,$NAMATRANS4,$INPUTTRA4,$OUTPUTTRA4" >> filelogngrs.csv
	echo "$JOBID,$NAMATRANS5,$INPUTTRA5,$OUTPUTTRA5" >> filelogngrs.csv
	echo "$JOBID,$NAMATRANS6,$INPUTTRA6,$OUTPUTTRA6" >> filelogngrs.csv
	echo "$JOBID,$NAMATRANS7,$INPUTTRA7,$OUTPUTTRA7" >> filelogngrs.csv
	echo "$JOBID,$NAMATRANS8,$INPUTTRA8,$OUTPUTTRA8" >> filelogngrs.csv
	echo "$JOBID,$NAMATRANS9,$INPUTTRA9,$OUTPUTTRA9" >> filelogngrs.csv
	echo "$JOBID,$NAMATRANS10,$INPUTTRA10,$OUTPUTTRA10" >> filelogngrs.csv
	echo "$JOBID,$NAMATRANS11,$INPUTTRA11,$OUTPUTTRA11" >> filelogngrs.csv
	echo "$JOBID,$NAMATRANS12,$INPUTTRA12,$OUTPUTTRA12" >> filelogngrs.csv
	echo "$JOBID,$NAMATRANS13,$INPUTTRA13,$OUTPUTTRA13" >> filelogngrs.csv
	echo "$JOBID,$NAMATRANS14,$INPUTTRA14,$OUTPUTTRA14" >> filelogngrs.csv
	echo "$JOBID,$NAMATRANS15,$INPUTTRA15,$OUTPUTTRA15" >> filelogngrs.csv

	mysql -uhdoop -p1q2q3q4q -Dfull <<EOF

	 load data local infile '/home/hdoop/filelogngrs.csv' into table logprocess_ngrs fields 
	 terminated by ',' 
	 enclosed by '"' 
	 lines terminated by '\n' 
	 (jobid,namatrans,input,output,created_date);
EOF
	cat HasilNGRS/h* > dataNGRS.csv

mysql -uhdoop -p1q2q3q4q -Dfull <<EOF

	 load data local infile '/home/hdoop/dataNGRS.csv' into table output_ngrs fields 
	 terminated by '|' 
	 enclosed by '"' 
	 lines terminated by '\n' 
	 (file_date,brand,offer_name,area,region,branch,subbranch,cluster,provinsi,kabupaten,kecamatan,kelurahan,area_hlr,region_hlr,city_hlr,channel_name,node,pre_post_flag,l1_name,l2_name,l3_name,cust_type_desc,cust_subtype_desc,vol,dur,rev,trx,flag,trx_date,load_date,rundate);

EOF

	rm /home/hdoop/filelock.lock ##hapus filelock.lock
fi
