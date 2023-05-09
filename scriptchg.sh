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
	CLASS=CHG_Reject
	export NAMAJAR=/home/hdoop/Yunita_spark2.jar

	if [ -d /home/hdoop/CHG_Reject ];then
		echo "Folder masih ada, hapus dulu ya"
		rm -r /home/hdoop/CHG_Reject
	fi

	spark-submit --class ${PACKAGE}.${CLASS} --master yarn \
	${NAMAJAR} \
	hdfs:///user/hdoop/datayunita/HLR \
	hdfs:///user/hdoop/datayunita/CHG \
	hdfs:///user/hdoop/HVC2/hgrid247-00001 \
	hdfs:///user/hdoop/datayunita/offer_dim.csv \
	hdfs:///user/hdoop/datayunita/channel.csv \
	file:///home/hdoop/CHG_Reject

	echo "Proses Selesai pada:" `date`

	LOGFILE=`ls -lrt ${PACKAGE}.${CLASS}*log | tail -1 | awk '{print $9}'`
	NAMATRANS1=TRA_Read_CHG
 	INPUTTRA1=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $1}' | awk -F":" '{print $2}'`
  	OUTPUTTRA1=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $1}' | awk -F":" '{print $3}'`

	NAMATRANS2=TRA_Read_HLR
	INPUTTRA2=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $2}' | awk -F":" '{print $2}'`
	OUTPUTTRA2=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $2}' | awk -F":" '{print $3}'`

	NAMATRANS3=RJO_RefJoin_1
	INPUTTRA3=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $3}' | awk -F":" '{print $2}'`
	OUTPUTTRA3=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $3}' | awk -F":" '{print $3}'`

	NAMATRANS4=TRA_Join_HLR
    INPUTTRA4=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $4}' | awk -F":" '{print $2}'`
    OUTPUTTRA4=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $4}' | awk -F":" '{print $3}'`

	NAMATRANS5=TRA_Join_HLR_2
    INPUTTRA5=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $5}' | awk -F":" '{print $2}'`
    OUTPUTTRA5=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $5}' | awk -F":" '{print $3}'`

	NAMATRANS6=TRA_read_HVC
	INPUTTRA6=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $6}' | awk -F":" '{print $2}'`
	OUTPUTTRA6=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $6}' | awk -F":" '{print $3}'`

	NAMATRANS7=TRA_read_HVC_2
	INPUTTRA7=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $7}' | awk -F":" '{print $2}'`
	OUTPUTTRA7=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $7}' | awk -F":" '{print $3}'`

	NAMATRANS8=JOI_Join_2
	INPUTTRA8=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $8}' | awk -F":" '{print $2}'`
	OUTPUTTRA8=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $8}' | awk -F":" '{print $3}'`

	NAMATRANS9=TRA_Join_HVC
	INPUTTRA9=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $9}' | awk -F":" '{print $2}'`
	OUTPUTTRA9=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $9}' | awk -F":" '{print $3}'`

	NAMATRANS10=TRA_read_OFFER
	INPUTTRA10=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $10}' | awk -F":" '{print $2}'`
	OUTPUTTRA10=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $10}' | awk -F":" '{print $3}'`

	NAMATRANS11=JOI_Join_3
	INPUTTRA11=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $11}' | awk -F":" '{print $2}'`
	OUTPUTTRA11=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $11}' | awk -F":" '{print $3}'`

	NAMATRANS12=TRA_Join_OFFER
	INPUTTRA12=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $12}' | awk -F":" '{print $2}'`
	OUTPUTTRA12=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $12}' | awk -F":" '{print $3}'`

	NAMATRANS13=TRA_read_CHANNEL
	INPUTTRA13=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $13}' | awk -F":" '{print $2}'`
	OUTPUTTRA13=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $13}' | awk -F":" '{print $3}'`
	
	NAMATRANS14=JOI_Join_4
	INPUTTRA14=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $14}' | awk -F":" '{print $2}'`
	OUTPUTTRA14=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $14}' | awk -F":" '{print $3}'`
	
	NAMATRANS15=TRA_Join_CHANNEL
	INPUTTRA15=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $15}' | awk -F":" '{print $2}'`
	OUTPUTTRA15=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $15}' | awk -F":" '{print $3}'`
	
	NAMATRANS16=TRA_Proses_Summary
	INPUTTRA16=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $16}' | awk -F":" '{print $2}'`
	OUTPUTTRA16=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $16}' | awk -F":" '{print $3}'`
	
	NAMATRANS17=GRO_GroupBy_2
	INPUTTRA17=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $17}' | awk -F":" '{print $2}'`
	OUTPUTTRA17=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $17}' | awk -F":" '{print $3}'`
	
	NAMATRANS18=COM_Combiner_2
	INPUTTRA18=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $18}' | awk -F":" '{print $2}'`
	OUTPUTTRA18=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $18}' | awk -F":" '{print $3}'`
	
	NAMATRANS19=TRA_Transformator_15
	INPUTTRA19=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $19}' | awk -F":" '{print $2}'`
	OUTPUTTRA19=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $19}' | awk -F":" '{print $3}'`
	
	NAMATRANS20=TRA_Transformator_16
	INPUTTRA20=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $20}' | awk -F":" '{print $2}'`
	OUTPUTTRA20=`cat ${LOGFILE} | head -3 | tail -1 | awk -F"|" '{print $14}' | awk -F";" '{print $20}' | awk -F":" '{print $3}'`
	
	echo "$JOBID,$NAMATRANS1,$INPUTTRA1,$OUTPUTTRA1" > filelogchg.csv
	echo "$JOBID,$NAMATRANS2,$INPUTTRA2,$OUTPUTTRA2" >> filelogchg.csv
	echo "$JOBID,$NAMATRANS3,$INPUTTRA3,$OUTPUTTRA3" >> filelogchg.csv
	echo "$JOBID,$NAMATRANS4,$INPUTTRA4,$OUTPUTTRA4" >> filelogchg.csv
	echo "$JOBID,$NAMATRANS5,$INPUTTRA5,$OUTPUTTRA5" >> filelogchg.csv
	echo "$JOBID,$NAMATRANS6,$INPUTTRA6,$OUTPUTTRA6" >> filelogchg.csv
	echo "$JOBID,$NAMATRANS7,$INPUTTRA7,$OUTPUTTRA7" >> filelogchg.csv
	echo "$JOBID,$NAMATRANS8,$INPUTTRA8,$OUTPUTTRA8" >> filelogchg.csv
	echo "$JOBID,$NAMATRANS9,$INPUTTRA9,$OUTPUTTRA9" >> filelogchg.csv
	echo "$JOBID,$NAMATRANS10,$INPUTTRA10,$OUTPUTTRA10" >> filelogchg.csv
	echo "$JOBID,$NAMATRANS11,$INPUTTRA11,$OUTPUTTRA11" >> filelogchg.csv
	echo "$JOBID,$NAMATRANS12,$INPUTTRA12,$OUTPUTTRA12" >> filelogchg.csv
	echo "$JOBID,$NAMATRANS13,$INPUTTRA13,$OUTPUTTRA13" >> filelogchg.csv
	echo "$JOBID,$NAMATRANS14,$INPUTTRA14,$OUTPUTTRA14" >> filelogchg.csv
	echo "$JOBID,$NAMATRANS15,$INPUTTRA15,$OUTPUTTRA15" >> filelogchg.csv
	echo "$JOBID,$NAMATRANS16,$INPUTTRA16,$OUTPUTTRA16" >> filelogchg.csv
	echo "$JOBID,$NAMATRANS17,$INPUTTRA17,$OUTPUTTRA17" >> filelogchg.csv
	echo "$JOBID,$NAMATRANS18,$INPUTTRA18,$OUTPUTTRA18" >> filelogchg.csv
	echo "$JOBID,$NAMATRANS19,$INPUTTRA19,$OUTPUTTRA19" >> filelogchg.csv
	echo "$JOBID,$NAMATRANS20,$INPUTTRA20,$OUTPUTTRA20" >> filelogchg.csv

	mysql -uhdoop -p1q2q3q4q -Dfull <<EOF

	 load data local infile '/home/hdoop/filelogchg.csv' into table logprocess_chg fields 
	 terminated by ',' 
	 enclosed by '"' 
	 lines terminated by '\n' 
	 (jobid,namatrans,input,output,created_date);
EOF
	cat /home/hdoop/CHG_Reject/h* > dataCHG.csv

mysql -uhdoop -p1q2q3q4q -Dfull <<EOF

	 load data local infile '/home/hdoop/dataCHG.csv' into table output_chg fields 
	 terminated by '|' 
	 enclosed by '"' 
	 lines terminated by '\n' 
	 (FILE_DATE,BRAND,OFFER_NAME,AREA,REGION,BRANCH,SUBBRANCH,CLUSTER,PROVINSI,KABUPATEN,KECAMATAN,KELURAHAN,AREA_HLR,REGION_HLR,CITY_HLR,CHANNEL_NAME,NODE,PRE_POST_FLAG,L1_NAME,L2_NAME,L3_NAME,CUST_TYPE_DESC,CUST_SUBTYPE_DESC,VOL,DUR,REV,TRX,FLAG,TRX_DATE,LOAD_DATE,RUNDATE);

EOF

	rm /home/hdoop/filelock.lock ##hapus filelock.lock
fi
