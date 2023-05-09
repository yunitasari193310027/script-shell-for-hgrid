#!/bin/bash

RUNDATE=`date`
JOBID=Bebas

### LOCKING PROCESS

if [ -e /home/hdoop/filelock.lock ];then
	echo "Proses masih jalan"
	exit ####aplikasi keluar
else
	touch /home/hdoop/filelock.lock ##genrate filelock.lock

	PACKAGE=SAUsageOCS
	CLASS=HH
	export NAMAJAR=/home/hdoop/SAUsageOCS_spark2.jar

	if [ -d /home/hdoop/USAGE_SA_RCG_HH ];then
		echo "Folder masih ada, hapus dulu ya"
		rm -r /home/hdoop/USAGE_SA_RCG_HH
	fi

	if [ -f /home/hdoop/filelogRCG_HH.csv ];then
		echo "Filelog masih ada, hapus dulu ya"
		rm /home/hdoop/filelogRCG_HH.csv
	fi


	spark-submit --class ${PACKAGE}.${CLASS} --master local[1] \
	${NAMAJAR} \
	file:///home/hdoop/datayunita/SAUsageOCS/HH \
	file:///home/hdoop/USAGE_SA_RCG_HH

	echo "Proses Selesai pada:" `date`

	counter=1
	getlog=-3
	while [ $counter -le 5 ]
	do
		
		LOGFILE=`ls -lrt ${PACKAGE}.${CLASS}*log | tail -1 | awk '{print $9}'`

		NAMATRANS=`cat ${LOGFILE} |awk -F"|" '{print $(NF-1)}' |awk '{gsub(";","\n")}1' | head $getlog | tail -1 | awk -F":" '{print $1}'`
		INPUTTRA=`cat ${LOGFILE} |awk -F"|" '{print $(NF-1)}' |awk '{gsub(";","\n")}1' | head $getlog  | tail -1 | awk -F":" '{print $3}'`
		OUTPUTTRA=`cat ${LOGFILE} |awk -F"|" '{print $(NF-1)}' |awk '{gsub(";","\n")}1' | head $getlog  | tail -1 | awk -F":" '{print $3}'`
		
		echo "$JOBID,$NAMATRANS,$INPUTTRA,$OUTPUTTRA" >> /home/hdoop/filelogRCG_HH.csv

		((counter++))
		((getlog--))
	
	done
	echo "succesfull create filelog"
	
	mysql -uhdoop -p1q2q3q4q -Dfull <<EOF

	 load data local infile '/home/hdoop/filelogRCG_HH.csv' into table logprocess_RCG_HH fields 
	 terminated by ',' 
	 enclosed by '"' 
	 lines terminated by '\n' 
	 (jobid,namatrans,input,output,created_date);
EOF
	cat /home/hdoop/USAGE_SA_RCG_HH/h* > USAGE_SA_RCG_HH.csv

mysql -uhdoop -p1q2q3q4q -Dfull <<EOF

	load data local infile '/home/hdoop/USAGE_SA_RCG_HH.csv' into table RCG_HH fields 
    terminated by '~' 
    enclosed by '"' 
    lines terminated by '\n' 
    (trx_date,trx_hour,subs_id,msisdn,cust_type_desc,cust_subtype_desc,offer_id,lacci_id,lac,ci,area_sales,region_sales,branch,subbranch,cluster_sales,provinsi,kabupaten,kecamatan,kelurahan,node_type,lacci_closing_flag,credit_debit_code,split_code,brand,trx,recharge_amount,load_ts,load_user,job_id,indicator_4g,event_date,load_date);
EOF

	rm /home/hdoop/filelock.lock ##hapus filelock.lock
fi
