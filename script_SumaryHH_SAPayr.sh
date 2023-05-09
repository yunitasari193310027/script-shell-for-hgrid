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
	CLASS=SumaryHH
	export NAMAJAR=/home/hdoop/Yunita_spark2.jar

	if [ -d /home/hdoop/SumaryHH_SAPAYR ];then
		echo "Folder masih ada, hapus dulu ya"
		rm -r /home/hdoop/SumaryHH_SAPAYR
	fi

	spark-submit --class ${PACKAGE}.${CLASS} --master local[1] \
	${NAMAJAR} \
	file:///home/hdoop/HasilPayrGood \
	file:///home/hdoop/SumaryHH_SAPAYR

	echo "Proses Selesai pada:" `date`

	counter=1
	getlog=-3
	while [ $counter -le 7 ]
	do
		
		LOGFILE=`ls -lrt ${PACKAGE}.${CLASS}*log | tail -1 | awk '{print $9}'`

		NAMATRANS=`cat ${LOGFILE} |awk -F"|" '{print $(NF-1)}' |awk '{gsub(";","\n")}1' | head $getlog | tail -1 | awk -F":" '{print $1}'`
		INPUTTRA=`cat ${LOGFILE} |awk -F"|" '{print $(NF-1)}' |awk '{gsub(";","\n")}1' | head $getlog  | tail -1 | awk -F":" '{print $3}'`
		OUTPUTTRA=`cat ${LOGFILE} |awk -F"|" '{print $(NF-1)}' |awk '{gsub(";","\n")}1' | head $getlog  | tail -1 | awk -F":" '{print $3}'`
		
		echo "$JOBID,$NAMATRANS,$INPUTTRA,$OUTPUTTRA" >> filelogSumaryHH.csv

		((counter++))
		((getlog--))
	
	done
	echo "succesfull create filelog"
	
	mysql -uhdoop -p1q2q3q4q -Dfull <<EOF

	 load data local infile '/home/hdoop/filelogSumaryHH.csv' into table logprocess_SumaryHH fields 
	 terminated by ',' 
	 enclosed by '"' 
	 lines terminated by '\n' 
	 (jobid,namatrans,input,output,created_date);
EOF
	cat SumaryHH_SAPAYR/h* > SumaryHH_SAPAYR.csv

mysql -uhdoop -p1q2q3q4q -Dfull <<EOF

	 load data local infile '/home/hdoop/SumaryHH_SAPAYR.csv' into table SumaryHH_SAPAYR fields 
	 terminated by '~' 
	 enclosed by '"' 
	 lines terminated by '\n' 
	 (trx_date,trx_hour,msisdn,subs_id,cust_type_desc,cust_subtype_desc,bss_order_id,offer_id,l4_name,l3_name,l2_name,l1_name,payment_channel,lacci_id,lacci_closing_flag,lac,ci,node_type,area_sales,region_sales,branch,subbranch,cluster_sales,provinsi,kabupaten,kecamatan,kelurahan,brand,trx,rev,indicator_4g,pre_post_flag,payment_timestamp,event_date,load_date,load_ts,load_user);
EOF

	rm /home/hdoop/filelock.lock ##hapus filelock.lock
fi
