#!/bin/bash

RUNDATE=`date`
JOBID=Bebas

### LOCKING PROCESS

if [ -e /home/hdoop/filelock.lock ];then
	echo "Proses masih jalan"
	exit ####aplikasi keluar
else
	touch /home/hdoop/filelock.lock ##genrate filelock.lock

	PACKAGE=SAPayj
	CLASS=SumaryHH
	export NAMAJAR=/home/hdoop/SAPayj_spark2.jar

	if [ -d /home/hdoop/SumaryHH_SAPAYJ ];then
		echo "Folder masih ada, hapus dulu ya"
		rm -r /home/hdoop/SumaryHH_SAPAYJ
	fi

	if [ -f /home/hdoop/filelogSumaryHHPayj.csv ];then
		echo "Filelog masih ada, hapus dulu ya"
		rm -r /home/hdoop/filelogSumaryHHPayj.csv
	fi

	spark-submit --class ${PACKAGE}.${CLASS} --master local[1] \
	${NAMAJAR} \
	file:///home/hdoop/datayunita/PAYJ_GOOD \
	file:///home/hdoop/SumaryHH_SAPAYJ

	echo "Proses Selesai pada:" `date`

	counter=1
	getlog=-3
	while [ $counter -le 7 ]
	do
		
		LOGFILE=`ls -lrt ${PACKAGE}.${CLASS}*log | tail -1 | awk '{print $9}'`

		NAMATRANS=`cat ${LOGFILE} |awk -F"|" '{print $(NF-1)}' |awk '{gsub(";","\n")}1' | head $getlog | tail -1 | awk -F":" '{print $1}'`
		INPUTTRA=`cat ${LOGFILE} |awk -F"|" '{print $(NF-1)}' |awk '{gsub(";","\n")}1' | head $getlog  | tail -1 | awk -F":" '{print $3}'`
		OUTPUTTRA=`cat ${LOGFILE} |awk -F"|" '{print $(NF-1)}' |awk '{gsub(";","\n")}1' | head $getlog  | tail -1 | awk -F":" '{print $3}'`
		
		echo "$JOBID,$NAMATRANS,$INPUTTRA,$OUTPUTTRA" >> filelogSumaryHHPayj.csv

		((counter++))
		((getlog--))
	
	done
	echo "succesfull create filelog"
	
	mysql -uhdoop -p1q2q3q4q -Dfull <<EOF

	 load data local infile '/home/hdoop/filelogSumaryHHPayj.csv' into table logprocess_SumaryHHPayj fields 
	 terminated by ',' 
	 enclosed by '"' 
	 lines terminated by '\n' 
	 (jobid,namatrans,input,output,created_date);
EOF
	cat SumaryHH_SAPAYJ/h* > SumaryHH_SAPAYJ.csv

mysql -uhdoop -p1q2q3q4q -Dfull <<EOF

	 load data local infile '/home/hdoop/SumaryHH_SAPAYJ.csv' into table SumaryHH_SAPAYJ fields 
	 terminated by '~' 
	 enclosed by '"' 
	 lines terminated by '\n' 
	 (trx_date,trx_hour,msisdn,subs_id,cust_type_desc,cust_subtype_desc,bss_order_id,offer_id,l4_name,l3_name,l2_name,l1_name,payment_channel,lacci_id,lacci_closing_flag,lac,ci,node_type,area_sales,region_sales,branch,subbranch,cluster_sales,provinsi,kabupaten,kecamatan,kelurahan,brand,trx,rev,indicator_4g,pre_post_flag,payment_timestamp,event_date,load_date,load_ts,load_user);
EOF

	rm /home/hdoop/filelock.lock ##hapus filelock.lock
fi
