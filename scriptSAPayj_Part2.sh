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
	CLASS=End
	export NAMAJAR=/home/hdoop/SAPayj_spark2.jar

	if [ -d /home/hdoop/HasilPayjGood ];then
		echo "Folder masih ada, hapus dulu ya"
		rm -r /home/hdoop/HasilPayjGood
	fi

	if [ -d /home/hdoop/HasilPayjReject ];then
		echo "Folder masih ada, hapus dulu ya"
		rm -r /home/hdoop/HasilPayjReject
	fi

	spark-submit --class ${PACKAGE}.${CLASS} --master local[1] \
	${NAMAJAR} \
	file:///home/hdoop/datayunita/20221219 \
	file:///home/hdoop/datayunita/sa_product \
	file:///home/hdoop/datayunita/dom_mm \
	file:///home/hdoop/datayunita/dom_mtd \
	file:///home/hdoop/PAYJ_Part1 \
	file:///home/hdoop/HasilPayjGood \
	file:///home/hdoop/HasilPayjReject

	echo "Proses Selesai pada:" `date`

	counter=1
	getlog=-3
	while [ $counter -le 51 ]
	do
		
		LOGFILE=`ls -lrt ${PACKAGE}.${CLASS}*log | tail -1 | awk '{print $9}'`

		NAMATRANS=`cat ${LOGFILE} |awk -F"|" '{print $(NF-1)}' |awk '{gsub(";","\n")}1' | head $getlog | tail -1 | awk -F":" '{print $1}'`
		INPUTTRA=`cat ${LOGFILE} |awk -F"|" '{print $(NF-1)}' |awk '{gsub(";","\n")}1' | head $getlog  | tail -1 | awk -F":" '{print $3}'`
		OUTPUTTRA=`cat ${LOGFILE} |awk -F"|" '{print $(NF-1)}' |awk '{gsub(";","\n")}1' | head $getlog  | tail -1 | awk -F":" '{print $3}'`
		
		echo "$JOBID,$NAMATRANS,$INPUTTRA,$OUTPUTTRA" >> filelogSAPayr_Part2.csv

		((counter++))
		((getlog--))
	
	done
	echo "succesfull create filelog"
	
	mysql -uhdoop -p1q2q3q4q -Dfull <<EOF

	 load data local infile '/home/hdoop/filelogSAPayj_Part2.csv' into table logprocess_SAPayjPart2 fields 
	 terminated by ',' 
	 enclosed by '"' 
	 lines terminated by '\n' 
	 (jobid,namatrans,input,output,created_date);
EOF
	cat HasilPayjGood/h* > HasilPayjGood.csv

mysql -uhdoop -p1q2q3q4q -Dfull <<EOF

	 load data local infile '/home/hdoop/HasilPayjGood.csv' into table SAPayj_good fields 
	 terminated by '~' 
	 enclosed by '"' 
	 lines terminated by '\n' 
	 (payment_timestamp,trx_date,trx_hour,msisdn,subs_id,cust_type_desc,cust_subtype_desc,bss_order_id,plan_id,plan_name,topping_id,topping_name,plan_price,offer_id,l4_name,l3_name,l2_name,l1_name,payment_channel,cell_id,lacci_id,lacci_closing_flag,lac,ci,node_type,area_sales,region_sales,branch,subbranch,cluster_sales,provinsi,kabupaten,kecamatan,kelurahan,indicator_4g,future_string_1,future_string_2,future_string_3,brand,site_name,file_id,load_ts,load_user,pre_post_flag,event_date,bundling_id,future_string_5,future_string_6,future_string_7,future_string_8,future_string_9,future_string_10);

EOF

	cat HasilPayjReject/h* > HasilPayjReject.csv

mysql -uhdoop -p1q2q3q4q -Dfull <<EOF

	 load data local infile '/home/hdoop/HasilPayjReject.csv' into table SAPayj_reject fields 
	 terminated by '~' 
	 enclosed by '"' 
	 lines terminated by '\n' 
	 (activation_timestamp,trx_date,trx_hour,msisdn,subs_id,cust_type_desc,cust_subtype_desc,bss_order_id,plan_id,plan_name,topping_id,topping_name,plan_price,payment_timestamp,offer_id,l4_name,l3_name,l2_name,l1_name,payment_channel,cell_id,lacci_id,lacci_closing_flag,lac,ci,node_type,area_sales,region_sales,branch,subbranch,cluster_sales,provinsi,kabupaten,kecamatan,kelurahan,indicator_4g,future_string_1,future_string_2,future_string_3,brand,site_name,file_id,load_ts,load_user,pre_post_flag,event_date,future_string_4,future_string_5,future_string_6,future_string_7,future_string_8,future_string_9,future_string_10,filename,trx_lacci,lacci_id_flag,retry_count,status_reject);

EOF
	rm /home/hdoop/filelock.lock ##hapus filelock.lock
fi
