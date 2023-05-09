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
	CLASS=Part1
	export NAMAJAR=/home/hdoop/SAPayj_spark2.jar

	if [ -d /home/hdoop/PAYJ_Part1 ];then
		echo "Folder masih ada, hapus dulu ya"
		rm -r /home/hdoop/PAYJ_Part1
	fi

	spark-submit --class ${PACKAGE}.${CLASS} --master local[1] \
	${NAMAJAR} \
	file:///home/hdoop/datayunita/payj/PAYJ_JKT_Pre_20221220235959_00000634_51.1.dat \
	file:///home/hdoop/datayunita/SA_REJECT \
	file:///home/hdoop/datayunita/event_date=2022-12-18 \
	file:///home/hdoop/PAYJ_Part1

	echo "Proses Selesai pada:" `date`

	counter=1
	getlog=-3
	while [ $counter -le 14 ]
	do
		
		LOGFILE=`ls -lrt ${PACKAGE}.${CLASS}*log | tail -1 | awk '{print $9}'`

		NAMATRANS=`cat ${LOGFILE} |awk -F"|" '{print $(NF-1)}' |awk '{gsub(";","\n")}1' | head $getlog | tail -1 | awk -F":" '{print $1}'`
		INPUTTRA=`cat ${LOGFILE} |awk -F"|" '{print $(NF-1)}' |awk '{gsub(";","\n")}1' | head $getlog  | tail -1 | awk -F":" '{print $3}'`
		OUTPUTTRA=`cat ${LOGFILE} |awk -F"|" '{print $(NF-1)}' |awk '{gsub(";","\n")}1' | head $getlog  | tail -1 | awk -F":" '{print $3}'`
		
		echo "$JOBID,$NAMATRANS,$INPUTTRA,$OUTPUTTRA" >> filelogSAPayj.csv

		((counter++))
		((getlog--))
	
	done
	echo "succesfull create filelog"
	
	mysql -uhdoop -p1q2q3q4q -Dfull <<EOF

	 load data local infile '/home/hdoop/filelogSAPayj.csv' into table logprocess_SAPayj fields 
	 terminated by ',' 
	 enclosed by '"' 
	 lines terminated by '\n' 
	 (jobid,namatrans,input,output,created_date);
EOF
	cat PAYJ_Part1/h* > PAYJ_Part1.csv

mysql -uhdoop -p1q2q3q4q -Dfull <<EOF

	 load data local infile '/home/hdoop/PAYJ_Part1.csv' into table SAPayj_Part1 fields 
	 terminated by '~' 
	 enclosed by '"' 
	 lines terminated by '\n' 
	 (activation_timestamp,trx_date,trx_hour,msisdn,bss_order_id,plan_id,plan_name,topping_id,topping_name,plan_price,payment_timestamp,offer_id,payment_channel,cell_id,indicator_4g,future_string_1,future_string_2,future_string_3,brand,site_name,pre_post_flag,event_date,future_string_4,future_string_5,future_string_6,future_string_7,future_string_8,future_string_9,future_string_10,filename,trx_lacci,subs_id,cust_type_desc,cust_subtype_desc,retry_count);

EOF

	rm /home/hdoop/filelock.lock ##hapus filelock.lock
fi
