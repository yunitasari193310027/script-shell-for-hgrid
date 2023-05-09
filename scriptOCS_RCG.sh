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
	CLASS=Final
	export NAMAJAR=/home/hdoop/SAUsageOCS_spark2.jar

	if [ -d /home/hdoop/USAGE_SA_RCG_REJECT ];then
		echo "Folder masih ada, hapus dulu ya"
		rm -r /home/hdoop/USAGE_SA_RCG_REJECT
	fi

	if [ -d /home/hdoop/USAGE_SA_RCG ];then
		echo "Folder masih ada, hapus dulu ya"
		rm -r /home/hdoop/USAGE_SA_RCG
	fi

	if [ -d /home/hdoop/MERGE_SA_RCG_REJECT ];then
		echo "Folder masih ada, hapus dulu ya"
		rm -r /home/hdoop/MERGE_SA_RCG_REJECT
	fi

	if [ -d /home/hdoop/MERGE_SA_RCG_GOOD ];then
		echo "Folder masih ada, hapus dulu ya"
		rm -r /home/hdoop/MERGE_SA_RCG_GOOD
	fi

	if [ -d /home/hdoop/USAGE_SA_RCG_BAD ];then
		echo "Folder masih ada, hapus dulu ya"
		rm -r /home/hdoop/USAGE_SA_RCG_BAD
	fi

	if [ -d /home/hdoop/USAGE_SA_RCG_DUP01 ];then
		echo "Folder masih ada, hapus dulu ya"
		rm -r /home/hdoop/USAGE_SA_RCG_DUP01
	fi

	if [ -d /home/hdoop/USAGE_SA_RCG_DUP03 ];then
		echo "Folder masih ada, hapus dulu ya"
		rm -r /home/hdoop/USAGE_SA_RCG_DUP03
	fi

	if [ -d /home/hdoop/USAGE_SA_RCG_DUP02 ];then
		echo "Folder masih ada, hapus dulu ya"
		rm -r /home/hdoop/USAGE_SA_RCG_DUP02
	fi

	if [ -f /home/hdoop/filelogOCS_RCG.csv ];then
		echo "Filelog masih ada, hapus dulu ya"
		rm /home/hdoop/filelogOCS_RCG.csv
	fi

	if [ -d /home/hdoop/USAGE_SA_RCG_SUM ];then
		echo "Folder masih ada, hapus dulu ya"
		rm -r /home/hdoop/USAGE_SA_RCG_SUM
	fi

	spark-submit --class ${PACKAGE}.${CLASS} --master local[1] \
	${NAMAJAR} \
	file:///home/hdoop/datayunita/USAGECHG/prepMTD \
	file:///home/hdoop/datayunita/USAGECHG/prepMM \
	file:///home/hdoop/datayunita/USAGECHG/LACCIMA_DIM.csv \
	file:///home/hdoop/datayunita/USAGECHG/SUB_DIM \
	file:///home/hdoop/datayunita/USAGECHG/SA_PRODLINE_BONUS \
	file:///home/hdoop/datayunita/SAUsageOCS/RCG \
	file:///home/hdoop/datayunita/SAUsageOCS/TC_RCG\
	file:///home/hdoop/USAGE_SA_RCG_REJECT \
	file:///home/hdoop/USAGE_SA_RCG \
	file:///home/hdoop/USAGE_SA_RCG_SUM \
	file:///home/hdoop/MERGE_SA_RCG_GOOD \
	file:///home/hdoop/MERGE_SA_RCG_REJECT \
	file:///home/hdoop/USAGE_SA_RCG_BAD \
	file:///home/hdoop/USAGE_SA_RCG_DUP01 \
	file:///home/hdoop/USAGE_SA_RCG_DUP03 \
	file:///home/hdoop/USAGE_SA_RCG_DUP02 

	echo "Proses Selesai pada:" `date`

	counter=1
	getlog=-3
	while [ $counter -le 80 ]
	do
		
		LOGFILE=`ls -lrt ${PACKAGE}.${CLASS}*log | tail -1 | awk '{print $9}'`

		NAMATRANS=`cat ${LOGFILE} |awk -F"|" '{print $(NF-1)}' |awk '{gsub(";","\n")}1' | head $getlog | tail -1 | awk -F":" '{print $1}'`
		INPUTTRA=`cat ${LOGFILE} |awk -F"|" '{print $(NF-1)}' |awk '{gsub(";","\n")}1' | head $getlog  | tail -1 | awk -F":" '{print $3}'`
		OUTPUTTRA=`cat ${LOGFILE} |awk -F"|" '{print $(NF-1)}' |awk '{gsub(";","\n")}1' | head $getlog  | tail -1 | awk -F":" '{print $3}'`
		
		echo "$JOBID,$NAMATRANS,$INPUTTRA,$OUTPUTTRA" >> /home/hdoop/filelogOCS_RCG.csv

		((counter++))
		((getlog--))
	
	done
	echo "succesfull create filelog"
	
	mysql -uhdoop -p1q2q3q4q -Dfull <<EOF

	 load data local infile '/home/hdoop/filelogOCS_RCG.csv' into table logprocess_USAGE_SA_RCG fields 
	 terminated by ',' 
	 enclosed by '"' 
	 lines terminated by '\n' 
	 (jobid,namatrans,input,output,created_date);
EOF
	cat /home/hdoop/USAGE_SA_RCG_REJECT/h* > USAGE_SA_RCG_REJECT.csv

mysql -uhdoop -p1q2q3q4q -Dfull <<EOF

	load data local infile '/home/hdoop/USAGE_SA_RCG_REJECT.csv' into table USAGE_SA_RCG_REJECT fields 
    terminated by '~' 
    enclosed by '"' 
    lines terminated by '\n' 
    (timestamp_r,trx_hour,trx_date,msisdn,subs_id,cust_type_desc,cust_subtype_desc,account,recharge_channel,expiration_date,serial_number,delta_balance,balance_amount,credit_indicator,recharge_method,recharge_id,bonus_information,provider_id,source_ip,user_id,result_code,bank_code,a_number_location,lacci_id,lacci_closing_flag,lac,ci,node_type,area_sales,region_sales,branch,subbranch,cluster_sales,provinsi,kabupaten,kecamatan,kelurahan,indicator_4g,balance_before,adjustment_reason,case_id,crmuser_id,old_expiration_date,split_code,recharge_amount,future_string_1,future_string_2,future_string_3,brand,file_id,load_ts,load_user,site_name,event_date,filename,rec_id,retry_count,retry_ts,load_date,p_event_date);
EOF

	cat /home/hdoop/USAGE_SA_RCG/h* > USAGE_SA_RCG.csv

mysql -uhdoop -p1q2q3q4q -Dfull <<EOF

	load data local infile '/home/hdoop/USAGE_SA_RCG.csv' into table USAGE_SA_RCG fields 
    terminated by '~' 
    enclosed by '"' 
    lines terminated by '\n' 
    (timestamp_r,trx_hour,trx_date,msisdn,subs_id,cust_type_desc,cust_subtype_desc,account,recharge_channel,expiration_date,serial_number,delta_balance,balance_amount,credit_indicator,recharge_method,recharge_id,bonus_information,provider_id,source_ip,user_id,result_code,bank_code,a_number_location,lacci_id,lacci_closing_flag,lac,ci,node_type,area_sales,region_sales,branch,subbranch,cluster_sales,provinsi,kabupaten,kecamatan,kelurahan,indicator_4g,balance_before,adjustment_reason,case_id,crmuser_id,old_expiration_date,split_code,recharge_amount,future_string_1,future_string_2,future_string_3,brand,file_id,load_ts,load_user,site_name,event_date,filename,rec_id,retry_count,retry_ts,load_date,p_event_date);
EOF
	rm /home/hdoop/filelock.lock ##hapus filelock.lock
fi
