#!/bin/bash

RUNDATE=`date`
JOBID=Bebas

### LOCKING PROCESS

if [ -e /home/hdoop/filelock.lock ];then
	echo "Proses masih jalan"
	exit ####aplikasi keluar
else
	touch /home/hdoop/filelock.lock ##genrate filelock.lock

	PACKAGE=SAUsageChg
	CLASS=HH
	export NAMAJAR=/home/hdoop/SAUsageChg_spark2.jar

	if [ -d /home/hdoop/HasilCHGUSAGE_HH ];then
		echo "Folder masih ada, hapus dulu ya"
		rm -r /home/hdoop/HasilCHGUSAGE_HH
	fi

	if [ -f /home/hdoop/filelogCHGUSAGE_HH.csv ];then
		echo "Filelog masih ada, hapus dulu ya"
		rm /home/hdoop/filelogCHGUSAGE_HH.csv
	fi

	spark-submit --class ${PACKAGE}.${CLASS} --master local[1] \
	${NAMAJAR} \
	file:///home/hdoop/datayunita/USAGECHG/job_id=20230120020001 \
	file:///home/hdoop/HasilCHGUSAGE_HH	

	echo "Proses Selesai pada:" `date`

	counter=1
	getlog=-3
	while [ $counter -le 7 ]
	do
		
		LOGFILE=`ls -lrt ${PACKAGE}.${CLASS}*log | tail -1 | awk '{print $9}'`

		NAMATRANS=`cat ${LOGFILE} |awk -F"|" '{print $(NF-1)}' |awk '{gsub(";","\n")}1' | head $getlog | tail -1 | awk -F":" '{print $1}'`
		INPUTTRA=`cat ${LOGFILE} |awk -F"|" '{print $(NF-1)}' |awk '{gsub(";","\n")}1' | head $getlog  | tail -1 | awk -F":" '{print $2}'`
		OUTPUTTRA=`cat ${LOGFILE} |awk -F"|" '{print $(NF-1)}' |awk '{gsub(";","\n")}1' | head $getlog  | tail -1 | awk -F":" '{print $3}'`
		
		echo "$JOBID,$NAMATRANS,$INPUTTRA,$OUTPUTTRA" >> filelogCHGUSAGE_HH.csv

		((counter++))
		((getlog--))
	
	done
	echo "succesfull create filelog"
	
	mysql -uhdoop -p1q2q3q4q -Dfull <<EOF

	 load data local infile '/home/hdoop/filelogCHGUSAGE_HH.csv' into table logprocess_USAGECHG_HH fields 
	 terminated by ',' 
	 enclosed by '"' 
	 lines terminated by '\n' 
	 (jobid,namatrans,input,output,created_date);
EOF
	cat HasilCHGUSAGE_HH/h* > HasilCHGUSAGE_HH.csv

mysql -uhdoop -p1q2q3q4q -Dfull <<EOF

	load data local infile '/home/hdoop/HasilCHGUSAGE_HH.csv' into table SA_USAGECHG_HH fields 
    terminated by '~' 
    enclosed by '"' 
    lines terminated by '\n' 
    (trx_date,trx_hour,subs_id,msisdn,cust_type_desc,cust_subtype_desc,call_type,call_subtype,l1_name,l2_name,l3_name,l4_name,offer_id,lacci_id,lac,ci,area_sales,region_sales,branch,subbranch,cluster_sales,provinsi,kabupaten,kecamatan,kelurahan,node_type,lacci_closing_flag,vlr_num,mcc_mnc,charge_code,apn_id,content_id,cp_name,credit_debit_code,service_filter,rating_group,vas_code,rating_offer,country,tap_code,split_cd_price,call_direction,brand,trx,trx_c,rev,dur,dur_c,dur_ci,dur_free,dur_rnd,dur_ncwb,dur_ncnb,dur_nci,vol_ncwb,vol_ncnb,vol,vol_c,vol_free,vol_rnd,load_ts,load_user,job_id,indicator_4g,pre_post_flag,event_date,load_date);
EOF

	rm /home/hdoop/filelock.lock ##hapus filelock.lock
fi
