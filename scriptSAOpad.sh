#!/bin/bash

RUNDATE=`date`
JOBID=Bebas

### LOCKING PROCESS

if [ -e /home/hdoop/filelock.lock ];then
	echo "Proses masih jalan"
	exit ####aplikasi keluar
else
	touch /home/hdoop/filelock.lock ##genrate filelock.lock

	PACKAGE=SAOPAD
	CLASS=Part1
	export NAMAJAR=/home/hdoop/SAOPAD_spark2.jar

	if [ -d /home/hdoop/HasilSA_OPAD ];then
		echo "Folder masih ada, hapus dulu ya"
		rm -r /home/hdoop/HasilSA_OPAD
	fi

	if [ -d /home/hdoop/HasilSA_OPADReject ];then
		echo "Folder masih ada, hapus dulu ya"
		rm -r /home/hdoop/HasilSA_OPADReject
	fi

	if [ -f /home/hdoop/filelogSAOPAD.csv ];then
		echo "Filelog masih ada, hapus dulu ya"
		rm /home/hdoop/filelogSAOPAD.csv
	fi

	spark-submit --class ${PACKAGE}.${CLASS} --master local[1] \
	${NAMAJAR} \
	file:///home/hdoop/datayunita/SA_OPAD/OPAD_JKT_Pre_20221220235959_00000534_51.1.dat \
	file:///home/hdoop/datayunita/SA_OPAD/LINEDIM \
	file:///home/hdoop/datayunita/SA_OPAD/IFRS \
	file:///home/hdoop/HasilSA_OPAD \
	file:///home/hdoop/HasilSA_OPADReject

	echo "Proses Selesai pada:" `date`

	counter=1
	getlog=-3
	while [ $counter -le 19 ]
	do
		
		LOGFILE=`ls -lrt ${PACKAGE}.${CLASS}*log | tail -1 | awk '{print $9}'`

		NAMATRANS=`cat ${LOGFILE} |awk -F"|" '{print $(NF-1)}' |awk '{gsub(";","\n")}1' | head $getlog | tail -1 | awk -F":" '{print $1}'`
		INPUTTRA=`cat ${LOGFILE} |awk -F"|" '{print $(NF-1)}' |awk '{gsub(";","\n")}1' | head $getlog  | tail -1 | awk -F":" '{print $3}'`
		OUTPUTTRA=`cat ${LOGFILE} |awk -F"|" '{print $(NF-1)}' |awk '{gsub(";","\n")}1' | head $getlog  | tail -1 | awk -F":" '{print $3}'`
		
		echo "$JOBID,$NAMATRANS,$INPUTTRA,$OUTPUTTRA" >> filelogSAOPAD.csv

		((counter++))
		((getlog--))
	
	done
	echo "succesfull create filelog"
	
	mysql -uhdoop -p1q2q3q4q -Dfull <<EOF

	 load data local infile '/home/hdoop/filelogSAOPAD.csv' into table logprocess_SAOPAD fields 
	 terminated by ',' 
	 enclosed by '"' 
	 lines terminated by '\n' 
	 (jobid,namatrans,input,output,created_date);
EOF
	cat HasilSA_OPAD/h* > HasilSA_OPAD.csv

mysql -uhdoop -p1q2q3q4q -Dfull <<EOF

	 load data local infile '/home/hdoop/HasilSA_OPAD.csv' into table SA_OPAD fields 
	 terminated by '~' 
	 enclosed by '"' 
	 lines terminated by '\n' 
	 (timestamp_r,trx_date,trx_hour,user_name,msisdn,order_id,main_plan_name,main_plan_id,topping_plan_name,topping_plan_id,price,brand,file_id,load_ts,load_user,offer_id,pre_post_flag,l1_payu,l2_service_type,l3_allowance_type,l4_product_category,l5_product,l1_name,l2_name,l3_name,l4_name,payment_channel);

EOF

	cat HasilSA_OPADReject/h* > HasilSA_OPADReject.csv

mysql -uhdoop -p1q2q3q4q -Dfull <<EOF

	 load data local infile '/home/hdoop/HasilSA_OPADReject.csv' into table SA_OPAD_Reject fields 
	 terminated by '~' 
	 enclosed by '"' 
	 lines terminated by '\n' 
	 (timestamp_r,trx_date,trx_hour,user_name,msisdn,order_id,main_plan_name,main_plan_id,topping_plan_name,topping_plan_id,price,brand,file_id,load_ts,load_user,offer_id,pre_post_flag,l1_payu,l2_service_type,l3_allowance_type,l4_product_category,l5_product,l1_name,l2_name,l3_name,l4_name,payment_channel);

EOF
	rm /home/hdoop/filelock.lock ##hapus filelock.lock
fi
