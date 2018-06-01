import numpy as np
import pandas as pd
import scipy.io as sio
import os

!wget http://csegroups.case.edu/sites/default/files/bearingdatacenter/files/Datafiles/97.mat
!wget http://csegroups.case.edu/sites/default/files/bearingdatacenter/files/Datafiles/98.mat
!wget http://csegroups.case.edu/sites/default/files/bearingdatacenter/files/Datafiles/99.mat
!wget http://csegroups.case.edu/sites/default/files/bearingdatacenter/files/Datafiles/100.mat

!mkdir  cwr_healthy
!mv *.mat cwr_healthy/


def read_folder(folder):
    data = 'dummy'
    skip = False
    for file in os.listdir(folder):
        file_id = file[:-4]
        mat_file_dict = sio.loadmat(folder+file)
        del data
        for key, value in mat_file_dict.items():
            if 'DE_time' in key or 'FE_time' in key:
                a = np.array(mat_file_dict[key])
                try:
                    data
                except NameError:
                    data = a
                else:
                    if (data.shape[0] != a.shape[0]):
                        print ('skipping ' + file_id)
                        skip = True
                        continue
                    data = np.hstack((data,a))
        if skip:
            skip=False
            continue
        id = np.repeat(file_id,data.shape[0])
        id.shape = (id.shape[0],1)
        data = np.hstack((id,data))
        if data.shape[1] == 2:
            zeros = np.repeat(float(0),data.shape[0])
            zeros.shape =(data.shape[0],1)
            data = np.hstack((data,zeros))
        try:
            result
        except NameError:
            result = data
        else:
            result = np.vstack((result,data))
    return result

    result_healthy = read_folder('./cwr_healthy/')
	pdf = pd.DataFrame(result_healthy)
	pdf.to_csv('result_healthy_pandas.csv', header=False, index=True)


	!for url in `curl -s csegroups.case.edu/bearingdatacenter/pages/12k-drive-end-bearing-fault-data |grep mat |grep http |awk -F'href="' '{print $2}' |awk -F'">' '{print $1}'`; do wget $url; done
	!for url in `curl -s csegroups.case.edu/bearingdatacenter/pages/48k-drive-end-bearing-fault-data |grep mat |grep http |awk -F'href="' '{print $2}' |awk -F'">' '{print $1}'`; do wget $url; done
	!for url in `curl -s csegroups.case.edu/bearingdatacenter/pages/12k-fan-end-bearing-fault-data |grep mat |grep http |awk -F'href="' '{print $2}' |awk -F'">' '{print $1}'`; do wget $url; done
	
	!mkdir cwr_faulty
	
	!mv *.mat cwr_faulty/
	
	result_faulty = read_folder('./cwr_faulty/')

	pdf = pd.DataFrame(result_faulty)

	pdf.to_csv('result_faulty_pandas.csv', header=False, index=True)

	df_healhty = spark.read.csv('result_healthy_pandas.csv')
	df_healhty.createOrReplaceTempView('df_healhty')

	spark.sql('select _c1,count(_c1) as cn from df_healhty group by _c1 order by cn asc').show()

	df_faulty = spark.read.csv('result_faulty_pandas.csv')
	df_faulty.createOrReplaceTempView('df_faulty')

	spark.sql('select _c1,count(_c1) as cn from df_faulty group by _c1 order by cn asc').show()

	import ibmos2spark

	# @hidden_cell
	credentials = {
	    'auth_url': 'https://identity.open.softlayer.com',
	    'project_id': '6aaf54352357483486ee2d4981f8ef15',
	    'region': 'dallas',
	    'user_id': 'e8a574f22ee84d29b7a1987b6103fced',
	    'username': 'member_adcb54bd899a7e39e31582bccad1577f68f1992f',
	    'password': 'P*/m8,!#7s6H9poz'
	}

	configuration_name = 'os_d3bd5b94a9334de59a55a7fed2bedeaa_configs'
	bmos = ibmos2spark.bluemix(sc, credentials, configuration_name)

	from pyspark.sql import SparkSession
	spark = SparkSession.builder.getOrCreate()

	df_healhty.write.parquet(bmos.url('courseraai', 'cwr_healthy1.parquet'))

	df_faulty.write.parquet(bmos.url('courseraai', 'cwr_faulty1.parquet'))

	df_faulty.count()
