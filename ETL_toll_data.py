from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator 
from airflow.utils.dates import days_ago

#Defining DAG arguments
default_args = {
    'owner':'Malims',
    'start_date': days_ago(0),
    'email':['raphaelmalimsj@gmail.com'],
    'email_on_failure': True,
    'email_on_retry':True,
    'reties':1,
    'retry_delay':timedelta(minutes=5),
}

#Define the DAG
dag = DAG(
    dag_id='ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Traffic Data',
    schedule_interval=timedelta(days=1),
)

#Define the tasks
#define the first task names unzip_data
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command = 'tar -zxvf  ~/Zen/Airflow-ETL_Toll_Data/tolldata.tgz',
    dag=dag,
)

#Task to extract data from csv file
extract_data_from_csv = BashOperator(
    task_id = 'extract_data_from_csv',
    bash_command = 'cut -d"," -f1,2,3,4 ~/Zen/Airflow-ETL_Toll_Data/vehicle-data.csv',
    dag=dag,
)

#Task to extract data from tsv file.
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -d$"\t" -f5,6,7 ~/Zen/Airflow-ETL_Toll_Data/tollplaza-data.tsv',
    dag=dag,
)

#Task to extract data from fixed width file.
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut -d " " -f 6-7 ~/Zen/Airflow-ETL_Toll_Data/payment-data.txt',
    dag=dag,
)

#Task to consolidate data
consolidate_data=BashOperator(
    task_id='consolidate_data',
    bash_command='paste vehicle-data.csv > tollplaza-data.tsv > payment-data.txt',
    dag=dag,
)


#Task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data 
