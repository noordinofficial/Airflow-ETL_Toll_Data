# README.md

# About

The project aims to de-congest the national highways by analyzing the road traffic from different toll plazas. Each highway is operated by a different toll operator with a different IT setup that uses different file formats. My job is to collect data available in different formats and consolidate it into a single file.

## Objectives

In this project I will author an Apache Airflow DAG that will:

- Extract data from a CSV file.
- Extract data from a tsv file.
- Extract data from a fixed width file.
- Transform the data.
- Load the transformed data into the staging area.

### Task 1.1 - Define DAG arguments

| Parameter | Value |
| --- | --- |
| owner | Raphael Malims |
| start_date | today |
| email | raphaelmalimsj@gmail.com |
| email_on_failure | True |
| email_on_retry | True |
| retries | 1 |
| retry_delay | 5 minutes |

### Task 1.2 - Define the DAG

| Parameter | Value |
| --- | --- |
| DAG id | ETL_toll_data |
| Schedule | Daily once |
| defualt_args | default_args |
| description | Highway Toll Data Using Airflow |

### Task 1.3 - Task to download data

Create a task that will download the traffic data from data-source: [https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final Assignment/tolldata.tgz](https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz) and stored into the directory `~ Zen/Airflow-ETL_Toll_Data`

### Task 1.4 - Task to unzip data

Uncompress the downloaded data into the destination directory.`~/Zen/Airflow-ETL_Toll_Data` 

### Task 1.5 - Task to extract data from CSV file

This task should extract the fields `Rowid,Timestamp,Anonymized Vehicle number,vehicle type` from `vehicle-data.csv` and save them into a file name `csv_data.csv` .

### Task 1.6 - Task to extract data from tsv file

This task should extract the fields `Number of axles, Tollplaza id, Tollplaza code` from `tollplaza-data.tsv` file and save it into a file name `tsv_data.csv` .

### Task 1.7 - Task to extract data from fixed width file

This task should extract the fields `Type of Payment code,Vehicle Code` from the fixed width file `payment-data.txt` and save it into a file named `fixed_width_data.csv`.

### Task 1.8 - Task to Consolidate data extracted from previous tasks

This task should create a single csv file names `extracted-data.csv` by combining data from:

- csv_data.csv
- tsv_data.csv
- fixed_width_data.csv

The final csv file should use the fields in the order given below:

`Rowid`, `Timestamp`, `Anonymized Vehicle number`, `Vehicle type`, `Number of axles`, `Tollplaza id`, `Tollplaza code`, `Type of Payment code`, and `Vehicle Code` .

### Task 1.9 - Define the task pipeline

`unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data`