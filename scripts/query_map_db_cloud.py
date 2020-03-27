import psycopg2
import pandas as pd
import configparser
import os
import boto3
import glob
from sql_queries_cloud import (months, years, meds, q, q_all_meds, q_all_year, q_all_med_all_year)

def get_db_info(cur):
    cur.execute('SELECT version()')
    version = cur.fetchone()[0]
    print("Connected to database:")
    print(version)

def connect_db_redshift(config):
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    return conn, cur

def run_query_and_save_to_s3(month, year, med_num,med,conn,cur):
    if med == 'All' and month == 'All':
        query = q_all_med_all_year.format(year=year)
    elif med == 'All' and not month == 'All':
        query = q_all_med.format(year=year,month=month)
    elif not med == 'All' and month == 'All':
        query = q_all_year.format(med=med,year=year)
    else:
        query = q.format(med=med,year=year,month=month)
    query_final = """
            UNLOAD('{query}')
            TO 's3://prescribing-data/unload/{year}/{month}/{med_num}/'
            IAM_ROLE '{iam_role}'
            CSV
            HEADER
            ALLOWOVERWRITE;
            """.format(query=query,year=year,month=month,med_num=i,iam_role=config['CLUSTER']['IAM_ROLE'])
    cur.execute(query_final)

def download_results_from_s3(month, year, med_num,med,s3):
    directory = '../visualisation_web_app/data_cloud/{year}/{month}/{med_num}'.format(year=year,month=month,med_num=i)
    if not os.path.exists(directory):
        os.makedirs(directory)
    files = s3.list_objects(Bucket = 'prescribing-data', Prefix='unload/{year}/{month}/{med_num}'.format(year=year,month=month,med_num=i))
    for j, item in enumerate(files['Contents']):
        s3.download_file('prescribing-data',item['Key'],directory+'/'+str(j)+'.csv')

def process_downloaded_files(month, year, med_num):
    paths = '../visualisation_web_app/data_cloud/{year}/{month}/{med_num}/*.csv'.format(year=year,month=month,med_num=i)
    files = glob.glob(paths)
    df_concat = pd.concat([pd.read_csv(f) for f in files])
    df_concat.to_csv('../visualisation_web_app/data_cloud/{year}/{month}/{med_num}datafile.csv'.format(year=year,month=month,med_num=i),index=False)

def clean_up(month, year, med_num):
    file_paths = '../visualisation_web_app/data_cloud/{year}/{month}/{med_num}/*.csv'.format(year=year,month=month,med_num=i)
    dir_path = '../visualisation_web_app/data_cloud/{year}/{month}/{med_num}'.format(year=year,month=month,med_num=i)
    files = glob.glob(file_paths)
    for f in files:
        os.remove(f)
    os.rmdir(dir_path)


def main():
    config = configparser.ConfigParser()
    config.read('./config.cfg')
    conn, cur = connect_db_redshift(config)
    get_db_info(cur)

    print("Running queries and saving to s3...")
    for year in years:
        for month in months:
            for i, med in enumerate(meds):
                run_query_and_save_to_s3(month, year,i,med,conn,cur)
    cur.close()
    conn.close()

    print("Downloading query results from s3...")
    s3 = boto3.client('s3')
    for year in years:
        for month in months:
            for i, med in enumerate(meds):
                download_results_from_s3(month, year,i,med,s3)
    
    print("Processing downloaded files...")
    for year in years:
        for month in months:
            for i, med in enumerate(meds):
                process_downloaded_files(month, year,i)
    
    print("Cleaning up files...")
    for year in years:
        for month in months:
            for i, med in enumerate(meds):
                clean_up(month, year,i)

if __name__ == "__main__":
    main()