import psycopg2
import pandas as pd
import configparser
import os
import glob
from sql_queries_cloud import (months, years, meds, q_all_prac)

def get_db_info(cur):
    cur.execute('SELECT version()')
    version = cur.fetchone()[0]
    print("Connected to database:")
    print(version)

def connect_db_redshift(config):
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    return conn, cur

def run_query(month, year, med_num,med,conn,cur):
    query_final = q_all_prac.format(med=med,year=year,month=month)
    cur.execute(query_final)
    result = cur.fetchall()
    return result

def main():
    config = configparser.ConfigParser()
    config.read('./config.cfg')
    conn, cur = connect_db_redshift(config)
    get_db_info(cur)

    result_dict = {}
    print("Running queries...")
    for i, med in enumerate(meds):
        if med == 'All':
            continue
        result_dict[med] = {}
        for year in years:
            result_dict[med][year] = []
            for month in months:
                if month == 'All':
                    continue
                result = run_query(month,year,i,med,conn,cur)
                result_dict[med][year].append(result)
    cur.close()
    conn.close()
    print(result_dict)

if __name__ == "__main__":
    main()