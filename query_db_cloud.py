import psycopg2
import pandas as pd
import configparser
import os
import boto3
import glob

meds = ['All','Antibacterial Drugs','Antiprotozoal Drugs','Diuretics', 'Beta-Adrenoceptor Blocking Drugs', 'Bronchodilators']

q = """
SELECT SUM(pre.nic) as total_cost, gp.name, gp.longitude, gp.latitude
FROM pres_fact_table pre
JOIN gp_pracs_dim_table gp
ON(pre.practice_id = gp.gp_prac_id)
JOIN bnf_info_dim_table bnf
ON(pre.bnf_code=bnf.bnf_code)
WHERE bnf.bnf_section=''{}''
AND gp.longitude IS NOT NULL
GROUP BY gp.name, gp.longitude, gp.latitude
"""

q_all = """
SELECT SUM(pre.nic) as total_cost, gp.name, gp.longitude, gp.latitude
FROM pres_fact_table pre
JOIN gp_pracs_dim_table gp
ON(pre.practice_id = gp.gp_prac_id)
JOIN bnf_info_dim_table bnf
ON(pre.bnf_code=bnf.bnf_code)
WHERE gp.longitude IS NOT NULL
GROUP BY gp.name, gp.longitude, gp.latitude
"""

def get_db_info(cur):
    cur.execute('SELECT version()')
    version = cur.fetchone()[0]
    print("Connected to database:")
    print(version)

def connect_db_redshift(config):
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    return conn, cur

def main():
    config = configparser.ConfigParser()
    config.read('./config.cfg')
    conn, cur = connect_db_redshift(config)
    get_db_info(cur)

    print("Running queries and saving to s3...")
    for i, med in enumerate(meds):
        if med == 'All':
            query = q_all
        else:
            query = q.format(med)
        query_final = """
                UNLOAD('{}')
                TO 's3://prescribing-data/unload/{}/'
                IAM_ROLE '{}'
                CSV
                HEADER
                ALLOWOVERWRITE;
                """.format(query,i,config['CLUSTER']['IAM_ROLE'])
        cur.execute(query_final)

    cur.close()
    conn.close()

    print("Downloading query results from s3...")
    s3 = boto3.client('s3')
    for i, med in enumerate(meds):
        directory = './visualisation_web_app/data_cloud/{}'.format(i)
        if not os.path.exists(directory):
            os.makedirs(directory)
        files = s3.list_objects(Bucket = 'prescribing-data', Prefix='unload/'+str(i)+'/')
        for j, item in enumerate(files['Contents']):
            s3.download_file('prescribing-data',item['Key'],directory+'/'+str(j)+'.csv')
    
    print("Processing downloaded files...")
    for i, med in enumerate(meds):
        paths = './visualisation_web_app/data_cloud/{}/*.csv'.format(i)
        files = glob.glob(paths)
        df_concat = pd.concat([pd.read_csv(f) for f in files])
        df_concat.to_csv('./visualisation_web_app/data_cloud/{}datafile.csv'.format(i),index=False)

if __name__ == "__main__":
    main()