import psycopg2
import pandas as pd
import configparser
import os

meds = ['All','Antibacterial Drugs','Antiprotozoal Drugs','Diuretics', 'Beta-Adrenoceptor Blocking Drugs', 'Bronchodilators']

q = """
SELECT SUM(pre.nic) as total_cost, gp.name, gp.longitude, gp.latitude
FROM pres_fact_table pre
JOIN gp_pracs_dim_table gp
ON(pre.practice_id = gp.gp_prac_id)
JOIN bnf_info_dim_table bnf
ON(pre.bnf_code=bnf.bnf_code)
WHERE bnf.bnf_section='{}'
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

def connect_db_local():
    conn = psycopg2.connect("host=127.0.0.1 dbname=database user=user password=password")
    cur = conn.cursor()
    return conn, cur

def main():

    conn, cur = connect_db_local()
    get_db_info(cur)
    data_dir = './visualisation_web_app/data_local/2019/12/'
    if not os.path.isdir(data_dir):
        os.makedirs(data_dir)
   
    for i, med in enumerate(meds):
        outfile = data_dir+'{}datafile.csv'.format(i)
        if med == 'All':
            query = q_all
        else:
            query = q.format(med)
        cur.execute(query)
        outputquery = "COPY ({}) TO STDOUT WITH CSV HEADER".format(query)
        with open(outfile, 'w') as f:
            cur.copy_expert(outputquery, f)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()