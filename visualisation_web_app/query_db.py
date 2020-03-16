import psycopg2
import pandas as pd

meds = ['Antibacterial Drugs','Antiprotozoal Drugs','Diuretics', 'Beta-Adrenoceptor Blocking Drugs', 'Bronchodilators']

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

def get_db_info(cur):
    cur.execute('SELECT version()')
    version = cur.fetchone()[0]
    print("Connected to database:")
    print(version)

def connect_db():
    conn = psycopg2.connect("host=127.0.0.1 dbname=database user=user password=password")
    cur = conn.cursor()
    return conn, cur

def main():
    conn, cur = connect_db()
    get_db_info(cur)

    # cur.execute(q)
    # [print(line) for line in cur.fetchall()]    
   
    for i, med in enumerate(meds):
        outfile = './data/{}datafile.csv'.format(i)
        query = q.format(med)
        outputquery = "COPY ({}) TO STDOUT WITH CSV HEADER".format(query)
        with open(outfile, 'w') as f:
            cur.copy_expert(outputquery, f)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()