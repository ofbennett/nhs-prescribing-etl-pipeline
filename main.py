import psycopg2
import pandas as pd
from sql_queries import (drop_all_tables, 
                        create_all_tables, 
                        populate_all_staging_tables,
                        insert_all_warehouse_tables,
                        all_table_names,
                        select_from_table)

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

    for drop_table in drop_all_tables:
        cur.execute(drop_table)
        conn.commit()
    for create_table in create_all_tables:
        cur.execute(create_table)
        conn.commit()
    for populate_stage_table in populate_all_staging_tables:
        cur.execute(populate_stage_table)
        conn.commit()
    for insert_table in insert_all_warehouse_tables:
        cur.execute(insert_table)
        conn.commit()
    for table_name in all_table_names:
        cur.execute(select_from_table.format(table=table_name))
        [print(line) for line in cur.fetchall()]

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()