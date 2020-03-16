pres_staging_table_drop = "DROP TABLE IF EXISTS pres_staging_table"
pres_fact_table_drop = "DROP TABLE IF EXISTS pres_fact_table"
gp_pracs_staging_table_drop = "DROP TABLE IF EXISTS gp_pracs_staging_table"
gp_pracs_dim_table_drop = "DROP TABLE IF EXISTS gp_pracs_dim_table"
bnf_info_staging_table_drop = "DROP TABLE IF EXISTS bnf_info_staging_table"
bnf_info_dim_table_drop = "DROP TABLE IF EXISTS bnf_info_dim_table"
postcode_info_staging_table_drop = "DROP TABLE IF EXISTS postcode_info_staging_table"

pres_staging_table_create = ("""
CREATE TABLE IF NOT EXISTS pres_staging_table (
pres_id serial PRIMARY KEY,
sha text NOT NULL,
pct text NOT NULL,
practice_id text NOT NULL,
bnf_code text NOT NULL,
bnf_name text NOT NULL,
items int NOT NULL,
nic float NOT NULL,
act_cost float NOT NULL,
quantity float NOT NULL,
time_period text NOT NULL
);
""")

pres_fact_table_create = ("""
CREATE TABLE IF NOT EXISTS pres_fact_table (
pres_id serial PRIMARY KEY,
practice_id text NOT NULL,
bnf_code text NOT NULL,
items int NOT NULL,
nic float NOT NULL,
act_cost float NOT NULL,
quantity float NOT NULL,
month int NOT NULL CHECK (month BETWEEN 1 AND 12),
year int NOT NULL CHECK (year BETWEEN 1970 AND 2050)
);
""")

gp_pracs_staging_table_create = ("""
CREATE TABLE IF NOT EXISTS gp_pracs_staging_table (
gp_prac_id text PRIMARY KEY,
time_period int NOT NULL,
addr1 text NOT NULL,
addr2 text,
addr3 text,
addr4 text,
addr5 text,
postcode text
);
""")

gp_pracs_dim_table_create = ("""
CREATE TABLE IF NOT EXISTS gp_pracs_dim_table (
gp_prac_id text PRIMARY KEY,
name text NOT NULL,
postcode text,
county text,
region text,
longitude float,
latitude float
);
""")

bnf_info_staging_table_create = ("""
CREATE TABLE IF NOT EXISTS bnf_info_staging_table (
bnf_chapter text,
bnf_chapter_code text,
bnf_section text,
bnf_section_code text,
bnf_paragraph text,
bnf_paragraph_code text,
bnf_subparagraph text,
bnf_subparagraph_code text,
bnf_chemical_sub text,
bnf_chemical_sub_code text,
bnf_product text,
bnf_product_code text,
bnf_presentation text,
bnf_presentation_code text PRIMARY KEY
);
""")

bnf_info_dim_table_create = ("""
CREATE TABLE IF NOT EXISTS bnf_info_dim_table (
bnf_chapter text NOT NULL,
bnf_section text NOT NULL,
bnf_paragraph text,
bnf_subparagraph text,
bnf_chemical_sub text,
bnf_product text,
bnf_presentation text,
bnf_code text PRIMARY KEY
);
""")

postcode_info_staging_table_create = ("""
CREATE TABLE IF NOT EXISTS postcode_info_staging_table (
postcode text PRIMARY KEY,
county text,
region text,
longitude float,
latitude float
);
""")

pres_staging_table_populate = ("""
COPY pres_staging_table(sha, pct, practice_id, bnf_code, bnf_name, items, nic, act_cost, quantity, time_period)
FROM '/home/T201911PDPI_BNFT.csv'
DELIMITER ',' 
CSV HEADER;
""")

gp_prac_staging_table_populate = ("""
COPY gp_pracs_staging_table(time_period, gp_prac_id, addr1, addr2, addr3, addr4, addr5, postcode)
FROM '/home/T201911ADDR_BNFT.csv'
DELIMITER ',';
""")

bnf_info_staging_table_populate = ("""
COPY bnf_info_staging_table(
bnf_chapter,
bnf_chapter_code,
bnf_section,
bnf_section_code,
bnf_paragraph,
bnf_paragraph_code,
bnf_subparagraph,
bnf_subparagraph_code,
bnf_chemical_sub,
bnf_chemical_sub_code,
bnf_product,
bnf_product_code,
bnf_presentation,
bnf_presentation_code)
FROM '/home/BNF_Code_Information.csv'
DELIMITER ',' 
CSV HEADER;
""")

postcode_info_staging_table_populate = ("""
COPY postcode_info_staging_table(postcode, region, county, longitude, latitude)
FROM '/home/postcode_info.csv'
DELIMITER ',' 
CSV HEADER;
""")

pres_fact_table_insert = ("""
INSERT INTO pres_fact_table
(practice_id, bnf_code, items, nic, act_cost, quantity, month, year)
SELECT practice_id, bnf_code, items, nic, act_cost, quantity, 
CAST(RIGHT(time_period, 2) AS int), 
CAST(LEFT(time_period, 4) AS int)
FROM pres_staging_table;
""")

gp_pracs_dim_table_insert = ("""
INSERT INTO gp_pracs_dim_table
(gp_prac_id, name, postcode, region, county, longitude, latitude)
SELECT gp.gp_prac_id, gp.addr1, gp.postcode, pc.region, pc.county, pc.longitude, pc.latitude
FROM gp_pracs_staging_table gp
LEFT JOIN postcode_info_staging_table pc
ON(gp.postcode = pc.postcode);
""")

bnf_info_dim_table_insert = ("""
INSERT INTO bnf_info_dim_table
(bnf_chapter, bnf_section, bnf_paragraph, bnf_subparagraph, bnf_chemical_sub, bnf_product, bnf_presentation, bnf_code)
SELECT bnf_chapter, bnf_section, bnf_paragraph, bnf_subparagraph, bnf_chemical_sub, bnf_product, bnf_presentation, bnf_presentation_code
FROM bnf_info_staging_table;
""")

quality_tests = [
    {'sql': "SELECT COUNT(*) FROM pres_fact_table", 'shouldBe': '>', 'value': 0},
    {'sql': "SELECT COUNT(*) FROM gp_pracs_dim_table", 'shouldBe': '>', 'value': 0},
    {'sql': "SELECT COUNT(*) FROM bnf_info_dim_table", 'shouldBe': '>', 'value': 0},
    {'sql': "SELECT COUNT(*) FROM pres_fact_table WHERE (pres_id is null)", 'shouldBe': '==', 'value': 0},
    {'sql': "SELECT COUNT(*) FROM pres_fact_table WHERE (practice_id is null)", 'shouldBe': '==', 'value': 0},
    {'sql': "SELECT COUNT(*) FROM pres_fact_table WHERE (bnf_code is null)", 'shouldBe': '==', 'value': 0},
    {'sql': "SELECT COUNT(*) FROM gp_pracs_dim_table WHERE (gp_prac_id is null)", 'shouldBe': '==', 'value': 0},
    {'sql': "SELECT COUNT(*) FROM bnf_info_dim_table WHERE (bnf_code is null)", 'shouldBe': '==', 'value': 0},
    {'sql': "SELECT COUNT(*) FROM bnf_info_dim_table WHERE (bnf_chapter is null)", 'shouldBe': '==', 'value': 0},    
    ]

select_from_table = ("""
SELECT *
FROM {table}
LIMIT 3;
""")

drop_all_tables = [pres_staging_table_drop, pres_fact_table_drop, gp_pracs_staging_table_drop, gp_pracs_dim_table_drop,bnf_info_staging_table_drop, bnf_info_dim_table_drop, postcode_info_staging_table_drop]

create_all_tables = [pres_staging_table_create, pres_fact_table_create, gp_pracs_staging_table_create, gp_pracs_dim_table_create, bnf_info_staging_table_create, bnf_info_dim_table_create, postcode_info_staging_table_create]

populate_all_staging_tables = [pres_staging_table_populate, gp_prac_staging_table_populate, bnf_info_staging_table_populate, postcode_info_staging_table_populate]

insert_all_warehouse_tables = [pres_fact_table_insert, gp_pracs_dim_table_insert, bnf_info_dim_table_insert]

all_table_names = ['pres_staging_table','gp_pracs_staging_table', 'bnf_info_staging_table', 'pres_fact_table', 'gp_pracs_dim_table', 'bnf_info_dim_table', 'postcode_info_staging_table']