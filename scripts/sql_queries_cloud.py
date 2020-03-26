months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12','All']
years = ['2019']
meds = ['All','Antibacterial Drugs','Antiprotozoal Drugs','Diuretics', 'Beta-Adrenoceptor Blocking Drugs', 'Bronchodilators']

q = """
SELECT SUM(pre.nic) as total_cost, gp.name, gp.longitude, gp.latitude
FROM pres_fact_table pre
JOIN gp_pracs_dim_table gp
ON(pre.practice_id = gp.gp_prac_id)
JOIN bnf_info_dim_table bnf
ON(pre.bnf_code=bnf.bnf_code)
WHERE bnf.bnf_section=''{med}''
AND pre.month={month}
AND pre.year={year}
AND gp.longitude IS NOT NULL
GROUP BY gp.name, gp.longitude, gp.latitude
"""

q_all_meds = """
SELECT SUM(pre.nic) as total_cost, gp.name, gp.longitude, gp.latitude
FROM pres_fact_table pre
JOIN gp_pracs_dim_table gp
ON(pre.practice_id = gp.gp_prac_id)
JOIN bnf_info_dim_table bnf
ON(pre.bnf_code=bnf.bnf_code)
WHERE gp.longitude IS NOT NULL
AND pre.month={month}
AND pre.year={year}
GROUP BY gp.name, gp.longitude, gp.latitude
"""

q_all_year = """
SELECT SUM(pre.nic) as total_cost, gp.name, gp.longitude, gp.latitude
FROM pres_fact_table pre
JOIN gp_pracs_dim_table gp
ON(pre.practice_id = gp.gp_prac_id)
JOIN bnf_info_dim_table bnf
ON(pre.bnf_code=bnf.bnf_code)
WHERE bnf.bnf_section=''{med}''
AND pre.year={year}
AND gp.longitude IS NOT NULL
GROUP BY gp.name, gp.longitude, gp.latitude
"""

q_all_med_all_year = """
SELECT SUM(pre.nic) as total_cost, gp.name, gp.longitude, gp.latitude
FROM pres_fact_table pre
JOIN gp_pracs_dim_table gp
ON(pre.practice_id = gp.gp_prac_id)
JOIN bnf_info_dim_table bnf
ON(pre.bnf_code=bnf.bnf_code)
WHERE pre.year={year}
AND gp.longitude IS NOT NULL
GROUP BY gp.name, gp.longitude, gp.latitude
"""

q_all_prac = """
SELECT SUM(pre.nic) as total_cost
FROM pres_fact_table pre
JOIN bnf_info_dim_table bnf
ON(pre.bnf_code=bnf.bnf_code)
WHERE bnf.bnf_section=''{med}''
AND pre.month={month}
AND pre.year={year}
"""

