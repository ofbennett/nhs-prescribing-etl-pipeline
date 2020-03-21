# Data Dictionary for GP Prescribing Database Schema

## pres_fact_table
### A fact table containing the details of the amount and location of all the GP prescriptions across England

| Column | Description |
|--------|-------------|
| pres_id | PRIMARY KEY. The unique key for each prescription row |
| practice_id | The unique code for each GP practice |
| bnf_code | The unique code for each type of medication in the BNF |
| items | The number of individual 'units' of this type of prescription dispensed |
| nic | The net ingredient cost of the prescriptions to the NHS |
| act_cost | The actual cost of the prescriptions to the NHS taking into account certain extra costs and discounts |
| quantity | The quantity prescribed (in tables, or milliliters for example) |
| month | The month the prescription amounts are aggregated within |
| year | The year that the month is within |

## gp_pracs_dim_table
### A dimention table providing information about each GP practice in the prescription dataset. Includes location information obtained from the https://postcodes.io API.

| Column | Description |
|--------|-------------|
| gp_prac_id | PRIMARY KEY. The unique code used to identify each GP practice |
| name | The name of the practice. Taken to the be first line of the practice's address |
| postcode | The postcode of the practice |
| county | The county the practice is within |
| region | The region the practice is within |
| longitude | The longitude of the practice |
| latitude | The latitude of the practice |

## bnf_info_dim_table
### A dimention table providing information about the nature of each medication from the ontology provided by the British National Formulary.

| Column | Description |
|--------|-------------|
| bnf_chapter | The title of the BNF chapter containing the item |
| bnf_section | The title of the BNF section containing the item |
| bnf_paragraph | The title of the BNF paragraph containing the item |
| bnf_subparagraph | The title of the BNF subparagraph containing the item |
| bnf_chemical_sub | The name of the chemical substance which the medication is formed from |
| bnf_product | The item's product name |
| bnf_presentation | The full name of the item |
| bnf_code | PRIMARY KEY. The unique code for each type of medication in the BNF |