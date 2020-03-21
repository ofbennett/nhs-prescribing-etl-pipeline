# Medical Prescribing Patterns Across NHS GP Practices

## Overview
This is a cloud based ETL data pipeline which feeds into a web app visualisation (currently hosted [here](www.talktin.com)). The goals are:
- To demonstrate patterns of prescribing of different types of medication across all the GP practices in England
- To provide a way to populate a cloud-based data warehouse in order to make it convenient to run queries and answer any question you might care to ask
- Demontrate how the use of modern data engineering tools and cloud based architecture makes it possible to do interesting things with health-related "Big Data"

<p align="center"><img src="./resources/map_viz.png" width="800"></p>

## The Architecture

<p align="center"><img src="./resources/architecture.png" width="850"></p>

The ETL data pipeline was built using Postgres, Apache Airflow, AWS Redshift, and S3. This web app was built using Flask, Plotly Dash and Mapbox and is currently hosted on DigitalOcean [here](www.talktin.com). 

As shown, the data is trasformed into a useful schema and loaded into an AWS Redshift data warehouse. Once this has been done it is simple to run any SQL query you like against the tables Redshift. The visualisation in the web app was created by running a query related to the amount of medication within a certain category being prescribed in all the GP practices across England. The various ETL steps are joined together in a DAG and orchestrated with Apache Airflow.

## The ETL Pipeline

- CSV files from NHS digital and the BNF are downloaded and transfered to the AWS S3 data lake
- CSV file from NHS digital containing GP practice details downloaded locally
- A Python script obtains location metadata for each GP practice via an API (https://postcodes.io)
- These API responses are converted into a CSV file and uploaded to the data lake
- **Data from the data lake is copied into AWS Redshift staging tables**
- **Data from these staging tables are transformed and loaded into a data warehouse table schema (see below)**
- **Automated data quality checks are run to ensure the integrity of the resulting data**
- A local Python script is run which runs a series of queries against the data warehouse and saves the results as CSV files in an S3 bucket
- These results are sent to the web app which generates a visualisation

Apache Airflow is used to schedule and orchestrate the steps in **bold**. The dependancy DAG is illustrated here:

<p align="center"><img src="./resources/airflow.png" width="900"></p>

## The Data Sources

The majority of the data comes from a large volume (~100GB) of anonymised GP prescription records which have been released publicly by [NHS digital](https://digital.nhs.uk). The data can be downloaded from their website [here](https://digital.nhs.uk/data-and-information/publications/statistical/practice-level-prescribing-data) and a detailed description of what the data contains can be found [here](https://digital.nhs.uk/data-and-information/areas-of-interest/prescribing/practice-level-prescribing-in-england-a-summary/practice-level-prescribing-data-more-information). This public sector information is published and made available under the [Open Government Licence v3.0](http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).

The prescribed medication in this dataset is referenced with a code used by the British National Formulary (BNF). An extra dataset (~19MB) providing more information and categorisation of each of these medications from the BNF was downloaded from the NHS Business Services Authority website ([here](https://apps.nhsbsa.nhs.uk/infosystems/data/showDataSelector.do?reportId=126)).

Finally, a free and open source API called [Postcodes.io](https://postcodes.io) was used to obtain latitude and longitude coordinates (as well as other location metadata) of the GP practices in the dataset to make plotting them easier.

## The Data Warehouse Schema

The schema used in the data warehouse is demonstrated in this diagram:

<p align="center"><img src="./resources/schema.png" width="800"></p>

## Data Quality Checks

Data quality checks are carried out by a custom operator. These ensure that rows are present in the fact and dimention tables and that certain columns do not contain any null values. 

## Alternative Data Scenarios

- **Data increased by x100**

- **Pipeline needs to be run every morning at 7am**

- **Database needs to be accessed by 100+ people**

## The Visualisation Web App

## How To Run the ETL Pipeline

## How To Run the Web App

## Next Steps
