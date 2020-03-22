# Medical Prescribing Patterns Across NHS GP Practices

## Overview
This is a cloud based ETL data pipeline which feeds into a web app visualisation (currently hosted [here](www.talktin.com)). The goals are:
- To demonstrate patterns of prescribing across all the GP practices in England
- To provide a way to populate a cloud-based data warehouse in order to make it convenient to run any query against this data
- Demontrate how the use of modern data engineering tools and cloud based architecture makes it possible to do interesting things with health-related Big Data

## The Architecture

<p align="center"><img src="./resources/architecture.png" width="850"></p>

The ETL data pipeline was built using Postgres, Apache Airflow, AWS Redshift, and S3. This web app was built using Flask, Plotly Dash and Mapbox and is currently hosted on DigitalOcean [here](www.talktin.com). Throughout I make extensive use of Docker and Docker-Compose to manage the various deployment environments for the different tools and databases.

As shown, the data is trasformed into a useful schema and loaded into an AWS Redshift data warehouse. Once this has been done it is simple to run any SQL query you like against the tables in Redshift. The visualisation in the web app was created by running a query related to the amount of medication within a certain category being prescribed in all the GP practices across England. The various ETL steps are joined together in a DAG and orchestrated with Apache Airflow.

## The ETL Pipeline

- CSV files from NHS digital and the BNF are downloaded and transfered to the AWS S3 data lake
- CSV file from NHS digital containing GP practice details downloaded locally
- A Python script obtains location metadata for each GP practice via an API (https://postcodes.io)
- These API responses are converted into a CSV file and uploaded to the data lake
- **Data from the data lake is copied into AWS Redshift staging tables**
- **Data from these staging tables are transformed and loaded into a data warehouse table schema (see below)**
- **Automated data quality checks are run to ensure the integrity of the resulting data**
- A local Python script runs a series of queries against the data warehouse and saves the results as CSV files in an S3 bucket and locally
- These results are sent to the web app which generates a visualisation

Apache Airflow is used to schedule and orchestrate the steps in **bold**. The dependancy DAG is illustrated here:

<p align="center"><img src="./resources/airflow.png" width="900"></p>

## The Data Sources

The majority of the data comes from a large volume (~100GB) of anonymised GP prescription records which have been released publicly by [NHS digital](https://digital.nhs.uk). The data can be downloaded from their website [here](https://digital.nhs.uk/data-and-information/publications/statistical/practice-level-prescribing-data) and a detailed description of what the data contains can be found [here](https://digital.nhs.uk/data-and-information/areas-of-interest/prescribing/practice-level-prescribing-in-england-a-summary/practice-level-prescribing-data-more-information). This public sector information is published and made available under the [Open Government Licence v3.0](http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/). Prescription data from each month consists over a little over 9 million rows.

The prescribed medication in this dataset is referenced with a code used by the British National Formulary (BNF). An extra dataset (~19MB) providing more information and categorisation of each of these medications from the BNF was downloaded from the NHS Business Services Authority website ([here](https://apps.nhsbsa.nhs.uk/infosystems/data/showDataSelector.do?reportId=126)).

Finally, a free and open source API called [Postcodes.io](https://postcodes.io) was used to obtain latitude and longitude coordinates (as well as other location metadata) of the GP practices in the dataset to make plotting them easier.

## The Data Warehouse Schema

The schema used in the data warehouse is demonstrated in this diagram:

<p align="center"><img src="./resources/schema.png" width="800"></p>

More information about the meaning of these table columns can be found in the [data dictionary](./resources/data_dict.md).

## Data Quality Checks

Data quality checks are carried out by a custom airflow operator. They carry out a series of basic checks to ensure that the pipeline ran correctly. For example, there are checks to ensure rows are present in the fact and dimention tables and that certain columns do not contain any null values. 

## Alternative Data Scenarios

- **Data increased by x100**

If the amount of data involved were x100 bigger there are a number of possible strategies that could be used to cope gracefully with this change. 

Depending on the types of queries expected to be run, it may be possible to store aggregations of the data in the warehouse rather than storing it in a mostly unaggregated form. 

The Redshift cluster would likely need to scale out with more nodes. The distribution of data across these nodes would need to be carefully thought about to improve the efficiency of expected queries. Using DISTKEYs and SORTKEYs it should be possible to limit the amount of internode network communication necessary to run analytics. 

The E and T steps of the ETL pipeline could be taken over by an AWS EMR cluster running Apache Spark. This type of approach would take advantage of the massive parallelism achievable with a Spark cluster to speed up the whole pipeline. This would be especially helpful if aggregations needed to be computed as part of the adapted ETL pipeline.

- **Pipeline needs to be run every morning at 7am**

Apache Airflow is designed with this requirement in mind. The ETL DAG could easily be scheduled to run once a day at 7am UTC. Email notifications could be set up to provide alerts of problems with the automated pipeline runs.

- **Database needs to be accessed by 100+ people**

Redshift is well setup to meet this need. The cluster can scale out to meet almost any level of concurrent querying using a feature called concurrency scaling. With this enabled the Redshift warehouse will add resources to match the level of concurrency as it increases or decreases.

## The Visualisation Web App

<p align="center"><img src="./resources/map_viz.png" width="800"></p>

This is a visualisation of patterns of GP prescribing across England. Different types of medication can be displayed - you can select the type of medication using the dropdown menu in the top left. The "total cost" to the NHS of medication prescribed by a practice within a medication category was used as a summary statistic of the "amount" prescribed.

This web app was built using Flask, Plotly Dash and Mapbox and is currently hosted on a DigitalOcean instance [here](www.talktin.com). A Gunicorn production server behind a Nginx reverse proxy was used to serve the app. The whole setup exists in two docker containers build and run with docker-compose.

In order to setup SSL/TSL and serve the app securely over https I setup a reasonably simple two step deployment process using [Let's Encrypt](https://letsencrypt.org) as a certificate authority. This process proceeded like this:
- Ran a simple Nginx server over http along with an installation of certbot (the official certbot docker image)
- Ran the certbot server challenge to obtain an SSL certificant for the chosen domain name
- Removed this simple server
- Finally installed and ran the whole app/gunicorn/nginx stack and ran this new server setup using these previously acquired SSL certificates

For details on how to run this process see below.

## How To Run the ETL Pipeline

There are two ways to run the ETL pipeline. You can either run it on your local machine using a modest subsample of the dataset, or you can run the full cloud-base ETL pipeline using any data sample size up to the full dataset. **NB Running the ETL pipeline on the cloud will cost you money!**

### How to run the ETL pipeline locally

This version of the pipeline runs ETL locally and sets up a data warehouse in a local installation of Postgres (instead of Redshift). 

### How to run the ETL pipeline in the cloud (AWS)



## How To Run the Web App

## Next Steps
