md1 = """
# What is this?

&nbsp;

This is a visualisation of patterns of GP prescribing across England. Different types of medication can be displayed - you can select the type of medication using the dropdown menu in the top left. The "total cost" to the NHS (in GBP) of medication prescribed by a practice within a medication category was used as a summary statistic of the "amount" prescribed.

At the moment this *only* displays the pattern from prescriptions in 2019. The plan is to extend it to display any pattern from the past 10 years in due course. 

&nbsp;

# Where did the data come from?

&nbsp;

The majority of the data comes from a large volume (~100GB) of anonymised and aggregated GP prescription records which have been released publicly by [NHS digital](https://digital.nhs.uk). The data can be downloaded from their website [here](https://digital.nhs.uk/data-and-information/publications/statistical/practice-level-prescribing-data) and a detailed description of what the data contains can be found [here](https://digital.nhs.uk/data-and-information/areas-of-interest/prescribing/practice-level-prescribing-in-england-a-summary/practice-level-prescribing-data-more-information). This public sector information is published and made available under the [Open Government Licence v3.0](http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).

The prescribed medication in this dataset is referenced with a code used by the British National Formulary (BNF). An extra dataset (~19MB) providing more information and categorisation of each of these medications from the BNF was downloaded from the NHS Business Services Authority website ([here](https://apps.nhsbsa.nhs.uk/infosystems/data/showDataSelector.do?reportId=126)).

Finally, a free and open source API called [Postcodes.io](https://postcodes.io) was used to obtain latitude and longitude coordinates (as well as other location metadata) of the GP practices in the dataset to make plotting them easier.

&nbsp;

# How was it made?

&nbsp;

 This web app was built using **Flask**, **Plotly Dash** and **Mapbox** and is currently hosted on DigitalOcean. There is a very large volume of data to process in order to generate these visualisations. The data pipeline therefore presented an interesting engineering problem. The partially cloud-based architecture I settled on is outlined in the schematic below. The ETL data pipeline was built using **Postgres**, **Apache Airflow**, **AWS Redshift**, and **S3**.
 """

md2 = """
Essentially the data is transformed into a useful schema and loaded into an AWS Redshift data warehouse. Once this has been done it is simple to run any SQL query you like against the tables in Redshift. The visualisations being demonstrated above were created by running and caching queries related to the amount of medication within a certain category being prescribed in GP practices across England. The various ETL steps are joined together in a DAG and orchestrated with Apache Airflow.

&nbsp;

# Can I see the code?

&nbsp;

Yup. It's kept in my GitHub repo [here](https://github.com/ofbennett/nhs-prescribing-etl-pipeline). The ETL pipeline can be either run locally with a small sample dataset or run using AWS resources with a large or full dataset. **NB: Running the pipeline on your AWS account will cost money!** Feel free to raise issues or contribute.

&nbsp;

# Who am I?

&nbsp;

I'm Oscar. I'm a data scientist and software engineer with a particular focus on biomedical and healthcare applications. If you're interested you can checkout my [blog](https://www.ofbennett.dev), my [Github](https://github.com/ofbennett), or my [LinkedIn](https://www.linkedin.com/in/oscar-bennett/).

"""
