# Building a data warehouse with Amazon Web Services. 

## Introduction, Background
    This project consits in creating a data warehouse with Amazon Web Services. We are in the postion of a data engineer in a music streaming startup called Sparkify, who wants to move their processes and data in the cloud. We can find the original data in S3, a directory of JSON logs on user activity in the app, as well as JSON metadata files of the songs played. 

    We have to build a ETL pipeline that extracts the data from S3, stages it in Redshift and transforms the data into dimensional tables for analytics teams to gain insights in the data For this we choose a star schema with tables that gathers user, song, artist and timestamps, each in its own table, and then we would create a fact table that represents all the user plays in the application, connecting the other four tables together. 

## Schema:
For this project we will build a start schema optimized to run queries on song play analysis. The tables are as follow:

![schema.jpeg](https://github.com/Alex-Skp/data-warehouse-aws/blob/main/schema.jpeg?raw=true)

## Methodology:

### Creating a redshift cluster in AWS
The process to create and connect a cluster in redshift is in the jupyter notebook: creating_cluster.ipynb In This notebook retrieves credentials data from a configuration file which includes the credentials and secret key to my AWS account. It's very important to not include this file in the repository to avoid third parties access your infrastructure. After creating the cluster, we'll include the cluster connection details also in a secret config file for the scripts create_tables.py and etl.py to access. 

### Running the scripts
To reset our tables you must run create_tables.py. This script accesses the queries written in sql_queries.py in order to delete existing tables with the name, and create new ones. This script creates all tables from our star schema, as well as the ones used to import the log and song metadata from the S3 bucket. 

The other script to run is etl.py. This piece of code also access queries in sql_queries.py to connect and import the data into the staging tables, and then inserts this data, transformed by the insert statement, into our star schema. 

### Testing the database
In creating_cluster.ipynb there are cells dedicated to test the schema, as this would be our current channel to connect and retrieve data with our redshift cluster. 

