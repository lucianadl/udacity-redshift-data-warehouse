# Udacity - Data Engineering Nanodegree
## Project: Data Warehouse


### Summary

The startup Sparkify has a music streaming app, and its data related to songs, artists and listening behavior resides in JSON files stored in Amazon S3. 

There are two main types of files:
* JSON logs on user activity on the app (`s3://udacity-dend/log_data`)
* JSON metadata on the songs available in the app (`s3://udacity-dend/song_data`)

The analytics team of the company is interested in moving their processes and Data Warehouse onto the cloud (Amazon Redshift) and import the data stored in the JSON files into that database.

The purpose of this project is building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional and fact tables, enabling Sparkify's analytics team to continue finding insights in what songs their users are listening to.

### Data model

Since the database for this project is meant to have an optimal performance for complex analytic queries, the schema chosen for the data model was the **star schema**. 

The dimension tables defined for the schema are the attributes that the analytics team is interested to understand in user activity: the actual *users*, the *songs* listened to by the users, the *artists* of those songs, and the *time* identifying when the songs started to be listened to. The fact table was defined to be the actual *songplays* registered by the app.

### Configuration file

It is assumed that the Redshift Cluster and the IAM Role were already created and are currently available.

The configuration parameters of this project are defined in the file `dwh.cfg`. Before running any script, ensure that the Redshift database settings and the IAM Role are correctly set in this file.

```
[CLUSTER]
HOST=
DB_NAME=
DB_USER=
DB_PASSWORD=
DB_PORT=

[IAM_ROLE]
ARN=
```

### Creating the Data Warehouse

The creation of the database tables and ETL processes for each table were developed in **Python scripts**.

The script `create_tables.py` creates the database schema `dwh` and all the tables (staging, dimension and fact). The `CREATE` and `DROP` statements called by that script are defined in the module `sql_queries.py`. 

To proceed with the creation of the tables, run the following command in the terminal:

```
python create_tables.py
```

This command can be rerun as much as needed in order to reset the database schema, since the script `create_tables.py` also contains statements to drop both the tables and the database schema if they exist.

### Importing the data

Once the tables have been properly created, the database will be ready to be loaded with the data stored in the JSON files.

The ETL process is defined in the file `etl.py`. This script contains a number of Python functions that, together, load all the song and log files from the S3 and loads the data first in the staging tables, using COPY commands, and then in the dimension and fact tables of the Data Warehouse, using INSERT commands. More explanation about the ETL pipeline can be found in the docstring of each function defined in that script.

In order to import the data into the database, run the following command in the terminal:

```
python etl.py
```

On the Query Editor of the Redshift console, run the queries below to confirm the records were successfully inserted into each table:

```
select COUNT(*) from dwh.staging_events;
select COUNT(*) from dwh.staging_songs;
select COUNT(*) from dwh.users;
select COUNT(*) from dwh.songs;
select COUNT(*) from dwh.artists;
select COUNT(*) from dwh.time;
select COUNT(*) from dwh.songplays;
```