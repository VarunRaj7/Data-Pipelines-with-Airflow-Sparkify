### Data Pipelines with Airflow : Sparkify

Sparkify's, a music streaming company, data warehouse ETL pipelines in Apache Airflow to achieve automationa nd monitoring.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.


#### Datasets

For this project, you'll be working with two datasets. Here are the s3 links for each:

Log data: `s3://udacity-dend/log_data`

Song data: `s3://udacity-dend/song_data`

### Schema

Fact Table

`songplays` - records in event data associated with song plays i.e. records with page NextSong

* songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables

`users` - users in the app
* user_id, first_name, last_name, gender, level

`songs` - songs in music database
* song_id, title, artist_id, year, duration

`artists` - artists in music database
* artist_id, name, location, lattitude, longitude

`time` - timestamps of records in songplays broken down into specific units
* start_time, hour, day, week, month, year, weekday

<div align='center'>
<img src="/Images/schema.png" height="400" width="400">
</div>

#### Project Template

The project contains three major components for the project:

- The dag template has all the imports and task templates in place
- The operators folder with operator templates
- A helper class for the SQL transformations

#### DAG Configurations

In the DAG, add default parameters according to these guidelines

- The DAG does not have dependencies on past runs
- On failure, the task are retried 3 times
- Retries happen every 5 minutes
- Catchup is turned off
- Do not email on retry

#### DAG

![SparkifyDAG](/Images/Sparkify-DAG.png)

#### Trouble shoot Issues

The main issue one can encounter is the broken dag, this is mainly due to the errors in the operators or helpers or dag itself. These errors can be seen when you run `/opt/airflow/start.sh` or `python -c "from airflow.models import DagBag; d = DagBag();"`

Reference:[Medium link](https://towardsdatascience.com/5-essential-tips-when-using-apache-airflow-to-build-an-etl-pipeline-for-a-database-hosted-on-3d8fd0430acc)




