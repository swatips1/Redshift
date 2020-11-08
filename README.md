Sparkify, a startup stramping app, wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. 
They would like to be able to analyze their streaming data, particularly what songs the users are listening to. They would like Amazon Web Services (AWS) to process the raw data stored in S3 buckets, with the warehousing handled by a redshift cluster. This will allow Sparkify analysis to quickly and easily run required queries without spending lot of time and resources in hardware and configuration.


The details of contents of this project are as below:
1) All the data used by this application can be found in S3 bucket, specifically:
    LOG_DATA='s3://udacity-dend/log_data'
    LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
    SONG_DATA='s3://udacity-dend/song_data'
2) sql_queries.py: This is a central place for all database queries needed for data processing, including creating and dropping tables and all queries needed for data manipulation.    
3) sql_queries.py: This is a central place for all database queries needed for data processing, including creating and dropping tables and all queries needed for data manipulation.
4) create_tables.py: This has the code for creating databases and tables. The main function re-creates database and tables. It uses sql_queries.py to drop and create required tables. 
5) etl.py: This is where the data is read from the S3 bucket into the staging tables and then from staging tables into the main tables.  
6) test.ipynb: Provides the developer an easy way to Calls create_tables.py and  etl.py and observe the results of each operation.

The DB Schema consists of the following:
1) Staging tables:
    a)events_staging: A staging table for temporary holding and/or preprocessing data coming from the 'log_data' S3 bucket.    
    b)songs_staging: A staging table for temporary holding and/or preprocessing data coming from the 'song_data' S3 bucket.
2) Final tables:
    a)users: List of all users of Sparkify.
    a)songs: List of all songs available for play in Sparkify.
    a)artists: List of all artists whose songs are available for play in Sparkify.
    a)times: A table that stores all unique times across all song played in Sparkify. It provides all aspects of time individually e.g. hour of play etc.
    a)songplays: The fact table supplies the metrics used for the song plays analytics.
    

To run:
The ETL process is quite simple to execute:
1) Launch a Redshit cluster in the same region as the S3 bucket. Make sure the IAM user and security group is propery configured and linked to the cluster.
2) Make sure all parameters in dwh.cfg are configured with valid information.
3) Run test.ipynb
    NOTE: It is best to make sure the Kernel is in proper state. One way would be to re-start the Kernel prior to repeate runs. 
4) Using Query Editor in the Redshift Cluster, run the following query:
    SELECT * FROM songplays LIMIT 2;
    
