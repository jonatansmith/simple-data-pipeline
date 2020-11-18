# simple-data-pipeline
Simple data pipeline. Common Data Engineering task: Ingest RAW relational or non-relational data from CSV, JSON, log files and etc., prepare data and persist into Stage Area using Parquet columnar format 

To change Glue JOB, edit csvloader.py and update the stack using:
'''
sam.cmd build 
sam.cmd deploy --profile smith-personal
'''
