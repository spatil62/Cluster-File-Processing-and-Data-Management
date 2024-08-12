
### Part One

This part contains two sub-sections

#### First Section

The first part of the assignment you will be data engineering. You will be converting raw text records, parsing them and saving them in many different formats in our Minio Object storage system.

* Use the raw data set you were assigned
* Use these initial parametersfor each job:

* Create a PySpark application that will read the `30.txt` from the bucket into a DataFrame
  * Name the PySpark application `XYZ-minio-read-and-process-AA.py`
  * AA is the decade you are working on
* In the same PySpark application parse the raw datasource assigned into the assigned 5 outputs and name the results the same prefix as the source (20.txt becomes 20-csv, or 20-csv-lz4, or 20-single-part-csv for example).
  * csv
  * json
  * csv with lz4 compression
  * parquet
  * csv with a single partition (need to adjust filename to not overwrite the first csv requirement)

#### Second Section

You will continue your data engineering experience in needing to read Raw text file and convert them into CSV and Parquet files. 

* Create multiple appropriate PySpark files to do the reading
* Save them as CSV and Parquet files in your own assign Minio S3 bucket.
  * Use the same convention as the previous questuion
  * 20-csv
  * 20-parquet

### Part Two

This part you will read the datasets you created back into your PySpark application and create schemas where necessary.

* Create a single PySpark application
  * Name: `XYZ-minio-read.py` 
* Read your partitioned csv into a DataFrame named: `csvdf`
  * Create a schema based on the sample given in blackboard
* Show the first 10 records and print the schema
* Read your partitioned csv into a DataFrame named: `jsondf`
  * Create a schema based on the sample given in Blackboard
* Show the first 10 records and print the schema
* Read your partitioned csv into a DataFrame named: `parquetdf`
  * Show the first 10 records and print the schema
* Connect to the MariaDB server using the below details in your PySpark application


### Part-Three

In this section you will execute the same command 3 times and modify run time parameters and make note of the execution times and **explain** what the adjustments did. To do this create a PySpark application to read your prescribed decade .parquet file data and find all of the weather station IDs that have registered days (count) of visibility less than 200 per year.

Create a file named: `part-three-answer.md` and Using Markdown explain your answer in technical detail from the book. Note - relative statements: *its faster, its better, its slower* are not correct.

Using these parameters on a reading 50-parquet from your Minio bucket

* `--driver-memory`
* `--executor-memory`
* `--executor-cores`
* `--total-executor-cores`

* First run
  * `--driver-memory 2G --executor-memory 4G --executor-cores 1 --total-executor-cores 20`
  * Your Expectation:
  * Your results/runtime:
* Second run
  * `--driver-memory 10G --executor-memory 12G --executor-cores 2`
  * Your Expectation:
  * Your results/runtime:
* Third run
  * `--driver-memory 4G --executor-memory 4G --executor-cores 2 --total-executor-cores 40`
  * Your Expectation:
  * Your results/runtime:

### Part Four

This part you will do some basic analytics using the Spark SQL or the native PySpark libraries. You will use the assigned dataset that you have already processed into the parquet format (load the .parquet file).  Use the Spark optimizations discussed in the previous section to optimize task run time -- resist the temptation to assign all cluster resources - use the first run options as a baseline.  Using the 50.txt or 60.txt depending on your lastname for month of February in each year.

* Using date ranges, select all records for the month of February for each year in the decade, find the following
  * Count the number of records
  * Average air temperature for month of February 
  * Median air temperature for month of February
  * Standard Deviation of air temperature for month of February
  * You may have to add filters to remove records that have values that are legal but not real -- such as 9999
  * Find AVG air temperature per StationID in the month of February
  * Write these value out to a DataFrame and save this to a Parquet file you created in your bucket
    * Name the file `XYZ-part-four-answers-parquet`
    * Will have to construct a schema
    * May want to make use of temp tables to keep smaller sets of data in memory


