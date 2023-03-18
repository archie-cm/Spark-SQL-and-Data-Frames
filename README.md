# Apache Spark, PySpark, and SparkSQL

![image](https://user-images.githubusercontent.com/85284506/207533871-48de7265-8150-4dd8-9699-4f33b097ddfd.png)

+ Installing Apache Spark on Linux
+ Installing PySpark and Integrating with Jupyter Notebook
+ Using PySpark (SparkSQL) as Data Analytics Framework

# Installing Apache Spark on Linux Ubuntu

## Installing Java

Download OpenJDK 11 or Oracle JDK 11 (It's important that the version is 11 - spark requires 8 or 11)

We'll use [OpenJDK](https://jdk.java.net/archive/)

Download it (e.g. to `~/spark`):

```
wget https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz
```

Unpack it:

```bash
tar xzfv openjdk-11.0.2_linux-x64_bin.tar.gz
```

define `JAVA_HOME` and add it to `PATH`:

```bash
export JAVA_HOME="${HOME}/spark/jdk-11.0.2"
export PATH="${JAVA_HOME}/bin:${PATH}"
```

check that it works:

```bash
java --version
```

Output:

```
openjdk 11.0.2 2019-01-15
OpenJDK Runtime Environment 18.9 (build 11.0.2+9)
OpenJDK 64-Bit Server VM 18.9 (build 11.0.2+9, mixed mode)
```

Remove the archive:

```bash
rm openjdk-11.0.2_linux-x64_bin.tar.gz
```

## Installing Spark

Download Spark. Use 3.3.0 version:

```bash
wget https://dlcdn.apache.org/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz
```

Unpack:

```bash
tar xzfv spark-3.3.0-bin-hadoop3.tgz
```

Remove the archive:

```bash
rm spark-3.3.0-bin-hadoop3.tgz
```

Add it to `PATH`:

```bash
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PYSPARK_PYTHON=/usr/bin/python3
```

### Testing Spark

Execute `spark-shell` and run the following:

```scala
val data = 1 to 10000
val distData = sc.parallelize(data)
distData.filter(_ < 10).collect()
```
### Screenshots
![spark-cli]![image](https://user-images.githubusercontent.com/108534539/226096370-1e2c870b-31e2-4327-b64d-8292754c7264.png)
## PySpark

### Integrating PySpark with Jupyter Notebook
The only requirement to get the Jupyter Notebook reference PySpark is to add the following environmental variables in your .bashrc or .zshrc file, which points PySpark to Jupyter.

```bash
export PYSPARK_DRIVER_PYTHON='jupyter'
export PYSPARK_DRIVER_PYTHON_OPTS='notebook --no-browser --port=8889'
```
The PYSPARK_DRIVER_PYTHON points to Jupiter, while the PYSPARK_DRIVER_PYTHON_OPTS defines the options to be used when starting the notebook. In this case, it indicates the no-browser option and the port 8889 for the web interface.

Now, we can directly launch a Jupyter Notebook instance by running the pyspark command in the terminal.

`$ pyspark`

![pyspark](https://user-images.githubusercontent.com/85284506/206484629-57b13b6e-84e8-4d46-a6cf-e26d8c93c061.jpg)

To install findspark just type:

`$ pip install findspark`

And then on Jupyter Notebook to initialize PySpark, run:

```python
import findspark
findspark.init()
import pyspark
sc = SparkContext.getOrCreate();
```

To print the Spark Version, run:

```python
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('myapp') \
    .getOrCreate()
    
def main():
    print(f'Spark Version:, {spark.version}')
    
if __name__ == '__main__':
    main()
```
![spark vesion](https://user-images.githubusercontent.com/85284506/206486380-c078386a-97da-474f-a670-423b35136e54.jpg)

# PySpark for Data Analytics:
Datasets:
February 2021 data from TLC Trip Record website (https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

+ How many taxi trips were there on February 15?
+ Find the longest trip for each day!
+ Find Top 5 Most frequent `dispatching_base_num`!
+ Find Top 5 Most common location pairs (PUlocationID and DOlocationID)!


## SparkSQL

### How many taxi trips were there on February 15?
```sql
total_taxi_trip_0215 = spark.sql(""" 

    SELECT COUNT(pickup_datetime) AS total_taxi_trip_0215
    FROM data_table
    WHERE pickup_datetime >= '2021-02-15 00:00:00' AND pickup_datetime < '2021-02-16 00:00:00'

""")

total_taxi_trip_0215.show()
```
![image](https://user-images.githubusercontent.com/108534539/226107827-79dbb310-a946-4ddf-842c-9e539ed4f178.png)

## Find the longest trip for each day!
```python
longesttrip_eachday = df.withColumn("pickup_datetime" , to_date(df['pickup_datetime']))\
                      .select(['pickup_datetime','trip_miles'])\
                      .where("pickup_datetime >= '2021-02-01' ")\
                      .groupby(F.col('pickup_datetime')).agg(F.max('trip_miles').alias('longest_trip')).sort(desc("longest_trip"))
longesttrip_eachday.show(10)
```
![image](https://user-images.githubusercontent.com/108534539/226107854-25c1bb38-2e74-4a00-adef-27dd7666d35b.png)

### Find Top 5 Most frequent dispatching_base_num!

```python
top5_frequent_dbm = df.groupBy("dispatching_base_num").count() \
                    .orderBy(F.col('count').desc())

top5_frequent_dbm.show(5)
""")
```

![image](https://user-images.githubusercontent.com/108534539/226107893-a04381fc-0fa7-492c-b298-df63a1cfd6ff.png)

### Find Top 5 Most common location pairs (PUlocationID and DOlocationID)!
```python
top5_location_pairs = df.where("PUlocationID IS NOT NULL AND DOlocationID IS NOT NULL") \
                      .groupBy(["PUlocationID",'DOlocationID']) \
                      .count() \
                      .orderBy(F.col('count').desc())
top5_location_pairs.show(5)
;
""")
```
![image](https://user-images.githubusercontent.com/108534539/226107899-81b80505-fbf8-41f0-b969-39fd614f3479.png)

### Write all of the result to BigQuery table.
```python
df_total_taxi_trip_0215 = total_taxi_trip_0215.toPandas()
df_longesttrip_eachday = longesttrip_eachday.toPandas()
df_top5_frequent_dbm = top5_frequent_dbm.toPandas()
df_top5_location_pairs = top5_location_pairs.toPandas()

def load_to_bq(df, table_name):
    pandas_gbq.to_gbq(df, f'{dataset_id}.{table_name}', project_id=project_id)

load_to_bq(df_total_taxi_trip_0215, 'taxi_trip_0215')
load_to_bq(df_longesttrip_eachday, 'longesttrip_eachday')
load_to_bq(df_top5_frequent_dbm, 'top5_frequent_dbm')
load_to_bq(df_top5_location_pairs, 'top5_location_pairs')
""")
```
![image](https://user-images.githubusercontent.com/108534539/226108128-f7562391-2dcd-4f0f-a1e5-68418c98274c.png)

![image](https://user-images.githubusercontent.com/108534539/226108184-23b60bcc-dbce-4ed0-9d43-56346334b893.png)

![image](https://user-images.githubusercontent.com/108534539/226095976-c96b4943-cf09-4f99-b8e8-f29eccfa1f45.png)

