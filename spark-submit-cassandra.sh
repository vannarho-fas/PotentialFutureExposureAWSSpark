# Adjust environment variables as needed
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3
export PYSPARK_PYTHON=python3
export PYTHONPATH=/usr/bin/python3
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export LD_LIBRARY_PATH=/usr/lib/hadoop/lib/native
export SPARK_HOME=/usr/lib/spark

# submit Spark job - adjust s3 bucket, filenames and scenario variables as needed

spark-submit \
--deploy-mode client \
--packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.3 \
--conf spark.cassandra.connection.host=localhost \
--conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions \
/Users/forsmith/Documents/PotentialFutureExposureAWSSpark/pfe_scenarios.py 10 48 0.376739 0.0209835 \
/Users/forsmith/Documents/PotentialFutureExposureAWSSpark/1504_USD_LIB_SWAP_CURVE.csv \
/Users/forsmith/Documents/PotentialFutureExposureAWSSpark/1504_EUR_LIB_SWAP_CURVE.csv \
/Users/forsmith/Documents/PotentialFutureExposureAWSSpark/1504_USD3MTD156N.csv \
/Users/forsmith/Documents/PotentialFutureExposureAWSSpark/1504_INSTRUMENTS.csv \
/Users/forsmith/Documents/PotentialFutureExposureAWSSpark/output090720