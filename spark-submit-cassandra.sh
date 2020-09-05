 # Adjust environment variables as needed
 # assumes Spark 3.0.0
 # for spark 2.4.5 --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.3

export PYSPARK_DRIVER_PYTHON=/usr/bin/python3
export PYSPARK_PYTHON=python3
export PYTHONPATH=/usr/bin/python3
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export LD_LIBRARY_PATH=/usr/lib/hadoop/lib/native
export SPARK_HOME=/usr/lib/spark
export PFE_PATH=*source_directory*
export BATCH_KEY=*batch_key*
export NUM_SCENARIOS=*number_of_scenarios* # e.g. 10 for a local machine batch
export NUM_PARTITIONS=*number_of_partitions* # e.g. 48 for a local machine batch
export HWA=*hull_white_alpha* # e.g. 0.376739
export HWS=*hull_white_sigma* #e.g. 0.0209835

# submit Spark job - adjust s3 bucket, filenames and scenario variables as needed

spark-submit \
--deploy-mode client \
--packages com.datastax.spark:spark-cassandra-connector_2.12:3.0.0-beta \
--conf spark.cassandra.connection.host=localhost \
--conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions \
$PFE_PATH/PFE_CALC_CASS.py $NUM_SCENARIOS $NUM_PARTITIONS $HWA $HWS \
$PFE_PATH/1504_USD_LIB_SWAP_CURVE.csv \
$PFE_PATH/1504_EUR_LIB_SWAP_CURVE.csv \
$PFE_PATH/1504_USD3MTD156N.csv \
$PFE_PATH/1504_INSTRUMENTS.csv \
$PFE_PATH/output090720 \
$BATCH_KEY
