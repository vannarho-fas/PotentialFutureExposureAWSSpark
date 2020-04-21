# Adjust environment variables as needed
LD_LIBRARY_PATH=/usr/local/lib
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3
export PYSPARK_PYTHON=python3
export PYTHONPATH=/usr/bin/python3


# submit Spark job - adjust s3 bucket, filenames and scenario variables as needed
sudo spark-submit \
--deploy-mode client \
--master yarn \
--conf spark.driver.extraLibraryPath="${LD_LIBRARY_PATH}" \
--conf spark.executorEnv.LD_LIBRARY_PATH="${LD_LIBRARY_PATH}"  \
--num-executors 4 \
--conf spark.executor.memoryOverhead=1280 \
--executor-memory 32G \
--conf spark.driver.memoryOverhead=1280 \
--driver-memory 32G \
--executor-cores 16 \
--driver-cores 16 \
--conf spark.default.parallelism=168 \
s3://pfe2020/1504_PFE_CALC.py 5000 48 0.376739 0.0209835 \
s3://pfe2020/1504_USD_LIB_SWAP_CURVE.csv \
s3://pfe2020/1504_EUR_LIB_SWAP_CURVE.csv \
s3://pfe2020/1504_USD3MTD156N.csv \
s3://pfe2020/1504_INSTRUMENTS.csv \
s3://pfe2020/output1504