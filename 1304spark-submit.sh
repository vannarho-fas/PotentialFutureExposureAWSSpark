LD_LIBRARY_PATH=/usr/local/lib
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3
export PYSPARK_PYTHON=/usr/bin/python3
export PYTHONPATH=/usr/bin/python3


sudo spark-submit \
--deploy-mode client \
--master yarn \
--conf spark.driver.extraLibraryPath="${LD_LIBRARY_PATH}" \
--conf spark.executorEnv.LD_LIBRARY_PATH="${LD_LIBRARY_PATH}"  \
--num-executors 4 \
--conf spark.executor.memoryOverhead=1280 \
--executor-memory 16G \
--conf spark.driver.memoryOverhead=1280 \
--driver-memory 16G \
--executor-cores 4 \
--driver-cores 4 \
--conf spark.default.parallelism=168 \
s3://pfepoc/1304_PFE_SPARK.py 5000 48 0.376739 0.0209835 \
s3://pfepoc/usd-libor-swap-curve.csv \
s3://pfepoc/eur-libor-swap-curve.csv \
s3://pfepoc/USD3MLibor-Fixings.csv \
s3://pfepoc/instruments.csv \
s3://pfepoc/output
