
# test for using Cassandra as the DB for storing PFE exposure batch calculations

from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from ssl import SSLContext, PROTOCOL_TLSv1, CERT_REQUIRED
from cassandra.auth import PlainTextAuthProvider
import random as rand
from pyspark import sql
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT, ConsistencyLevel
from cassandra.query import tuple_factory

profile = ExecutionProfile(
    consistency_level=ConsistencyLevel.LOCAL_QUORUM,
    request_timeout=15,
    row_factory=tuple_factory
)

ssl_context = SSLContext(PROTOCOL_TLSv1)
ssl_context.load_verify_locations('/home/hadoop/AmazonRootCA1.pem')
ssl_context.verify_mode = CERT_REQUIRED
auth_provider = PlainTextAuthProvider(username='Administrator-at-952436753265', password='LEupSM26+oER0WePcyydcaXEmBLu/n3rd7brkC8mtc0=')
cluster = Cluster(['cassandra.ap-southeast-2.amazonaws.com'], ssl_context=ssl_context, auth_provider=auth_provider, port=9142, execution_profiles={EXEC_PROFILE_DEFAULT: profile})
session = cluster.connect()
r = session.execute('select * from system.peers')
print(r.current_rows)

# sc = SparkContext.getOrCreate(SparkConf())
conf = SparkConf().setAppName("PFE-POC")
sc = SparkContext(conf=conf)
sc.setLogLevel('INFO')

sqlContext = sql.SQLContext(sc)
sess = SparkSession(sc)

a = rand.randrange(100, 10000)
b = rand.randrange(-10, 10)
date = '01-05-2020'
counterparty = 111
coll_exp = 100.00 + b + b/10
non_coll_exp = 100.00 + b + b/10
batch_key = date + '_' + str(counterparty) + '_' + str(a)

exp_insert_stmt = session.prepare("INSERT INTO pfe_poc.pfe (batch_key, counterparty, date,coll_exp,non_coll_exp) VALUES (?, ?, ?, ?,?)")
user = session.execute(exp_insert_stmt, [batch_key, counterparty, date,coll_exp,non_coll_exp])

r = session.execute('select * from pfe_poc.pfe')
print(r.current_rows)


def load_sim(sim_file):
    sim_vals = sc.textFile(sim_file) \
        .map(lambda line: line.split("[")) \
        .map(lambda line: line.split("]")) \
        .map(lambda line: (float(line[0]), float(line[1]))).cache()
    return sim_vals




# load simulation file
# outp = load_sim("file:///Users/forsmith/Documents/PotentialFutureExposureAWSSpark/work-in-progress/out.0")
# outp = load_sim("https://pfefiles.s3-ap-southeast-2.amazonaws.com/out.0.rtf")

#Zip the two rdd together
#rdd_temp = rdd1.zip(rdd2)

#Perform Map operation to get your desired output by flattening each element
#Reference : https://stackoverflow.com/questions/952914/making-a-flat-list-out-of-list-of-lists-in-python
#sim_rdd = rdd_temp.map(lambda x: [item for sublist in x for item in sublist])
#print sim_rdd.take(2)

