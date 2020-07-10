
# test for using Cassandra as the DB for storing PFE exposure batch calculations



from ssl import SSLContext, PROTOCOL_TLSv1, CERT_REQUIRED
from cassandra.auth import PlainTextAuthProvider
import random as rand
from pyspark import sql
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT, ConsistencyLevel
from cassandra.query import tuple_factory
import numpy as np


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
r = session.execute('select * from pfe_poc.pfe')
print(r.current_rows)



a = rand.randrange(100, 10000)
b = rand.randrange(-10, 10)
date = '01-05-2020'
counterparty = 111
coll_exp = 100.00 + b + b/10
non_coll_exp = 100.00 + b + b/10
batch_key = date + '_' + str(counterparty) + '_' + str(a)

exp_insert_stmt = session.prepare("INSERT INTO pfe_poc.pfe (batch_key, counterparty, date,non_coll_exp, coll_exp) VALUES (?, ?, ?, ?,?)")
user = session.execute(exp_insert_stmt, [batch_key, counterparty, date,non_coll_exp,coll_exp])

r = session.execute('select * from pfe_poc.pfe')
print(r.current_rows)



