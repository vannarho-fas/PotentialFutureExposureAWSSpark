from pylab import *
from boto3.session import Session
from io import StringIO

NSim = 5000 # I know the number of simulations


# This is a quick and dirty way to connect to S3, not to be used in production
session = Session(aws_access_key_id='AWS_ACCESS_KEY',
    aws_secret_access_key='AWS_SECRET_ACCESS_KEY')
s3 = session.resource("s3")
s3.Bucket('your-bucket').download_file('output/time-grid/part-00000', 'time_grid')
s3.Bucket('your-bucket').download_file('output/npv_cube/part-00000', 'npv_cube')

#load the files into numpy arrays for analysis
T = np.loadtxt('time_grid')
data_file = open('npv_cube', 'r')
block = ''

npv_cube = np.zeros((NSim,len(T),2))

#quick way to load, I am sure there are better ways
for line in data_file:
    block += line.replace('[','').replace(']','').lstrip()
data_file.close()

npv_cube = np.loadtxt(StringIO(unicode(block)))
#reshape to array with NSim rows, time-grid columns and with each cell having 2 NPVs
npv_cube = npv_cube.reshape(NSim, len(T), 2)

#plot simulated exposure paths
plt.figure(figsize=(7, 5), dpi=300)
for i in range(0, 30):
    plt.plot(T, npv_cube[i,:,0]/1000., 'r') # Uncollateralized
    plt.plot(T, npv_cube[i, :, 1] / 1000., 'b') # Collateralized
plt.xlabel("Time in years")
plt.ylabel("Exposure in Thousands")
plt.title("Simulated Exposure paths")
plt.show()

# Calculate the expected exposure
E = npv_cube.copy()
uncoll_exposures = npv_cube[:,:,0]
uncoll_exposures[uncoll_exposures < 0] = 0
uncoll_expected_exposures = np.sum(uncoll_exposures, axis=0) / NSim

coll_exposures = npv_cube[:,:,1]
coll_exposures[coll_exposures < 0] = 0
coll_expected_exposures = np.sum(coll_exposures, axis=0) / NSim


plt.figure(figsize=(7, 5), dpi=300)
plt.plot(T, uncoll_expected_exposures/1000., 'r', label='Uncollateralized')
plt.plot(T, coll_expected_exposures/1000., 'b', label='Collateralized')
plt.xlabel("Time in years")
plt.ylabel("Expected Exposure in Thousands")
plt.title("Expected Exposure")
plt.legend(loc='upper left')

# Calculate the PFE curve (95% quantile)
uncoll_PFE_curve = np.percentile(uncoll_exposures, 95, axis=0, interpolation='higher')
coll_PFE_curve = np.percentile(coll_exposures, 95, axis=0, interpolation='higher')
plt.figure(figsize=(7,7), dpi=300)
plt.plot(T, uncoll_PFE_curve, 'r', label='Uncollateralized')
plt.plot(T, coll_PFE_curve, 'b', label='Collateralized')
plt.xlabel("Time in years")
plt.ylabel("PFE")
plt.title("PFE Curves")
plt.legend(loc='upper left')

# calculate the maximum pfe
MPFE = np.max(uncoll_PFE_curve)
print 'Maximum Uncollateralized PFE:%f' % MPFE
# Maximum Uncollateralized PFE:260962.609258

MPFE = np.max(coll_PFE_curve)
print 'Maximum Collateralized PFE:%f' % MPFE
# Maximum Collateralized PFE:252916.082352



