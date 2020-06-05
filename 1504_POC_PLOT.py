from pylab import *
import matplotlib.pyplot as plt
import botocore
import boto3
from boto3.session import Session
from io import StringIO

# Config the plot scheme and #scenarios
matplotlib.use('cairo')
NSim = 5000 # Edit this to match the number of simulations used in the initial NPV calcs

# Not to be used in a production setting
session = Session(aws_access_key_id='AWS_ACCESS_KEY',
    aws_secret_access_key='AWS_SECRET_ACCESS_KEY')

# Retrieve the files - CHANGE THE BUCKET NAME AND PATH AS NEEDED
# latest POC has separate folders for each counterparty
get3 = boto3.client('s3')
get3.download_file('#bucket#', '#path_to_file#', 'time_grid')
get3.download_file('#bucket#', '#path_to_file#', 'npv_cube')


# Load the files into numpy arrays for analysis
T = np.loadtxt('time_grid')
data_file = open('npv_cube', 'r')
block = ''
npv_cube = np.zeros((NSim,len(T),2))

# Realign and load data
for line in data_file:
    block += line.replace('[','').replace(']','').lstrip()
npv_cube = np.loadtxt(StringIO(str(block)))

# Reshape to array with NSim rows, time-grid columns and with each cell having 2 NPVs (Uncollateralised and non-Uncollateralised)
npv_cube = npv_cube.reshape(NSim, len(T), 2)

# Plot and save the simulated exposure paths using matplotlib
plt.figure(dpi=300)
for i in range(0, 30):
    plt.plot(T, npv_cube[i,:,0]/1000., 'r') # Uncollateralised
    plt.plot(T, npv_cube[i, :, 1] / 1000., 'b') # Collateralised
plt.ylabel("Exposure in Thousands")
plt.title("Simulated Exposure paths")
plt.xlabel("Time in years")
plt.show()  # if using notebook
plt.savefig('simex.png')
s3_client = boto3.resource('s3')
response = s3_client.meta.client.upload_file('simex.png', 'pfe2020', 'simex.png')


# Calculate the expected exposure
E = npv_cube.copy()
uncoll_exposures = npv_cube[:,:,0]
uncoll_exposures[uncoll_exposures < 0] = 0
uncoll_expected_exposures = np.sum(uncoll_exposures, axis=0) / NSim
coll_exposures = npv_cube[:,:,1]
coll_exposures[coll_exposures < 0] = 0
coll_expected_exposures = np.sum(coll_exposures, axis=0) / NSim

# Plot the expected exposure
plt.figure(figsize=(7, 5), dpi=300)
plt.plot(T, uncoll_expected_exposures/1000., 'r', label='Uncollateralised')
plt.plot(T, coll_expected_exposures/1000., 'b', label='Collateralised')
plt.ylabel("Expected Exposure in USD$k")
plt.title("Expected Exposure")
plt.xlabel("Time in years")
plt.legend(loc='upper left')
# added
plt.savefig('epex.png')
s3_client = boto3.resource('s3')
# set bucket name
response = s3_client.meta.client.upload_file('epex.png', '#bucket#', 'epex.png')

# Calculate and plot the PFE curve (95% quantile)
uncoll_PFE_curve = np.percentile(uncoll_exposures, 95, axis=0, interpolation='higher')
coll_PFE_curve = np.percentile(coll_exposures, 95, axis=0, interpolation='higher')
plt.figure(figsize=(7,7), dpi=300)
plt.plot(T, uncoll_PFE_curve, 'r', label='Uncollateralised')
plt.plot(T, coll_PFE_curve, 'b', label='Collateralised')
plt.xlabel("Time in years")
plt.ylabel("PFE USD$")
plt.title("PFE Curves")
plt.legend(loc='upper left')
# added
plt.savefig('pfe.png')
s3_client = boto3.resource('s3')
# set bucket name
response = s3_client.meta.client.upload_file('pfe.png', '#bucket#', 'pfe.png')

# calculate the maximum pfe
MPFE = np.max(uncoll_PFE_curve)
print ('Maximum Uncollateralised PFE:%f' % MPFE)

MPFE = np.max(coll_PFE_curve)
print ('Maximum Collateralised PFE:%f' % MPFE)


# Maximum Uncollateralised PFE from previous calc:899797.307413
# Maximum Collateralised PFE from previous calc:779188.920790



