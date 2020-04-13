# Potential Future Exposure estimation using AWS Elastic Map Reduce (EMR) & Spark

** A proof of concept for estimating potential future exposure ("PFE") with QuantLib and AWS EMR. **

Amazon EMR provides a managed Hadoop framework that makes it easy, fast, and cost-effective to process vast amounts of data across dynamically scalable Amazon EC2 instances. You can also run these other popular distributed frameworks such as Apache Spark, Zepplin, Hive and interact with data in other AWS data stores such as Amazon S3 and Amazon DynamoDB.

If you use common big data Apache tools, you should seriously consider Amazon EMR because it simplifies the configuration process. A big advantage of AWS over other platforms for the enterprise user, especially in highly regulated financial services domains. is the wide selection of features and the robust security & permissions. 

That’s not to say it is always easy to set up, though. :sweat:

This will cover:
* Launching a EC2 Instance
* Installing Anaconda, Boost, QuantLib, Quantlib-Swig
* Building a Amazon Machine Image (AMI)
* Creating a EMR Spark Cluster
* Building the PFE script
* Running the PFE script
* Analysing the results
* Considerations for extending this limited POC

## Context

_Counterparty risk is the risk that a party to an OTC derivatives contract may fail to perform on its contractual obligations, causing losses to the other party. Credit exposure is the actual loss in the event of a counterparty default._

Some of the ways to reduce counterparty risk:

**Netting:** Offset positive and negative contract values with the same counterparty reduces exposure to that counterparty

**Collateral:** Holding cash or securities against an exposure

**Central counterparties (CCP):** Use a third party clearing house as a counterparty between buyer and seller and post margin

**Potential Future Exposure (PFE)** is a measure of credit risk and is the worst exposure one could have to a counterparty at a certain time in future with a certain level of confidence. For example, for a PFE of 100,000with95100,000 in only 5% of scenarios.

Netting set is a group of OTC trades (could be interest rate swaps, FxFwds or CCS) that are facing the same counterparty;  a firm can often “net” the exposures of different instruments in the set which reduces the exposure. For example, a positive exposure on a swap could be netted with a negative exposure on FxFwd.

Estimating PFE involves simulating future market risk scenarios, calculating “netted” mark-to-market ("MtMs") values of OTC trades that are facing the same counterparty at various dates in future at each scenario and taking only the positive MtMs which represent our exposure to counterparty, then taking (for example) 95% quantile of the peak exposures.

For an interest rate swap, market risk factor is the underlying forward curve which determines the NPV of the floating leg. For an FxFwd, its the forward interest curves for the two currencies and the forward FX rate.


## We will assume you've done the following

* Set up an AWS account
* Created a S3 Bucket
* Created and Downloaded a Key Pair
* Created A Security Group (optional for this simple proof of concept)

## Launching a EC2 Instance

* Navigate to EC2 Instances
* Select "launch instance"
* Step 1: Choose an Amazon Machine Image (AMI) - Choose _Amazon Linux 2 AMI (HVM)_, SSD Volume Type - ami-0970010f37c4f9c8d (64-bit x86)
* Step 2: Choose Instance Type - choose _t2.xlarge_ (you can choose a free instance however the compilations times will be longer)
* Step 3: Configure Instance Details - leave settings as per default
* Step 4: Add Storage - leave as default (8 GiB)
* Step 5: Add tags - leave as default
* Step 6: Configure Security Group - either leave as default or choose your own security group. Recommend setting "Source" to "My IP" (for ssh'ing into the instance)
* Step 7: Review and launch - on launch choose your keypair to allow access via ssh. 

Go to instance page and "connect" to the instance via ssh (using your downloaded key pair)


## Installing Anaconda, Boost, QuantLib, Quantlib-Swig

Once you're connect to the instance, you'll install the base software required. 




