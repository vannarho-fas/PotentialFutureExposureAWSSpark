# Potential Future Exposure estimation using AWS Elastic Map Reduce & Spark

** A proof of concept for estimating potential future exposure ("PFE") with QuantLib and AWS Elastic Map Reduce ("EMR"). **

This will cover:
* The methods used for determing the potential exposure and different time periods for different OTC traded products
* The steps needed to build and run the infrastructure and software and then analyse the data outputs
* The proposed next steps to further extend this proof of concept ("POC") further

## Context

The first decade of the 21st Century was  disastrous for financial institutions, derivatives and risk management. Counterparty credit risk has become the key element of financial risk management, highlighted by the bankruptcy of the investment bank Lehman Brothers and failure of other high profile institutions such as Bear Sterns, AIG, Fannie Mae and Freddie Mac. The sudden realisation of extensive counterparty risks has severely compromised the health of global financial markets. Counterparty risk and the estimate of potential future expsoure is now a key problem for all financial institutions.

Over-the-counter ("OTC") products are traded (and privately negotiated) directly between two parties, without going through an exchange or other intermediary. The key types of OTC products are:

* Interest rate derivatives: The underlying asset is a standard interest rate. Examples of interest rate OTC derivatives include LIBOR, Swaps, US Treasury bills, Swaptions and FRAs.

* Commodity derivatives: The underlying are physical commodities like wheat or gold. E.g. forwards.

* Forex derivatives: The underlying is foreign exchange fluctuations.

* Equity derivatives: The underlying are equity securities. E.g. Options and Futures

* Fixed Income: The underlying are fixed income securities.

* Credit derivatives: It transfers the credit risk from one party to another without transferring the underlying. These can be funded or unfunded credit derivatives. e.g: Credit default swap (CDS), Credit linked notes (CLN).

_Counterparty risk is the risk that a party to an OTC derivatives contract may fail to perform on its contractual obligations, causing losses to the other party. Credit exposure is the actual loss in the event of a counterparty default._

Some of the ways to reduce counterparty risk:

**Netting:** Offset positive and negative contract values with the same counterparty reduces exposure to that counterparty

**Collateral:** Holding cash or securities against an exposure

**Central counterparties (CCP):** Use a third party clearing house as a counterparty between buyer and seller and post margin (see https://www.theice.com/article/clearing/how-clearing-mitigates-risk)

**Potential Future Exposure (PFE)** is a measure of credit risk and is the worst exposure one could have to a counterparty at a certain time in future with a certain level of confidence. For example, for a PFE of 100,000with95100,000 in only 5% of scenarios.

_More high level context here: https://www.edupristine.com/blog/otc-derivatives._

### Amazon EMR & PFE

EMR provides a managed Hadoop framework that makes it easy, fast, and cost-effective to process vast amounts of data across dynamically scalable Amazon EC2 instances, like the data analysis required to estimate FPE. You can also run these other popular distributed frameworks such as Apache Spark, Zepplin, Hive and interact with data in other AWS data stores such as Amazon S3 and Amazon DynamoDB.

If you use common big data Apache tools, consider Amazon EMR because it simplifies the configuration process. A big advantage of AWS over other platforms for the enterprise user, especially in highly regulated financial services domains, are the robust security & permissions features. 

That’s not to say it is always easy to set up, though. :sweat:

The field of quantitative finance upon which this POC rests is vast and quickly becomes exceedingly complex! A key theoretical references is: https://www.amazon.com/Counterparty-Credit-Risk-challenge-financial/dp/047068576X

This work would not have been possible without the specific Python design and examples from:
* http://gouthamanbalaraman.com/blog/hull-white-simulation-quantlib-python.html
* http://suhasghorp.com/estimating-potential-future-exposure-with-quantlib-and-aws-emr-part-i/
* https://ipythonquant.wordpress.com/2015/04/08/expected-exposure-and-pfe-simulation-with-quantlib-and-python/


## Methods used

### Interest rate swap

For an interest rate swap, market risk factor is the underlying forward curve which determines the NPV of the floating leg. To generate future scenarios of the curves, a Hull-White one factor short rate model was used.  

### Currency Foreign Exhange Forward

For an FxFwd, the exposure is the forward interest curves for the two currencies and the forward FX rate. The FxFwd example in this POC is unrealistically simple. FxFwd exposure at time t is calculated by estimating the future spot rate at various points in time using the same yield curve simulation used for the interest rate swap calculation. 

### Netting

A firm can often “net” the exposures of different instruments from the same counterparty. Estimating PFE involves simulating future market risk scenarios, calculating “netted” mark-to-market ("MtMs") values of OTC trades that are facing the same counterparty at various dates in future at each scenario and taking only the positive MtMs which represent the exposure to counterparty, then taking (for example) 95% quantile of the peak exposures. This is used for regulator reporting and for the provision of capital to manage the situation of counterparty default. 


## PFE POC


### We will assume you've done the following

* Set up an AWS account
* Created a S3 Bucket
* Created and Downloaded a Key Pair
* Created A Security Group (optional for this simple proof of concept)


### Launching a EC2 Instance

* Navigate to EC2 Instances
* Select "launch instance"
* Step 1: Choose an Amazon Machine Image (AMI) - Choose _Amazon Linux 2 AMI (HVM)_, SSD Volume Type - ami-0970010f37c4f9c8d (64-bit x86)
* Step 2: Choose Instance Type - you can choose a free instance however the install time will be a little bit longer
* Step 3: Configure Instance Details - leave settings as per default
* Step 4: Add Storage - set as 8 GiB
* Step 5: Add tags - leave as default
* Step 6: Configure Security Group - either leave as default or choose your own security group. Recommend setting "Source" to "My IP" (for ssh'ing into the instance)
* Step 7: Review and launch - on launch choose your keypair to allow access via ssh. 

Go to instance page and "connect" to the instance via ssh (using your downloaded key pair)

## Installing Boost, QuantLib and other packages

Once you're connect to the instance, you'll install the base software required. 

ssh into the box using your local PEM file and the specific machine address. e.g. `ssh -i /Users/XX/XX.pem ec2-user@ec2-XX-XX-XXX-XXX.ap-southeast-2.compute.amazonaws.com`. 

Run the 1304instpack_pip.sh script either by downloading it (e.g. `wget https://raw.githubusercontent.com/fordesmith/PotentialFutureExposureAWSSpark/master/1304instpack_pip.sh`) and running it (e.g. bash ./1304instpack_pip.sh) or by copying the file into your terminal window. Please forgive the liberal use of sudo; this is only a POC, after all. 

Once complete, choose "Image > Create Image" to save an AMI to use for your cluster. 

## Setting up the cluster

Go to EMR and "Create Cluster". Go to "Advanced Options". In "software configuration" choose release 6.0.0 plus check "Hadoop" and "Spark". Choose "next". In "Hardware" choose 4 x core nodes. Leave other settings as is. Choose "next". In "Additional Options" choose the AMI you created above. Choose "next". In "Security Options" choose the key pair you created and downloaded. Then "create cluster". 

A spark cluster has n nodes managed by a central master. This allow it offer large scale parallel processing. 

![Spark Cluster Diagram](https://raw.githubusercontent.com/fordesmith/PotentialFutureExposureAWSSpark/master/visualisations/cluster-overview.png).


## Developing the PFE script inputs

Gather inputs
* Define and set start date
* Load historical libor rates, swap specifications and FxFwd specifications from input file into an RDD and collect the results
** Daily Libor rates are 3M USD Libor rates for previous years
** Swap specifications: commencement date, term, amount, fixed rate, type (pay, receive)
** FxFwd specifications: commencement date, term, amount, rate, currency 1, currency 2
** Load USD, EUR libor swap curve from input file
 
Prepare objects from inputs
* Build a QuantLib swap and index object
* Generate a matrix of normally distributed random numbers and spread them across the cluster
* Define the NPV cube array
* Hull White parameter estimations to generate USD & EUR discount factors
* Use Garman-Kohlagen process to build FX rate simulation for FxFwd
 
Loop through dates and define NPVs
* X
* X


## Submitting the spark job

Copy the files in this repo to your s3 bucket and amend the 1304spark-submit.sh file to point to your s3 bucket. Run the file. It will take somewhere from about 7 - 25 minutes for Pyspark job to complete, depending on the hardware spec. It computes a netting set NPV for 5000 simulations across future 454 dates for 2 swaps and 1 FxFwd.  

After the spark job completes, it will create an "output" folder in your s3 bucket. The output files  are time-grid array and NPV cube. 

## Visualising the results

Change the destination in the 1304_pfe_visualization.py to your s3 bucket and run it (e.g. via bash, notebook, ipython terminal). Once we have the time grid and NPV cube in memory,  this script will visualize the simulated exposure paths. The Blue paths are for Collateralised exposures and Red are for Uncollateralised.

![Estimated Potential Exposure](https://raw.githubusercontent.com/fordesmith/PotentialFutureExposureAWSSpark/master/visualisations/simex.png).

![Expected Exposure](https://raw.githubusercontent.com/fordesmith/PotentialFutureExposureAWSSpark/master/visualisations/epex.png).

![Potential Future Exposure - Collatoralised & Uncollateralised](https://raw.githubusercontent.com/fordesmith/PotentialFutureExposureAWSSpark/master/visualisations/pfe.png).


## Next steps to extend the POC

* Add more derivative types (e.g. commodity, credit derivatives) 
* Extend to and create reports for different counterparties



