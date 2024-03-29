# Potential Future Exposure estimation using Python, QuantLib, Spark on AWS Elastic Map Reduce

*A first-draft proof of concept ("POC") for estimating potential future exposure ("PFE") with QuantLib and AWS Elastic Map Reduce ("EMR").*

This covers:
* The methods used for determing the potential exposure over different time periods for different over-the-counter ("OTC") products (for this exercise, two interest rates swaps and one foreign exchange forward ("FXFwd") - includes basic netting, collateral
* The steps needed to build and run the infrastructure and software and then analyse the data outputs
* Thoughts on extending this POC 

NOTE:
1. The instructions below cover setting AWS EMR to run a scaled set of scenarios. You can of course run this on your local, however, the set up instructions aren't included here (e.g. how to install spark, hadoop, etc). If you do I'd recommend reducing the number of scenarios to a lower number e.g. 10-20 rather than 5000 as it will be very S L O W. 
2. Since writing this I have experimented with the design further to write out to Cassandra (see the files PFE_CALC_CASS.py and spark-submit-cassandra.sh). This can be used on local by setting up a local Cassandra node, setting up a keyspace and table with a matching schema to the pyspark dataframe, and by adding some additional parameters to the spark-submit job (to pick up the cassandra drivers). Using it on AWS requires some additional credentials and set up of AWS keyspaces. I haven't documented instructions for this here. 

## Context (you can skip this bit if you work in capital markets :wink: )

The first decade of the 21st Century was  disastrous for financial institutions, derivatives and risk management. Counterparty credit risk has become the key element of financial risk management, highlighted by the bankruptcy of the investment bank Lehman Brothers and failure of other high profile institutions such as Bear Sterns, AIG, Fannie Mae and Freddie Mac. The sudden realisation of extensive counterparty risks, primarily from OTC products, severely compromised the health of global financial markets. Estimating potential future exposure is now a key task for all financial institutions.

OTC products are traded and privately negotiated directly between two parties, without going through an exchange or other intermediary. The key types of OTC products are:

* Interest rate derivatives: The underlying asset is a standard interest rate. Examples of interest rate OTC derivatives include LIBOR, Swaps, US Treasury bills, Swaptions and FRAs.

* Commodity derivatives: The underlying are physical commodities like wheat or gold. E.g. forwards.

* Forex derivatives: The underlying is foreign exchange fluctuations.

* Equity derivatives: The underlying are equity securities. E.g. Options and Futures

* Fixed Income: The underlying are fixed income securities.

* Credit derivatives: It transfers the credit risk from one party to another without transferring the underlying. These can be funded or unfunded credit derivatives. e.g: Credit default swap (CDS), Credit linked notes (CLN).

_Counterparty risk is the risk that a party to an OTC derivatives contract may fail to perform on its contractual obligations, causing losses to the other party. Credit exposure is the actual loss in the event of a counterparty default._

Some of the ways to reduce counterparty risk:

* **Netting:** Offset positive and negative contract values with the same counterparty reduces exposure to that counterparty 

* **Collateral:** Holding cash or securities against an exposure 

* **Central counterparties (CCP):** Use a third party clearing house as a counterparty between buyer and seller and post margin (see https://www.theice.com/article/clearing/how-clearing-mitigates-risk) 

**Potential Future Exposure (PFE)** is a measure of credit risk and is the worst exposure one could have to a counterparty at a certain time in future with a certain level of confidence. For example, for a PFE of 100,000with95 = $100,000 in only 5% of scenarios.

_More high level context here: https://www.edupristine.com/blog/otc-derivatives._

### QuantLib

QuantLib is a comprehensive open source software framework for quantitative finance. QuantLib is written in C++ and exposed via different languages (python used here). It offers tools that are useful both for practical implementation and for advanced modeling, with features such as market conventions, yield curve models, solvers, PDEs, Monte Carlo (low-discrepancy included), exotic options, VAR, and so on.

### Amazon EMR & PFE

EMR provides a managed Hadoop framework that makes it easy, fast, and cost-effective to process vast amounts of data across dynamically scalable Amazon EC2 instances, like the data analysis required to estimate FPE. You can also run other popular distributed frameworks such as Apache Spark, Zepplin, Hive and interact with data in other AWS data stores such as Amazon S3 and Amazon DynamoDB.

If you use common big data Apache tools, consider Amazon EMR because it simplifies the configuration process. A big advantage of AWS over other platforms for the enterprise user, especially in highly regulated financial services domains, are the robust security & permissions features. 

That’s not to say it is always easy to set up, though. :sweat:

The field of quantitative finance upon which this POC rests is vast and quickly becomes exceedingly complex! A key theoretical references is: https://www.amazon.com/Counterparty-Credit-Risk-challenge-financial/dp/047068576X

This work would not have been possible without the specific help, Python design and examples from:
* https://www.implementingquantlib.com/ - thanks to Luigi for being the "father" of Quantlib and for replying to my questions
* https://quantlib-python-docs.readthedocs.io/en/latest/
* http://gouthamanbalaraman.com/blog/hull-white-simulation-quantlib-python.html
* https://ipythonquant.wordpress.com/2015/04/08/expected-exposure-and-pfe-simulation-with-quantlib-and-python/
* http://suhasghorp.com/estimating-potential-future-exposure

...and all the people who helped on Stack Overflow or otherwise. 


## Methods used

### Interest rate swap

For an interest rate swap, market risk factor is the underlying forward curve which determines the NPV of the floating leg. To generate future scenarios of the curves, a Hull-White one factor short rate model was used.  

### Currency Foreign Exhange Forward

For an FxFwd, the exposure is the forward interest curves for the two currencies and the forward FX rate. The FxFwd example in this POC is unrealistically simple. FxFwd exposure at time t is calculated by estimating the future spot rate at various points in time using the same yield curve simulation used for the interest rate swap calculation. 

### Netting

A firm can often “net” the exposures of different instruments from the same counterparty. Estimating PFE involves simulating future market risk scenarios, calculating “netted” mark-to-market ("MtMs") values of OTC trades that are facing the same counterparty at various dates in future at each scenario and taking only the positive MtMs which represent the exposure to counterparty, then taking (for example) 95% quantile of the peak exposures. This is used for regulator reporting and for the provision of capital to manage the situation of counterparty default. 


## PFE POC

The instructions below are for running the job on AWS. You can run it on your local machine, but I haven't described this below. You'll need spark 2.4.5 (now a back version - version 3 may not work)and there's lots of resources online for this (e.g. https://medium.com/beeranddiapers/installing-apache-spark-on-mac-os-ce416007d79f)


### We will assume you've done the following

* Set up an AWS account
* Created a S3 Bucket
* Created and Downloaded a Key Pair
* Created A Security Group (optional for this simple proof of concept)

### Version assumptions

* AWS linux 64 bit
* EMR Version 6
* Python 3.7+
* Spark 2.4.5
* Hadoop 3.2.1
* Java 8 (if you're using Cassandra 3 on local)


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

## Installing Boost, QuantLib and other packages - Creating the Amazon Machine Image ("AMI")

Once you're connect to the instance, you'll install the base software required. 

ssh into the box using your local PEM file and the specific machine address. e.g. `ssh -i /Users/XX/XX.pem ec2-user@ec2-XX-XX-XXX-XXX.ap-southeast-2.compute.amazonaws.com`. 

Run the 1504_PFE_INSTALL_PACKAGES.sh script by downloading it: e.g.

<pre><code> 
wget https://raw.githubusercontent.com/fordesmith/PotentialFutureExposureAWSSpark/master/1504_PFE_INSTALL_PACKAGES.sh
sudo bash ./1504_PFE_INSTALL_PACKAGES.sh
</pre></code>

The AMI provides the base packages required for the cluster instance. 

Once complete, choose "Image > Create Image" to save an AMI to use for your cluster. 

## Setting up the cluster

### By Terminal

This assumes you've set up the AWS CLI. 

<pre><code>
aws emr create-cluster \
--applications Name=Hadoop Name=Hive Name=Hue Name=Spark Name=Zeppelin \
--ec2-attributes '{"KeyName":"*YOUR_KEY*","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-1aa9dc43","EmrManagedSlaveSecurityGroup":"sg-08f83f2680c4721b9","EmrManagedMasterSecurityGroup":"sg-0f4a0166888d51211"}' \
--release-label emr-6.0.0 \
--log-uri 's3n://aws-logs-*YOUR_LOG-YOUR_REGION*/elasticmapreduce/' \
--instance-groups '[{"InstanceCount":4,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":64,"VolumeType":"gp2"},"VolumesPerInstance":4}]},"InstanceGroupType":"CORE","InstanceType":"m5a.4xlarge","Name":"Core - 4"},{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"MASTER","InstanceType":"m5.xlarge","Name":"Master - 1"}]' \
--custom-ami-id ami-*YOUR_AMI* \
--auto-scaling-role EMR_AutoScaling_DefaultRole \
--ebs-root-volume-size 16 \
--service-role EMR_DefaultRole \
--enable-debugging \
--repo-upgrade-on-boot SECURITY \
--name 'PFE POC' \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--region *YOUR_REGION*
</pre></code>


### By AWS Console

Go to EMR and "Create Cluster". Go to "Advanced Options". In "software configuration" choose release 6.0.0 plus check "Hadoop", "Hive", "Hue", "Zepplin" and "Spark". Choose "next". In "Hardware" choose 4 x core nodes. Choose servers with at least 16GB memory. The disk size needs to be as big or bigger than the AMI image (e.g. >=8G).

For my test I used the following config: 
** Executors: 
m5a.4xlarge
16 vCore, 64 GiB memory, EBS only storage
EBS Storage:256 GiB

** Driver / Master: 1
m5.xlarge
4 vCore, 16 GiB memory, EBS only storage
EBS Storage:64 GiB

Leave other settings as is. Choose "next". In "Additional Options" choose the AMI you created above. Choose "next". In "Security Options" choose the key pair you created and downloaded. Then "create cluster". 

A spark cluster has n nodes managed by a central master. This allows it offer large scale parallel processing. 

![Spark Cluster Diagram](./visualisations/cluster-overview.png).


For the example here, the job computes a netting set NPV for 5000 simulations across 454 future dates for three counterparties each with 2-3 swaps and 1 FxFwd. The job completed in ~13.2 minutes with 4 workers and 4.2 minutes with 10 workers (with 1 worker per node). After reading more (e.g. https://medium.com/datakaresolutions/key-factors-to-consider-when-optimizing-spark-jobs-72b1a0dc22bf) I realised this is not a good design as the workers are too fat (need to have more per server). I then tried a higher spec cluster using GPU chips (Executors: 3 x p2.8xlarge 32 vCore, 488 GiB memory, EBS only storage, EBS Storage:256 GiB, Driver: 1 x m5.xlarge 4 vCore, 16 GiB memory, EBS only storage EBS Storage:64 GiB) with 4 workers per node ( 8 CPUs per worker) and the job completed in 1 minute. 

To speed the job up further I considered using Cython (C-compiled Python) which I believe could provide a further 30%+ improvement by just typing each variable, and more if python-specific routines are optimised. 

## Creating the input files

The inputs needed for this POC are: 
* Historical data - in this case for the interest rate swaps, 3M USD / Libor fixings e.g. from here: https://fred.stlouisfed.org/series/USD3MTD156N
* Future LIBOR swap curves for USD and EUR (in this case out to 2070)
* The list of instruments

If you are adjusting the files here or developing your own, format dates as YYYY-MM-DD to avoid further reformatting / parsing. 

## The key aspects of the PFE script for running the simulations

Assumes you are using Python3. 

### Gather inputs

#### Load modules

<pre><code>
from pyspark import SparkConf, SparkContext
from pyspark.mllib.random import RandomRDDs
import QuantLib as ql
import datetime as dt
import numpy as np
import math
import sys
</pre></code>

#### Define and set start date

<pre><code>
    broadcast_dict = {}
    pytoday = dt.datetime(2020, 4, 7)
    broadcast_dict['python_today'] = pytoday
    today = ql.Date(pytoday.day, pytoday.month, pytoday.year)
    ql.Settings.instance().evaluationDate = today
    usd_calendar = ql.UnitedStates()
    usd_dc = ql.Actual365Fixed()
</pre></code>

#### Load historical libor rates, swap specifications and FxFwd specifications from input file into an RDD and collect the results

<pre><code>
# Loads libor fixings from input file into an RDD and then collects the results
def load_libor_fixings(libor_fixings_file):
    libor_fixings = sc.textFile(libor_fixings_file) \
        .map(lambda line: line.split(",")) \
        .filter(lambda r: value_is_number(r[1])) \
        .map(lambda line: (str(line[0]), float(line[1]))).cache()

    fixing_dates = libor_fixings.map(lambda r: r[0]).collect()
    fixings = libor_fixings.map(lambda r: r[1]).collect()
    return fixing_dates, fixings


def load_counterparties(instruments_file):
    cps = sc.textFile(instruments_file) \
        .map(lambda line: line.split(",")) \
        .filter(lambda r: value_is_number(r[1])) \
        .filter(lambda r: (int(r[1])>0)) \
        .map(lambda line: (int(line[1]))) \
        .distinct() \
        .collect()
    return sorted(cps)


# Loads counterparty input swap specifications from input file into an RDD and then collects the results
def load_counterparty_irs_swaps(instruments_file, counterparty):
    cp_swaps = sc.textFile(instruments_file) \
        .map(lambda line: line.split(",")) \
        .filter(lambda r: r[0] == 'IRS') \
        .filter(lambda r: r[1] == str(counterparty)) \
        .map(lambda line: (int(line[1]), # counterparty number
                           str(line[2]),
                           str(line[3]),
                           float(line[4]),
                           float(line[5]),
                           str(line[6]))) \
        .collect()
    return cp_swaps




# Loads counterparty FxFwd specifications from input file into an RDD and then collects the results
def load_counterparty_fxfwds(instruments_file,counterparty):
    cp_fxfwds = sc.textFile(instruments_file) \
        .map(lambda line: line.split(",")) \
        .filter(lambda r: r[0] == 'FXFWD') \
        .filter(lambda r: r[1] == str(counterparty)) \
        .map(lambda line: (int(line[1]),  # counterparty number
                           str(line[2]),
                           str(line[3]),
                           float(line[4]),
                           float(line[5]),
                           str(line[6]),
                           str(line[7]))) \
        .collect()
    return cp_fxfwds

</pre></code>
 
 Note:
* Daily Libor rates are 3M USD Libor rates for previous years
* Swap specifications: commencement date, term, amount, fixed rate, type (pay, receive)
* FxFwd specifications: commencement date, term, amount, rate, currency 1, currency 2
* Load USD, EUR libor swap curve from input file


#### Build a QuantLib swap and index object

<pre><code>
def create_quantlib_swap_object(today, start, maturity, nominal, fixedRate, index, typ=ql.VanillaSwap.Payer):
    calendar = ql.UnitedStates()
    fixedLegTenor = ql.Period(6, ql.Months)
    floatingLegBDC = ql.ModifiedFollowing
    fixedLegDC = ql.Thirty360(ql.Thirty360.BondBasis)
    spread = 0.0
    settle_date = calendar.advance(start, 2, ql.Days)
    end = calendar.advance(settle_date, maturity, floatingLegBDC)

    fixedSchedule = ql.Schedule(settle_date,
                                end,
                                fixedLegTenor,
                                calendar,
                                ql.ModifiedFollowing, ql.ModifiedFollowing,
                                ql.DateGeneration.Forward, False)
    floatSchedule = ql.Schedule(settle_date,
                                end,
                                index.tenor(),
                                index.fixingCalendar(),
                                index.businessDayConvention(),
                                index.businessDayConvention(),
                                ql.DateGeneration.Forward,
                                False)
    swap = ql.VanillaSwap(typ,
                          nominal,
                          fixedSchedule,
                          fixedRate,
                          fixedLegDC,
                          floatSchedule,
                          index,
                          spread,
                          index.dayCounter())
    return swap, [index.fixingDate(x) for x in floatSchedule if index.fixingDate(x) >= today][:-1]
</pre></code>

#### Loop calculations to create scenarios for each counterparty, build QuantLib index object

<pre><code>
    # load counter parties
    counterparties = load_counterparties(instruments_file)
    broadcast_dict['counterparties'] = counterparties

    for counterparty in counterparties:

        swaps_list = load_counterparty_irs_swaps(instruments_file, counterparty)
        broadcast_dict['swaps'] = swaps_list

        fxfwds = load_counterparty_fxfwds(instruments_file,counterparty)
        broadcast_dict['fxfwds'] = fxfwds

        swaps = [
            create_quantlib_swap_object(today, ql.DateParser.parseFormatted(swap[1], '%Y-%m-%d'),
                                    ql.Period(swap[2]), swap[3], swap[4], usdlibor3m,
                                    ql.VanillaSwap.Payer if swap[5] == 'Payer' else ql.VanillaSwap.Receiver)
            for swap in swaps_list
        ]

        longest_swap_maturity = max([s[0].maturityDate() for s in swaps])
        broadcast_dict['longest_swap_maturity'] = quantlib_date_to_datetime(longest_swap_maturity)

        Nsim = int(args_dict['NSim'])

        a = float(args_dict['a'])  # 0.376739
        sigma = float(args_dict['sigma'])  # 0.0209835
        broadcast_dict['a'] = a
        broadcast_dict['sigma'] = sigma
 </pre></code>
    
#### Generate a matrix of normally distributed random numbers and spread them across the cluster - used the default settings from Spark for the partitions. 

<pre><code>
  random_array_rdd = RandomRDDs.normalVectorRDD(sc, Nsim, len(T), seed=1)
</pre></code>

    
#### Hull White parameter estimations to generate USD & EUR discount factors

<pre><code>
    def gamma(t):
        forwardRate = usd_t0_curve.forwardRate(t, t, ql.Continuous, ql.NoFrequency).rate()
        temp = sigma * (1.0 - np.exp(- a * t)) / a
        return forwardRate + 0.5 * temp * temp

    def B(t, T):
        return (1.0 - np.exp(- a * (T - t))) / a

    def A(t, T):
        forward = usd_t0_curve.forwardRate(t, t, ql.Continuous, ql.NoFrequency).rate()
        value = B(t, T) * forward - 0.25 * sigma * B(t, T) * sigma * B(t, T) * B(0.0, 2.0 * t)
        return np.exp(value) * usd_t0_curve.discount(T) / usd_t0_curve.discount(t)

    usd_rmat = np.zeros(shape=(len(time_grid)))
    usd_rmat[:] = usd_t0_curve.forwardRate(0, 0, ql.Continuous, ql.NoFrequency).rate()

    eur_rmat = np.zeros(shape=(len(time_grid)))
    eur_rmat[:] = eur_t0_curve.forwardRate(0, 0, ql.Continuous, ql.NoFrequency).rate()

    spotmat = np.zeros(shape=(len(time_grid)))
    spotmat[:] = eurusd_fx_spot
</pre></code>


#### For each counterparty, loop through dates and define NPVs (including Garman-Kohlagen process to build FX rate simulation for FxFwd) netting off exposure

<pre><code>
    for iT in range(1, len(time_grid)):
        mean = usd_rmat[iT - 1] * np.exp(- a * (time_grid[iT] - time_grid[iT - 1])) + \
               gamma(time_grid[iT]) - gamma(time_grid[iT - 1]) * \
               np.exp(- a * (time_grid[iT] - time_grid[iT - 1]))

        var = 0.5 * sigma * sigma / a * (1 - np.exp(-2 * a * (time_grid[iT] - time_grid[iT - 1])))
        rnew = mean + random_numbers[iT - 1] * np.sqrt(var)
        usd_rmat[iT] = rnew
        # USD discount factors as generated by HW model
        usd_disc_factors = [1.0] + [A(time_grid[iT], time_grid[iT] + k) *
                                    np.exp(- B(time_grid[iT], time_grid[iT] + k) * rnew) for k in
                                    range(1, maturity + 1)]

        mean = eur_rmat[iT - 1] * np.exp(- a * (time_grid[iT] - time_grid[iT - 1])) + \
               gamma(time_grid[iT]) - gamma(time_grid[iT - 1]) * \
               np.exp(- a * (time_grid[iT] - time_grid[iT - 1]))

        var = 0.5 * sigma * sigma / a * (1 - np.exp(-2 * a * (time_grid[iT] - time_grid[iT - 1])))
        rnew = mean + random_numbers[iT - 1] * np.sqrt(var)
        eur_rmat[iT] = rnew

        # EUR discount factors as generated by HW model
        eur_disc_factors = [1.0] + [A(time_grid[iT], time_grid[iT] + k) *
                                    np.exp(- B(time_grid[iT], time_grid[iT] + k) * rnew) for k in
                                    range(1, maturity + 1)]

        if dates[iT].serialNumber() > longest_swap_maturity.serialNumber():
            break

        # Reset the valuation date
        ql.Settings.instance().setEvaluationDate(dates[iT])
        crv_date = dates[iT]
        crv_dates = [crv_date] + [crv_date + ql.Period(k, ql.Years) for k in range(1, maturity + 1)]
        # use the new disc factors to build a new simulated curve
        usd_crv = ql.DiscountCurve(crv_dates, usd_disc_factors, ql.Actual365Fixed(), ql.UnitedStates())
        usd_crv.enableExtrapolation()
        eur_crv = ql.DiscountCurve(crv_dates, eur_disc_factors, ql.ActualActual(), ql.TARGET())
        eur_crv.enableExtrapolation()
        usd_disc_term_structure.linkTo(usd_crv)
        eur_disc_term_structure.linkTo(eur_crv)
        usdlibor3m.addFixings(fixing_dates, fixings)
        swap_engine = ql.DiscountingSwapEngine(usd_disc_term_structure)

        # build Garman-Kohlagen process for FX rate simulation for FxFwd
        gk_process = ql.GarmanKohlagenProcess(ql.QuoteHandle(ql.SimpleQuote(eurusd_fx_spot)),
                                              usd_disc_term_structure, eur_disc_term_structure, flat_vol_hyts)
        dt = time_grid[iT] - time_grid[iT - 1]
        spotmat[iT] = gk_process.evolve(time_grid[iT - 1], spotmat[iT - 1], dt, random_numbers[iT - 1])
        nettingset_npv = 0.
        for s in range(len(swaps)):
            if usdlibor3m.isValidFixingDate(dates[iT]):
                fixing = usdlibor3m.fixing(dates[iT])
                usdlibor3m.addFixing(dates[iT], fixing)
            swaps[s][0].setPricingEngine(swap_engine)
            nettingset_npv += swaps[s][0].NPV()

        if dates[iT].serialNumber() <= fx_maturity.serialNumber():
            fxfwd_exp = fxfwd_exposure(dates[iT], spotmat[iT], usd_crv)
            nettingset_npv += fxfwd_exp

        # Uncollateralised netting set NPV
        npvMat[iT, 0] = nettingset_npv

        collateral_held = collateral_held + collateral_posted
        nettingset_npv = nettingset_npv + IA2 - IA1

        # Eq. 8.6 Jon Gregory Counterparty CVA book 2nd edition
        collateral_required = max(nettingset_npv - threshold2, 0) \
                              - max(-nettingset_npv - threshold1, 0) - collateral_held
        collateral_posted = collateral_required
        if collateral_posted > 0:
            if collateral_posted < MTA2:
                collateral_posted = 0.
        elif -collateral_posted < MTA1:
            collateral_posted = 0.
        if collateral_posted != 0 and rounding != 0:
            collateral_posted = math.ceil(collateral_posted / rounding) * rounding
        if collateral_posted < 0:
            collateral_held = collateral_held + collateral_posted
            collateral_posted = 0.
        # collateralized netting set NPV
        npvMat[iT, 1] = nettingset_npv - collateral_held

    return np.array2string(npvMat, formatter={'float_kind': '{0:.6f}'.format})
</pre></code>

#### Define the NPV cube array

<pre><code>
    npv_list = random_array_rdd.map(lambda p: (calculate_potential_future_exposure(p, T, br_dict))).collect()

    npv_dataframe = sc.parallelize(npv_list)
  </pre></code>
  
#### write out the npv cube
  
  <pre><code>
    npv_dataframe.coalesce(1).saveAsTextFile(output_dir + '/exposure_scenarios')
</pre></code>


## Submitting the spark job

Copy the files in this repo to your s3 bucket and amend the 1504_SPARK_SUBMIT.sh file to point to your s3 bucket and tweak any settings (e.g. #simulations, memory sizes, core, # executors). Run the file. It will take somewhere from about 1-5 minutes for the Spark job to complete, depending on the hardware spec. 

After the spark job completes, it will create an "output" folder in your s3 bucket. The output files  are time-grid array and NPV cube. 

<pre><code>
# Adjust environment variables as needed
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3
export PYSPARK_PYTHON=python3
export PYTHONPATH=/usr/bin/python3
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export LD_LIBRARY_PATH=/usr/lib/hadoop/lib/native
export SPARK_HOME=/usr/lib/spark
export PFE_HOME=*/path/to/S3orlocal_drive*

# submit Spark job - adjust s3 bucket, filenames and scenario variables as needed
sudo spark-submit \
--deploy-mode client \
--master yarn \
--conf spark.driver.extraLibraryPath="${LD_LIBRARY_PATH}" \
--conf spark.executorEnv.LD_LIBRARY_PATH="${LD_LIBRARY_PATH}"  \
--num-executors 10 \
--conf spark.executor.memoryOverhead=5G \
--executor-memory 50G \
--conf spark.driver.memoryOverhead=2G \
--driver-memory 14G \
--executor-cores 16 \
--driver-cores 4 \
--conf spark.default.parallelism=168 \
$PFE_HOME/1504_PFE_CALC.py 5000 48 0.376739 0.0209835 \
$PFE_HOME/1504_USD_LIB_SWAP_CURVE.csv \
$PFE_HOME/1504_EUR_LIB_SWAP_CURVE.csv \
$PFE_HOME/1504_USD3MTD156N.csv \
$PFE_HOME/1504_INSTRUMENTS.csv \
$PFE_HOME/*your_output_dir*
</pre></code>

Note: this cluster design has not been optimised and is one of the areas I'd like to explore further. 

If you've like to test using Cassandra on *local* (it currently also writes to csv), then you need a slightly modified spark-submit file to pick up the cassandra drivers. Note some further work is required to connect to AWS keyspaces (AWS managed service for Cassandra) - e.g. see https://docs.aws.amazon.com/keyspaces/latest/devguide/using_python_driver.html

<pre><code>

spark-submit \
--deploy-mode client \
--packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.3 \
--conf spark.cassandra.connection.host=localhost \
--conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions \
$PFE_PATH/pfe_scenarios.py 10 48 0.376739 0.0209835 \
$PFE_PATH/1504_USD_LIB_SWAP_CURVE.csv \
$PFE_PATH/1504_EUR_LIB_SWAP_CURVE.csv \
$PFE_PATH/1504_USD3MTD156N.csv \
$PFE_PATH/1504_INSTRUMENTS.csv \
$PFE_PATH/output090720

</pre></code>

## Visualising the results

Change the destination in the 1504_POC_PLOT.py to your s3 bucket and run it (e.g. via bash, notebook, ipython terminal). Once we have the time grid and NPV cube in memory,  this script will visualise the simulated exposure paths. The Blue paths are for Collateralised exposures and Red are for Uncollateralised.

The program will output something like this:

* Maximum Uncollateralised PFE @95: $895,831
* Maximum Collateralised PFE @95:$778,937

![Estimated Potential Exposure](./visualisations/simex.png).

![Expected Exposure](./visualisations/epex.png).

![Potential Future Exposure - Collatoralised & Uncollateralised](./visualisations/pfe.png).


## Some ideas for ways to extend the POC

* Add more OTC derivative types e.g. Swing option for NYMEX gas forward for a time period starting in 6 months and ending in 12 months, credit default swap maturing in 10 years 
* Add database support to record the results of the PFE calculations for different counterparties (e.g. Cassandra - work in progress)
* Test larger numbers of products to simulate more real-life batch jobs
* Develop an API to enable the jobs to be triggered externally
* Extend the instruments file (or move it to Cassandra) to include all input variables e.g. alpha and sigma for Hull White model, numbers of simulations


