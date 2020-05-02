# Potential Future Exposure estimation using QuantLib, AWS Elastic Map Reduce & Spark

*A proof of concept ("POC") for estimating potential future exposure ("PFE") with QuantLib and AWS Elastic Map Reduce ("EMR").*

This will cover:
* The methods used for determing the potential exposure over different time periods for different over-the-counter ("OTC") products (for this exercise, two interest rates swaps and one foreign exchange forward ("FXFwd")
* The steps needed to build and run the infrastructure and software and then analyse the data outputs
* The proposed next steps to further extend this POC further

## Context (you can skip this bit if you work in capital markets :wink: )

The first decade of the 21st Century was  disastrous for financial institutions, derivatives and risk management. Counterparty credit risk has become the key element of financial risk management, highlighted by the bankruptcy of the investment bank Lehman Brothers and failure of other high profile institutions such as Bear Sterns, AIG, Fannie Mae and Freddie Mac. The sudden realisation of extensive counterparty risks, primarily from over-the-counter ("OTC") products, severely compromised the health of global financial markets. Estimating potential future exposure is now a key task for all financial institutions.

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

### Amazon EMR & PFE

EMR provides a managed Hadoop framework that makes it easy, fast, and cost-effective to process vast amounts of data across dynamically scalable Amazon EC2 instances, like the data analysis required to estimate FPE. You can also run other popular distributed frameworks such as Apache Spark, Zepplin, Hive and interact with data in other AWS data stores such as Amazon S3 and Amazon DynamoDB.

If you use common big data Apache tools, consider Amazon EMR because it simplifies the configuration process. A big advantage of AWS over other platforms for the enterprise user, especially in highly regulated financial services domains, are the robust security & permissions features. 

That’s not to say it is always easy to set up, though. :sweat:

The field of quantitative finance upon which this POC rests is vast and quickly becomes exceedingly complex! A key theoretical references is: https://www.amazon.com/Counterparty-Credit-Risk-challenge-financial/dp/047068576X

This work would not have been possible without the specific help, Python design and examples from:
* https://www.implementingquantlib.com/ - thanks to Luigi for being the "father" of Quantlib and for replying to my questions
* https://quantlib-python-docs.readthedocs.io/en/latest/
* http://gouthamanbalaraman.com/blog/hull-white-simulation-quantlib-python.html
* http://suhasghorp.com/estimating-potential-future-exposure-with-quantlib-and-aws-em/
* https://ipythonquant.wordpress.com/2015/04/08/expected-exposure-and-pfe-simulation-with-quantlib-and-python/

...and all the people who helped on Stack Overflow or otherwise. 


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

## Installing Boost, QuantLib and other packages - Creating the Amazon Machine Image ("AMI")

Once you're connect to the instance, you'll install the base software required. 

ssh into the box using your local PEM file and the specific machine address. e.g. `ssh -i /Users/XX/XX.pem ec2-user@ec2-XX-XX-XXX-XXX.ap-southeast-2.compute.amazonaws.com`. 

Run the 1504_PFE_INSTALL_PACKAGES.sh script either by downloading it (e.g. `wget https://raw.githubusercontent.com/fordesmith/PotentialFutureExposureAWSSpark/master/1304instpack_pip.sh`) and running it (e.g. sudo bash ./1504_PFE_INSTALL_PACKAGES.sh).

The AMI provides the base packages required for the cluster instance. 

Once complete, choose "Image > Create Image" to save an AMI to use for your cluster. 

## Setting up the cluster

Go to EMR and "Create Cluster". Go to "Advanced Options". In "software configuration" choose release 6.0.0 plus check "Hadoop", "Hive", "Hue", "Zepplin" and "Spark". Choose "next". In "Hardware" choose 4 x core nodes. Choose servers with at least 16GB memory. The disk size needs to be as big or bigger than the AMI image (e.g. >=8G).

For my test I used the following config: 
** Executors: tried both 4 and 14 servers
m5a.4xlarge
16 vCore, 64 GiB memory, EBS only storage
EBS Storage:256 GiB

** Driver / Master: 1
m5.xlarge
4 vCore, 16 GiB memory, EBS only storage
EBS Storage:64 GiB

Leave other settings as is. Choose "next". In "Additional Options" choose the AMI you created above. Choose "next". In "Security Options" choose the key pair you created and downloaded. Then "create cluster". 

A spark cluster has n nodes managed by a central master. This allows it offer large scale parallel processing. 

![Spark Cluster Diagram](https://raw.githubusercontent.com/fordesmith/PotentialFutureExposureAWSSpark/master/visualisations/cluster-overview.png).

For the example here, the job completes in ~1 minute with 14 workers and ~2 minutes with 4 workers. In the first few runs the core NPV was only running on one node and the job took 30 minutes and as a result didn`t speed up when adding up to 30 worker nodes. I tweaked the python code to ensure it spread work across the cluster, but apart from that, I haven`t optimised the job, so you may be able to make it run faster. 

## Creating the input files

The inputs needed for this POC are: 
* Historical data - in this case for the interest rate swaps, 3M USD / Libor fixings e.g. from here: https://fred.stlouisfed.org/series/USD3MTD156N
* Future LIBOR swap curves for USD and EUR (in this case out to 2070)
* The list of instruments

If you are adjusting the files here or developing your own, you will need to formats dates as YYYY-MM-DD otherwise QuantLib won`t be able to parse the data. 

## The key aspects of the PFE script for running the simulations

### Gather inputs

#### Load modules

<pre><code>

from pyspark import SparkConf, SparkContext
from pyspark.mllib.random import RandomRDDs
from pyspark import AccumulatorParam
import QuantLib as ql
import datetime as dt
import numpy as np
import math
import sys

</pre></code>

#### Define and set start date

<pre><code>

   broadcast_dict = br_dict.value
    today = py_to_qldate(broadcast_dict['python_today'])
    ql.Settings.instance().setEvaluationDate(today)
    usd_calendar = ql.UnitedStates()
    usd_dc = ql.Actual365Fixed()
    eur_calendar = ql.TARGET()
    eur_dc = ql.ActualActual()
    
</pre></code>

#### Load historical libor rates, swap specifications and FxFwd specifications from input file into an RDD and collect the results

<pre><code>

def loadLiborFixings(libor_fixings_file):

    libor_fixings = sc.textFile(libor_fixings_file) \
        .map(lambda line: line.split(",")) \
        .filter(lambda r: is_number(r[1])) \
        .map(lambda line: (str(line[0]), float(line[1]))).cache()

    fixingDates = libor_fixings.map(lambda r: r[0]).collect()
    fixings = libor_fixings.map(lambda r: r[1]).collect()
    return fixingDates, fixings


def load_swaps(instruments_file):
    swaps = sc.textFile(instruments_file) \
            .map(lambda line: line.split(",")) \
            .filter(lambda r: r[0] == 'IRS') \
            .map(lambda line: (str(line[1]), str(line[2]), float(line[3]), float(line[4]), str(line[5])))\
            .collect()
    return swaps

def load_fxfwds(instruments_file):
    fxfwds = sc.textFile(instruments_file) \
        .map(lambda line: line.split(",")) \
        .filter(lambda r: r[0] == 'FXFWD') \
        .map(lambda line: (str(line[1]), str(line[2]), float(line[3]), float(line[4]), str(line[5]), str(line[6])))\
        .collect()
    return fxfwds
    
</pre></code>
    
** Daily Libor rates are 3M USD Libor rates for previous years
** Swap specifications: commencement date, term, amount, fixed rate, type (pay, receive)
** FxFwd specifications: commencement date, term, amount, rate, currency 1, currency 2
** Load USD, EUR libor swap curve from input file

 
### Prepare objects from inputs

#### Build a QuantLib swap and index object

<pre><code>

def makeSwap(today, start, maturity, nominal, fixedRate, index, typ=ql.VanillaSwap.Payer):
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

#### Build QuantLib index object

<pre><code>

    usdlibor3m = ql.USDLibor(ql.Period(3, ql.Months), usd_disc_term_structure)
    # don't need EUR fixings since we don't have a EUR swap
    fixingDates, fixings = loadLiborFixings(libor_fixings_file)
    fixingDates = [ql.DateParser.parseFormatted(r, '%Y-%m-%d') for r in fixingDates]
    three_month_old_date = usd_calendar.advance(today, -90, ql.Days, ql.ModifiedFollowing)
    latestFixingDates = fixingDates[fixingDates.index(three_month_old_date):]
    latestFixings = fixings[fixingDates.index(three_month_old_date):]
    usdlibor3m.addFixings(latestFixingDates, latestFixings)
    broadcast_dict['fixing_dates'] = [ql_to_datetime(x) for x in latestFixingDates]
    broadcast_dict['fixings'] = latestFixings

    swaps = load_swaps(instruments_file)
    broadcast_dict['swaps'] = swaps
    fxfwds = load_fxfwds(instruments_file)
    broadcast_dict['fxfwds'] = fxfwds

    swaps = [
                makeSwap(today, ql.DateParser.parseFormatted(swap[0], '%Y-%m-%d'),
                     ql.Period(swap[1]), swap[2], swap[3], usdlibor3m,
                     ql.VanillaSwap.Payer if swap[4] == 'Payer' else ql.VanillaSwap.Receiver)
                for swap in swaps
            ]

    longest_swap_maturity = max([s[0].maturityDate() for s in swaps])
    broadcast_dict['longest_swap_maturity'] = ql_to_datetime(longest_swap_maturity)
    
 </pre></code>
    
#### Generate a matrix of normally distributed random numbers and spread them across the cluster. I've used the default settings from Spark for the partitions. 

<pre><code>
  
  randArrayRDD = RandomRDDs.normalVectorRDD(sc, Nsim, len(T), seed=1)

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
    usd_rmat[:] = usd_t0_curve.forwardRate(0,0, ql.Continuous, ql.NoFrequency).rate()

    eur_rmat = np.zeros(shape=(len(time_grid)))
    eur_rmat[:] = eur_t0_curve.forwardRate(0, 0, ql.Continuous, ql.NoFrequency).rate()

    spotmat = np.zeros(shape=(len(time_grid)))
    spotmat[:] = eurusd_fx_spot
    
</pre></code>


#### Loop through dates and define NPVs (including Garman-Kohlagen process to build FX rate simulation for FxFwd)

<pre><code>

    for iT in range(1, len(time_grid)):
        mean = usd_rmat[iT - 1] * np.exp(- a * (time_grid[iT] - time_grid[iT - 1])) + \
               gamma(time_grid[iT]) - gamma(time_grid[iT - 1]) * \
                                        np.exp(- a * (time_grid[iT] - time_grid[iT - 1]))

        var = 0.5 * sigma * sigma / a * (1 - np.exp(-2 * a * (time_grid[iT] - time_grid[iT - 1])))
        rnew = mean + rnumbers[iT-1] * np.sqrt(var)
        usd_rmat[iT] = rnew
        # USD discount factors as generated by HW model
        usd_disc_factors = [1.0] + [A(time_grid[iT], time_grid[iT] + k) *
                                np.exp(- B(time_grid[iT], time_grid[iT] + k) * rnew) for k in range(1, maturity + 1)]

        mean = eur_rmat[iT - 1] * np.exp(- a * (time_grid[iT] - time_grid[iT - 1])) + \
               gamma(time_grid[iT]) - gamma(time_grid[iT - 1]) * \
                                      np.exp(- a * (time_grid[iT] - time_grid[iT - 1]))

        var = 0.5 * sigma * sigma / a * (1 - np.exp(-2 * a * (time_grid[iT] - time_grid[iT - 1])))
        rnew = mean + rnumbers[iT - 1] * np.sqrt(var)
        eur_rmat[iT] = rnew
        # EUR discount factors as generated by HW model
        eur_disc_factors = [1.0] + [A(time_grid[iT], time_grid[iT] + k) *
                                    np.exp(- B(time_grid[iT], time_grid[iT] + k) * rnew) for k in range(1, maturity + 1)]

        if dates[iT].serialNumber() > longest_swap_maturity.serialNumber():
            break

        # very important to reset the valuation date
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
        spotmat[iT] = gk_process.evolve(time_grid[iT - 1], spotmat[iT - 1], dt, rnumbers[iT - 1])
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

        # uncollateralized netting set NPV
        npvMat[iT,0] = nettingset_npv

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
            collateral_posted = math.ceil(collateral_posted/rounding) * rounding
        if collateral_posted < 0:
            collateral_held = collateral_held + collateral_posted
            collateral_posted = 0.
        # collateralized netting set NPV
        npvMat[iT,1] = nettingset_npv - collateral_held
        
    return np.array2string(npvMat, formatter={'float_kind':'{0:.6f}'.format})

</pre></code>

#### Define the NPV cube array

<pre><code>

    npv_list = randArrayRDD.map(lambda p: (calc_exposure(p, T, br_dict))).collect()
    
    npv_dataframe = sc.parallelize(npv_list)
  
  </pre></code>
  
#### write out the npv cube
  
  <pre><code>
 
    npv_dataframe.coalesce(1).saveAsTextFile(output_dir + '/npv_cube')
    
</pre></code>


## Submitting the spark job

Copy the files in this repo to your s3 bucket and amend the 1504_SPARK_SUBMIT.sh file to point to your s3 bucket and tweak any settings (e.g. #simulations, memory sizes, core, # executors). Run the file. It will take somewhere from about 1-5 minutes for the Spark job to complete, depending on the hardware spec. It computes a netting set NPV for 5000 simulations across 454 future dates for 2 swaps and 1 FxFwd.  

After the spark job completes, it will create an "output" folder in your s3 bucket. The output files  are time-grid array and NPV cube. 

<pre><code>

# Adjust environment variables as needed
# LD_LIBRARY_PATH=/usr/local/lib
# export LD_LIBRARY_PATH=$LD_LIBRARY_PATH
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3
export PYSPARK_PYTHON=python3
export PYTHONPATH=/usr/bin/python3
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export LD_LIBRARY_PATH=/usr/lib/hadoop/lib/native
export SPARK_HOME=/usr/lib/spark


# submit Spark job - adjust s3 bucket, filenames and scenario variables as needed
sudo spark-submit \
--deploy-mode client \
--master yarn \
--conf spark.driver.extraLibraryPath="${LD_LIBRARY_PATH}" \
--conf spark.executorEnv.LD_LIBRARY_PATH="${LD_LIBRARY_PATH}"  \
--num-executors 2 \
--conf spark.executor.memoryOverhead=5G \
--executor-memory 50G \
--conf spark.driver.memoryOverhead=2G \
--driver-memory 14G \
--executor-cores 16 \
--driver-cores 4 \
--conf spark.default.parallelism=168 \
s3://pfe2020/1504_PFE_CALC.py 5000 48 0.376739 0.0209835 \
s3://pfe2020/1504_USD_LIB_SWAP_CURVE.csv \
s3://pfe2020/1504_EUR_LIB_SWAP_CURVE.csv \
s3://pfe2020/1504_USD3MTD156N.csv \
s3://pfe2020/1504_INSTRUMENTS.csv \
s3://pfe2020/output0105

</pre></code>

Note: this cluster design has not been optimised and is one of the areas I'd like to explore further. 

## Visualising the results

Change the destination in the 1504_POC_PLOT.py to your s3 bucket and run it (e.g. via bash, notebook, ipython terminal). Once we have the time grid and NPV cube in memory,  this script will visualize the simulated exposure paths. The Blue paths are for Collateralised exposures and Red are for Uncollateralised.

The progran will output something like this:

* Maximum Uncollateralised PFE @95: $895,831
* Maximum Collateralised PFE @95:$778,937

![Estimated Potential Exposure](https://raw.githubusercontent.com/fordesmith/PotentialFutureExposureAWSSpark/master/visualisations/simex.png).

![Expected Exposure](https://raw.githubusercontent.com/fordesmith/PotentialFutureExposureAWSSpark/master/visualisations/epex.png).

![Potential Future Exposure - Collatoralised & Uncollateralised](https://raw.githubusercontent.com/fordesmith/PotentialFutureExposureAWSSpark/master/visualisations/pfe.png).


## Planned steps to extend the POC

* Add more derivative types e.g. 
** Credit default swap maturing in 10 years 
** 10-year interest rate swap, forward starting in 5 years 
** Forward rate agreement for NYMEX gas for a time period starting in 6 months and ending in 12 months 
** Cash-settled European swaption referencing 5-year interest rate swap with exercise date in 6 months
** Physically-settled European swaption referencing 5-year interest rate swap with exercise date in 6 months
** Bermudan swaption for commodity forward with annual exercise dates 
** Interest rate cap or floor specified for semi-annual interest rate with maturity
** Option on a bond maturing in 5 years with the latest exercise date in 1 year 
** 3-month Eurodollar futures that matures in 1 year  
** Futures on 20-year treasury bond that matures in 2 years 
** 6-month option on 2-year futures on 20-year treasury bond 

* Extend to and create reports for different counterparties
* Tune the spark cluster so it performs faster



