# What Is a Swing Option?
# A swing option is a type of contract used by investors in energy markets that lets
# the option holder buy a predetermined quantity of energy at a predetermined price while
# retaining a certain degree of flexibility in the amount purchased and the price paid.
# A swing option contract delineates the least and most energy an option holder can buy
# (or "take") per day and per month, how much that energy will cost (known as its strike price),
# and how many times during the month the option holder can change or "swing" the daily quantity
# of energy purchased.
# How Swing Options Work
# Swing options (also known as “swing contracts,” “take-and-pay options” or
# “variable base-load factor contracts”) are most commonly used for the purchase of oil,
# natural gas, and electricity. They may be used as hedging instruments by the option holder,
# to protect against price changes in these commodities.

import QuantLib as ql
import math
from pyspark import SparkConf, SparkContext
import datetime as dt

# Used in loading the various input text files
def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

# QuantLib date to Python date
def ql_to_datetime(d):
    return dt.datetime(d.year(), d.month(), d.dayOfMonth())

# Python date to QuantLib date
def py_to_qldate(d):
    return ql.Date(d.day, d.month, d.year)

# string to Python date to QuantLib date
def str_to_qldate(strd):
    d = dt.datetime.strptime(strd, '%d-%m-%Y')
    return ql.Date(d.day, d.month, d.year)

# Loads NYMEX fixings from input file into an RDD and then collects the results
def load_nymex_curve(nymex_curve_file):
    nymex_curve = sc.textFile(nymex_curve_file) \
        .map(lambda line: line.split(",")) \
        .filter(lambda r: is_number(r[1])) \
        .map(lambda line: (str(line[0]), float(line[1]))).cache()

    dates = nymex_curve.map(lambda r: r[0]).collect()
    rates = nymex_curve.map(lambda r: r[1]).collect()
    return dates, rates

# Loads input swap specifications from input file into an RDD and then collects the results
def load_swing_options(instruments_file):
    swingO = sc.textFile(instruments_file) \
        .map(lambda line: line.split(",")) \
        .filter(lambda r: r[0] == 'SWING') \
        .map(lambda line: (str(line[1]), str(line[2]), float(line[3]), float(line[4]), float(line[5]), float(line[6]), float(line[7]), float(line[8]), float(line[9]), float(line[10]), float(line[11]), float(line[12]), float(line[13]), float(line[14]), int(line[15]), int(line[16]), int(line[17]))).cache() \
        .collect()
    return swingO

conf = SparkConf().setAppName("swing-poc")
sc = SparkContext(conf=conf)
sc.setLogLevel('INFO')

nymexCurveDates, nymexRates = load_nymex_curve('/Users/forsmith/Documents/PotentialFutureExposureAWSSpark/work-in-progress/nymexhh-gas-forward-curve.csv')
nymexCurveDates = [ql.DateParser.parseFormatted(r, '%Y-%m-%d') for r in nymexCurveDates]
swingOpts = load_swing_options('/Users/forsmith/Documents/PotentialFutureExposureAWSSpark/work-in-progress/instruments.csv')

for s in range(len(swingOpts)):
    todaysDate = str_to_qldate(swingOpts[s][0])
    ql.Settings.instance().evaluationDate = todaysDate
    settlementDate = todaysDate
    exDate = str_to_qldate(swingOpts[s][1])
    volumeGas = swingOpts[s][2]
    rFR = swingOpts[s][4]
    riskFreeRate = ql.FlatForward(settlementDate, rFR, ql.Actual365Fixed())
    dYLD =  swingOpts[s][5]
    dividendYield = ql.FlatForward(settlementDate,dYLD, ql.Actual365Fixed())
    underlying = ql.SimpleQuote(swingOpts[s][4]) #nymex spot price
    vOL = swingOpts[s][6]
    volatility = ql.BlackConstantVol(todaysDate, ql.TARGET(), vOL, ql.Actual365Fixed())

    exerciseDates = [exDate + i for i in range(60)]

    swingOption = ql.VanillaSwingOption(
        ql.VanillaForwardPayoff(ql.Option.Call, underlying.value()), ql.SwingExercise(exerciseDates), 0, len(exerciseDates)
    )

    bsProcess = ql.BlackScholesMertonProcess(
      ql.QuoteHandle(underlying),
     ql.YieldTermStructureHandle(dividendYield),
     ql.YieldTermStructureHandle(riskFreeRate),
        ql.BlackVolTermStructureHandle(volatility),
    )

    swingOption.setPricingEngine(ql.FdSimpleBSSwingEngine(bsProcess))

    print("Swing Option " + str(s+1) + " - Black Scholes Price: %f" % swingOption.NPV())

# Kluge Model Price

    x0 = swingOpts[s][7] #0.08
    x1 = swingOpts[s][8] #0.08
    beta = swingOpts[s][9] #market risk 6
    eta = swingOpts[s][10] #theta 5
    jumpIntensity = swingOpts[s][11] #2.5
    speed = swingOpts[s][12] #kappa 1
    volatility = swingOpts[s][13] #sigma 0.2
    gridT = swingOpts[s][14]
    gridX = swingOpts[s][15]
    gridY = swingOpts[s][16]

    curveShape = []
    for d in exerciseDates:
        t = ql.Actual365Fixed().yearFraction(todaysDate, d)
        gs = (
            math.log(underlying.value())
            - volatility * volatility / (4 * speed) * (1 - math.exp(-2 * speed * t))
            - jumpIntensity / beta * math.log((eta - math.exp(-beta * t)) / (eta - 1.0))
        )
        curveShape.append((t, gs))

    ouProcess = ql.ExtendedOrnsteinUhlenbeckProcess(speed, volatility, x0, lambda x: x0)
    jProcess = ql.ExtOUWithJumpsProcess(ouProcess, x1, beta, jumpIntensity, eta)

    swingOption.setPricingEngine(ql.FdSimpleExtOUJumpSwingEngine(jProcess, riskFreeRate, gridT, gridX, gridY, curveShape))

    print("Swing Option " + str(s+1) + " - Kluge Model Price  : %f" % swingOption.NPV())