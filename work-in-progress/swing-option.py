# A swing option is a type of contract used by investors in energy markets that lets
# the option holder buy a predetermined quantity of energy at a predetermined price while
# retaining a certain degree of flexibility in the amount purchased and the price paid.
# A swing option contract delineates the least and most energy an option holder can buy
# (or "take") per day and per month, how much that energy will cost,
# and how many times during the month the option holder can change or "swing" the daily quantity
# of energy purchased.

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

# write column headings
def format_price(p, digits=2):
    fmt = "%%.%df" % digits
    return fmt % p


def format_rate(r, digits=2):
    fmt = "%%.%df %%%%" % digits
    return fmt % (r * 100)


def report_out(Info, bS, kM, fmt):
    if fmt == "Price":
        bS = format_price(bS)
        kM = format_price(kM)
    else:
        bS = format_rate(bS)
        kM = format_rate(kM)

    print("%19s" % Info + " |" + " |".join(["%15s" % y for y in [bS, kM]]))

#Config
conf = SparkConf().setAppName("swing-poc")
sc = SparkContext(conf=conf)
sc.setLogLevel('INFO')

#Load dates, options
nymex_curve_dates, nymex_rates = load_nymex_curve('/Users/forsmith/Documents/PotentialFutureExposureAWSSpark/work-in-progress/nymexhh-gas-forward-curve.csv')
nymex_curve_dates = [ql.DateParser.parseFormatted(r, '%Y-%m-%d') for r in nymex_curve_dates]
swing_options = load_swing_options('/Users/forsmith/Documents/PotentialFutureExposureAWSSpark/work-in-progress/instruments.csv')

#set up printing
headers = [" Black Scholes ", " Kluge Model "]
print("")
print("%19s" % "" + " |" + " |".join(["%10s" % y for y in headers]))
separator = " | "
widths = [20, 12, 12]
width = widths[0] + widths[1] + widths[2] + widths[2]
rule = "-" * width
dblrule = "=" * width
tab = " " * 2

# main loop through swing options in instruments for BS and KM pricing models
for s in range(len(swing_options)):
# Black Scholes price
    todays_date = str_to_qldate(swing_options[s][0])
    ql.Settings.instance().evaluationDate = todays_date
    settlement_date = todays_date
    ex_date = str_to_qldate(swing_options[s][1])
    volume_gas = swing_options[s][2]
    r_fr = swing_options[s][4]
    risk_free_rate = ql.FlatForward(settlement_date, r_fr, ql.Actual365Fixed())
    div_yield =  swing_options[s][5]
    dividend_yield = ql.FlatForward(settlement_date,div_yield, ql.Actual365Fixed())
    underlying = ql.SimpleQuote(swing_options[s][4]) #nymex spot price
    vol_bs = swing_options[s][6]
    volatility_bs = ql.BlackConstantVol(todays_date, ql.TARGET(), vol_bs, ql.Actual365Fixed())

    exercise_dates = [ex_date + i for i in range(60)]

    swing_option_object = ql.VanillaSwingOption(
        ql.VanillaForwardPayoff(ql.Option.Call, underlying.value()), ql.SwingExercise(exercise_dates), 0, len(exercise_dates)
    )

    bs_process = ql.BlackScholesMertonProcess(
      ql.QuoteHandle(underlying),
     ql.YieldTermStructureHandle(dividend_yield),
     ql.YieldTermStructureHandle(risk_free_rate),
        ql.BlackVolTermStructureHandle(volatility_bs),
    )

    swing_option_object.setPricingEngine(ql.FdSimpleBSSwingEngine(bs_process))
    bs_price = swing_option_object.NPV()

# Kluge Model Price
    x0 = swing_options[s][7] #0.08
    x1 = swing_options[s][8] #0.08
    beta = swing_options[s][9] #market risk 6
    eta = swing_options[s][10] #theta 5
    jump_intensity = swing_options[s][11] #2.5
    speed = swing_options[s][12] #kappa 1
    volatility_km = swing_options[s][13] #sigma 0.2
    grid_t = swing_options[s][14]
    grid_x = swing_options[s][15]
    grid_y = swing_options[s][16]

    curve_shape = []
    for d in exercise_dates:
        t = ql.Actual365Fixed().yearFraction(todays_date, d)
        gs = (
            math.log(underlying.value())
            - volatility_km * volatility_km / (4 * speed) * (1 - math.exp(-2 * speed * t))
            - jump_intensity / beta * math.log((eta - math.exp(-beta * t)) / (eta - 1.0))
        )
        curve_shape.append((t, gs))

    ou_process = ql.ExtendedOrnsteinUhlenbeckProcess(speed, volatility_km, x0, lambda x: x0)
    j_process = ql.ExtOUWithJumpsProcess(ou_process, x1, beta, jump_intensity, eta)

    swing_option_object.setPricingEngine(ql.FdSimpleExtOUJumpSwingEngine(j_process, risk_free_rate, grid_t, grid_x, grid_y, curve_shape))

    km_price = swing_option_object.NPV()
    # print price outputs
    print(rule)
    report_out("Swing Option " + str(s + 1), bs_price ,km_price , "Price")

print(rule)