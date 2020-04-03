from pyspark import SparkConf, SparkContext
from pyspark.mllib.random import RandomRDDs
from pyspark import AccumulatorParam
import QuantLib as ql
import datetime as dt
import numpy as np
import math
import sys

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

# Loads libor fixings from input file into an RDD and then collects the results
def loadLiborFixings(libor_fixings_file):

    libor_fixings = sc.textFile(libor_fixings_file) \
        .map(lambda line: line.split(",")) \
        .filter(lambda r: is_number(r[1])) \
        .map(lambda line: (str(line[0]), float(line[1]))).cache()

    fixingDates = libor_fixings.map(lambda r: r[0]).collect()
    fixings = libor_fixings.map(lambda r: r[1]).collect()
    return fixingDates, fixings

# Loads input swap specifications from input file into an RDD and then collects the results
def load_swaps(instruments_file):
    swaps = sc.textFile(instruments_file) \
            .map(lambda line: line.split(",")) \
            .filter(lambda r: r[0] == 'IRS') \
            .map(lambda line: (str(line[1]), str(line[2]), float(line[3]), float(line[4]), str(line[5])))\
            .collect()
    return swaps

# Loads input FxFwd specifications from input file into an RDD and then collects the results
def load_fxfwds(instruments_file):
    fxfwds = sc.textFile(instruments_file) \
        .map(lambda line: line.split(",")) \
        .filter(lambda r: r[0] == 'FXFWD') \
        .map(lambda line: (str(line[1]), str(line[2]), float(line[3]), float(line[4]), str(line[5]), str(line[6])))\
        .collect()
    return fxfwds

# Builds a QuantLib swap object from given specification
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

# main method invoked by Spark driver program
def main(sc, args_dict):
    # Broadcast dictionary obejct, which will hold various pure python objects
    # needed by the executors
    # QuantLib SWIG wrappers cannot be pickled and sent to the executotrs
    # so we use broadcast variables in Spark
    broadcast_dict = {}
    pytoday = dt.datetime(2017, 9, 8)
    broadcast_dict['python_today'] = pytoday
    today = ql.Date(pytoday.day, pytoday.month,pytoday.year)
    ql.Settings.instance().evaluationDate = today

    usd_calendar = ql.UnitedStates()
    usd_dc = ql.Actual365Fixed()
    eurusd_fx_spot = ql.SimpleQuote(1.1856)
    broadcast_dict['eurusd_fx_spot'] = eurusd_fx_spot.value()
    output_dir = args_dict['output_dir']
    instruments_file = args_dict['instruments_file']
    libor_fixings_file = args_dict['libor_fixings_file']

    # Loads USD libor swap curve from input file
    usd_swap_curve = sc.textFile(args_dict['usd_swap_curve_file']) \
        .map(lambda line: line.split(",")) \
        .filter(lambda r: is_number(r[1])) \
        .map(lambda line: (str(line[0]), float(line[1]))) \
        .cache()
    usd_curve_dates = usd_swap_curve.map(lambda r: r[0]).collect()
    usd_disc_factors = usd_swap_curve.map(lambda r: r[1]).collect()
    broadcast_dict['usd_curve_dates'] = usd_curve_dates
    broadcast_dict['usd_disc_factors'] = usd_disc_factors
    usd_crv_today = ql.DiscountCurve([ql.DateParser.parseFormatted(x,'%Y-%m-%d')
                                      for x in usd_curve_dates] , usd_disc_factors, usd_dc, usd_calendar )
    usd_disc_term_structure = ql.RelinkableYieldTermStructureHandle(usd_crv_today)

    # Loads EUR libor swap curve from input file
    eur_swap_curve = sc.textFile(args_dict['eur_swap_curve_file']) \
        .map(lambda line: line.split(",")) \
        .filter(lambda r: is_number(r[1])) \
        .map(lambda line: (str(line[0]), float(line[1]))) \
        .cache()
    eur_curve_dates = eur_swap_curve.map(lambda r: r[0]).collect()
    eur_disc_factors = eur_swap_curve.map(lambda r: r[1]).collect()
    broadcast_dict['eur_curve_dates'] = eur_curve_dates
    broadcast_dict['eur_disc_factors'] = eur_disc_factors

    # Build QuantLib index object
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

    Nsim = int(args_dict['NSim'])
    NPartitions = int(args_dict['NPartitions'])
    a = float(args_dict['a']) #0.376739
    sigma = float(args_dict['sigma']) #0.0209835
    broadcast_dict['a'] = a
    broadcast_dict['sigma'] = sigma

    # Simulate swap NPVs until we reach the longest maturity
    years_to_sim = math.ceil(ql.Actual360().yearFraction(today,longest_swap_maturity))

    dates = [today + ql.Period(i, ql.Weeks) for i in range(0, 52 * int(years_to_sim))]
    # Very important to add swap reset dates to our universe of dates
    for idx in xrange(len(swaps)):
        dates += swaps[idx][1]
    dates = np.unique(np.sort(dates))
    broadcast_dict['dates'] = [ql_to_datetime(x) for x in dates]

    br_dict = sc.broadcast(broadcast_dict)

    # Write out the time grid to a text file which can be parsed later
    T = [ql.Actual360().yearFraction(today, dates[i]) for i in xrange(1, len(dates))]
    temp_rdd = sc.parallelize(T)
    # coalesce with shrink the partition size to 1 so we have only 1 file to write and parse later
    temp_rdd.coalesce(1).saveAsTextFile(output_dir +'/time-grid')

    # the main routine, generate a matrix of normally distributed random numbers and
    # spread them across the cluster
    # we are not setting the number of partitions here as the default parallelism should be enough
    randArrayRDD = RandomRDDs.normalVectorRDD(sc, Nsim, len(T), seed=1L)

    # for each row of the matrix, which corresponds to one simulated path,
    # compute netting set NPV (collateralized and uncollateralized)
    npv_cube = randArrayRDD.map(lambda p: (calc_exposure(p,T,br_dict)))
    # write out the npv cube, remove '[' and ']' added by numpy array to ease parsing later
    npv_cube.coalesce(1).saveAsTextFile(output_dir + '/npv_cube')


def calc_exposure(rnumbers, time_grid, br_dict):

    # get hold of broadcasted dictionary and rebuild the curves, libor index, swaps and FxFwd
    # we need to do this since QuantLib SWIG objects cannot be passed around

    broadcast_dict = br_dict.value
    today = py_to_qldate(broadcast_dict['python_today'])
    ql.Settings.instance().setEvaluationDate(today)
    usd_calendar = ql.UnitedStates()
    usd_dc = ql.Actual365Fixed()
    eur_calendar = ql.TARGET()
    eur_dc = ql.ActualActual()

    maturity = 10
    a = broadcast_dict['a']
    sigma = broadcast_dict['sigma']
    dates = [py_to_qldate(x) for x in broadcast_dict['dates']]
    longest_swap_maturity = py_to_qldate(broadcast_dict['longest_swap_maturity'])
    fixing_dates = [py_to_qldate(x) for x in broadcast_dict['fixing_dates']]
    fixings = broadcast_dict['fixings']

    usd_curve_dates = broadcast_dict['usd_curve_dates']
    usd_disc_factors = broadcast_dict['usd_disc_factors']
    usd_t0_curve = ql.DiscountCurve([ql.DateParser.parseFormatted(x, '%Y-%m-%d')
                        for x in usd_curve_dates], usd_disc_factors, usd_dc, usd_calendar)
    usd_t0_curve.enableExtrapolation()
    usd_disc_term_structure = ql.RelinkableYieldTermStructureHandle(usd_t0_curve)

    eur_curve_dates = broadcast_dict['eur_curve_dates']
    eur_disc_factors = broadcast_dict['eur_disc_factors']
    eur_t0_curve = ql.DiscountCurve([ql.DateParser.parseFormatted(x, '%Y-%m-%d')
                                     for x in eur_curve_dates], eur_disc_factors, eur_dc, eur_calendar)
    eur_t0_curve.enableExtrapolation()
    eur_disc_term_structure = ql.RelinkableYieldTermStructureHandle(eur_t0_curve)

    eurusd_fx_spot = broadcast_dict['eurusd_fx_spot']
    flat_vol_hyts = ql.BlackVolTermStructureHandle(ql.BlackConstantVol(today, usd_calendar, 0.20, usd_dc))
    usdlibor3m = ql.USDLibor(ql.Period(3, ql.Months), usd_disc_term_structure)
    usdlibor3m.addFixings(fixing_dates, fixings)
    engine = ql.DiscountingSwapEngine(usd_disc_term_structure)
    swaps = broadcast_dict['swaps']
    swaps = [
        makeSwap(today, ql.DateParser.parseFormatted(swap[0], '%Y-%m-%d'),
                 ql.Period(swap[1]), swap[2], swap[3], usdlibor3m,
                 ql.VanillaSwap.Payer if swap[4] == 'Payer' else ql.VanillaSwap.Receiver)
        for swap in swaps
    ]

    # I know there is only one fxfwd in the instruments file
    fxfwds = broadcast_dict['fxfwds']
    fx_startdate = ql.DateParser.parseFormatted(fxfwds[0][0], '%Y-%m-%d')
    fx_tenor = ql.Period(fxfwds[0][1])
    fx_notional = fxfwds[0][2]
    fwd_rate = fxfwds[0][3]
    fx_maturity = usd_calendar.advance(fx_startdate, fx_tenor, ql.Following)

    # define the NPV cube array
    # number of rows = number of dates in time-grid
    # number of columns = 2 (collateralized and uncollateralized NPV)
    npvMat = np.zeros((len(time_grid),2), dtype=np.float64)

    # utility method to calc FxFwd exposure
    def fxfwd_exposure(date, spot, usd_curve):
        usd_zero_rate = usd_curve.zeroRate(usd_dc.yearFraction(date, fx_maturity),
                                           ql.Compounded, ql.Annual).rate()
        yf = usd_dc.yearFraction(date, fx_maturity)
        fwd_points = fwd_rate - spot
        fxfwd_npv = ((spot + fwd_points) * fx_notional) / (1 + (usd_zero_rate * yf))
        fxfwd_exp = (fx_notional * eurusd_fx_spot) - fxfwd_npv
        return fxfwd_exp

    # Intialize NPV cube with today's NPVs
    nettingset_npv = 0.
    for idx in xrange(len(swaps)):
        swaps[idx][0].setPricingEngine(engine)
        nettingset_npv += swaps[idx][0].NPV()
    fxfwd_exp = fxfwd_exposure(today, eurusd_fx_spot, usd_t0_curve)
    nettingset_npv += fxfwd_exp
    npvMat[0,0] = nettingset_npv
    # assume 100K collateral has been posted already
    npvMat[0,1] = nettingset_npv - 100000.0

    # Hull White parameter estimations
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

    # CSA terms for Collateral movements
    IA1 = 0; IA2 = 0; threshold1 = 100000.0; threshold2 = 100000.0; MTA1 = 25000.0; MTA2 = 25000.0;rounding = 5000.0
    collateral_held = IA2 - IA1
    # assume collateral posted last week was 100K, a reasonable number given the exposure was about
    # 280K and 50,000 Threshold
    collateral_posted = 100000.0


    # the main loop of NPV computations
    for iT in xrange(1, len(time_grid)):
        mean = usd_rmat[iT - 1] * np.exp(- a * (time_grid[iT] - time_grid[iT - 1])) + \
               gamma(time_grid[iT]) - gamma(time_grid[iT - 1]) * \
                                        np.exp(- a * (time_grid[iT] - time_grid[iT - 1]))

        var = 0.5 * sigma * sigma / a * (1 - np.exp(-2 * a * (time_grid[iT] - time_grid[iT - 1])))
        rnew = mean + rnumbers[iT-1] * np.sqrt(var)
        usd_rmat[iT] = rnew
        # USD discount factors as generated by HW model
        usd_disc_factors = [1.0] + [A(time_grid[iT], time_grid[iT] + k) *
                                np.exp(- B(time_grid[iT], time_grid[iT] + k) * rnew) for k in xrange(1, maturity + 1)]

        mean = eur_rmat[iT - 1] * np.exp(- a * (time_grid[iT] - time_grid[iT - 1])) + \
               gamma(time_grid[iT]) - gamma(time_grid[iT - 1]) * \
                                      np.exp(- a * (time_grid[iT] - time_grid[iT - 1]))

        var = 0.5 * sigma * sigma / a * (1 - np.exp(-2 * a * (time_grid[iT] - time_grid[iT - 1])))
        rnew = mean + rnumbers[iT - 1] * np.sqrt(var)
        eur_rmat[iT] = rnew
        # EUR discount factors as generated by HW model
        eur_disc_factors = [1.0] + [A(time_grid[iT], time_grid[iT] + k) *
                                    np.exp(- B(time_grid[iT], time_grid[iT] + k) * rnew) for k in xrange(1, maturity + 1)]

        if dates[iT].serialNumber() > longest_swap_maturity.serialNumber():
            break

        # very important to reset the valuation date
        ql.Settings.instance().setEvaluationDate(dates[iT])
        crv_date = dates[iT]
        crv_dates = [crv_date] + [crv_date + ql.Period(k, ql.Years) for k in xrange(1, maturity + 1)]
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
        if collateral_posted <> 0. and rounding <> 0.:
            collateral_posted = math.ceil(collateral_posted/rounding) * rounding
        if collateral_posted < 0:
            collateral_held = collateral_held + collateral_posted
            collateral_posted = 0.
        # collateralized netting set NPV
        npvMat[iT,1] = nettingset_npv - collateral_held
    return np.array2string(npvMat, formatter={'float_kind':'{0:.6f}'.format})


if __name__ == "__main__":

    if len(sys.argv) != 10:
        print('Usage: ' + sys.argv[0] + ' <num_of_simulations> <num_of_partitions> <hull_white_a> <hull_white_sigma> <usd_swap_curve><eur_swap_curve><libor_fixings><instruments><output_dir>')
        sys.exit(1)
    conf = SparkConf().setAppName("SPARK-PFE")
    sc  = SparkContext(conf=conf)
    sc.setLogLevel('INFO')

    args = {'NSim':sys.argv[1], 'NPartitions': sys.argv[2], 'a': sys.argv[3], 'sigma':sys.argv[4],
            'usd_swap_curve_file': sys.argv[5], 'eur_swap_curve_file': sys.argv[6], 'libor_fixings_file': sys.argv[7],
            'instruments_file': sys.argv[8], 'output_dir': sys.argv[9]}

    main(sc, args)


