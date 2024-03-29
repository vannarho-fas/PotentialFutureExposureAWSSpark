
import QuantLib as ql

swaptionVols = [
    # maturity,          length,             volatility
    (ql.Period(1, ql.Years), ql.Period(5, ql.Years), 0.1148),
    (ql.Period(2, ql.Years), ql.Period(4, ql.Years), 0.1108),
    (ql.Period(3, ql.Years), ql.Period(3, ql.Years), 0.1070),
    (ql.Period(4, ql.Years), ql.Period(2, ql.Years), 0.1021),
    (ql.Period(5, ql.Years), ql.Period(1, ql.Years), 0.1000),
]


def formatVol(v, digits=2):
    fmt = "%%.%df %%%%" % digits
    return fmt % (v * 100)


def formatPrice(p, digits=2):
    fmt = "%%.%df" % digits
    return fmt % p


def calibrate(model, helpers, l, name):

    fmt = "%12s |%12s |%12s |%12s |%12s"
    header = fmt % ("maturity", "length", "volatility", "implied", "error")
    rule = "-" * len(header)
    dblrule = "=" * len(header)

    print("")
    print(dblrule)
    print(name)
    print(rule)

    method = ql.Simplex(l)
    model.calibrate(helpers, method, ql.EndCriteria(1000, 250, 1e-7, 1e-7, 1e-7))

    print("Parameters: %s" % model.params())
    print(rule)

    print(header)
    print(rule)

    totalError = 0.0
    for swaption, helper in zip(swaptionVols, helpers):
        maturity, length, vol = swaption
        NPV = helper.modelValue()
        implied = helper.impliedVolatility(NPV, 1.0e-4, 1000, 0.05, 0.50)
        error = implied - vol
        totalError += abs(error)
        print(fmt % (maturity, length, formatVol(vol, 4), formatVol(implied, 4), formatVol(error, 4)))
    averageError = totalError / len(helpers)

    print(rule)
    fmt = "%%%ds" % len(header)
    print(fmt % ("Average error: " + formatVol(averageError, 4)))
    print(dblrule)


todaysDate = ql.Date(15, ql.April, 2020)
ql.Settings.instance().evaluationDate = todaysDate
calendar = ql.TARGET()
settlementDate = ql.Date(19, ql.July, 2021)

# flat yield term structure impling 1x5 swap at 5%
rate = ql.QuoteHandle(ql.SimpleQuote(0.04875825))
termStructure = ql.YieldTermStructureHandle(ql.FlatForward(settlementDate, rate, ql.Actual365Fixed()))

# define the ATM/OTM/ITM swaps

swapEngine = ql.DiscountingSwapEngine(termStructure)

fixedLegFrequency = ql.Annual
fixedLegTenor = ql.Period(1, ql.Years)
fixedLegConvention = ql.Unadjusted
floatingLegConvention = ql.ModifiedFollowing
fixedLegDayCounter = ql.Thirty360(ql.Thirty360.European)
floatingLegFrequency = ql.Semiannual
floatingLegTenor = ql.Period(6, ql.Months)

payFixed = ql.VanillaSwap.Payer
fixingDays = 2
index = ql.Euribor6M(termStructure)
floatingLegDayCounter = index.dayCounter()

swapStart = calendar.advance(settlementDate, 1, ql.Years, floatingLegConvention)
swapEnd = calendar.advance(swapStart, 5, ql.Years, floatingLegConvention)

fixedSchedule = ql.Schedule(
    swapStart,
    swapEnd,
    fixedLegTenor,
    calendar,
    fixedLegConvention,
    fixedLegConvention,
    ql.DateGeneration.Forward,
    False,
)
floatingSchedule = ql.Schedule(
    swapStart,
    swapEnd,
    floatingLegTenor,
    calendar,
    floatingLegConvention,
    floatingLegConvention,
    ql.DateGeneration.Forward,
    False,
)

dummy = ql.VanillaSwap(
    payFixed, 100.0, fixedSchedule, 0.0, fixedLegDayCounter, floatingSchedule, index, 0.0, floatingLegDayCounter
)
dummy.setPricingEngine(swapEngine)
atmRate = dummy.fairRate()

atmSwap = ql.VanillaSwap(
    payFixed, 1000.0, fixedSchedule, atmRate, fixedLegDayCounter, floatingSchedule, index, 0.0, floatingLegDayCounter
)
otmSwap = ql.VanillaSwap(
    payFixed,
    1000.0,
    fixedSchedule,
    atmRate * 1.2,
    fixedLegDayCounter,
    floatingSchedule,
    index,
    0.0,
    floatingLegDayCounter,
)
itmSwap = ql.VanillaSwap(
    payFixed,
    1000.0,
    fixedSchedule,
    atmRate * 0.8,
    fixedLegDayCounter,
    floatingSchedule,
    index,
    0.0,
    floatingLegDayCounter,
)
atmSwap.setPricingEngine(swapEngine)
otmSwap.setPricingEngine(swapEngine)
itmSwap.setPricingEngine(swapEngine)

helpers = [
    ql.SwaptionHelper(
        maturity,
        length,
        ql.QuoteHandle(ql.SimpleQuote(vol)),
        index,
        index.tenor(),
        index.dayCounter(),
        index.dayCounter(),
        termStructure,
    )
    for maturity, length, vol in swaptionVols
]

times = {}
for h in helpers:
    for t in h.times():
        times[t] = 1
times = sorted(times.keys())

grid = ql.TimeGrid(times, 30)

G2model = ql.G2(termStructure)
HWmodel = ql.HullWhite(termStructure)
HWmodel2 = ql.HullWhite(termStructure)
BKmodel = ql.BlackKarasinski(termStructure)

print("Calibrating...")

for h in helpers:
    h.setPricingEngine(ql.G2SwaptionEngine(G2model, 6.0, 16))
calibrate(G2model, helpers, 0.05, "G2 (analytic formulae)")

for h in helpers:
    h.setPricingEngine(ql.JamshidianSwaptionEngine(HWmodel))
calibrate(HWmodel, helpers, 0.05, "Hull-White (analytic formulae)")

for h in helpers:
    h.setPricingEngine(ql.TreeSwaptionEngine(HWmodel2, grid))
calibrate(HWmodel2, helpers, 0.05, "Hull-White (numerical calibration)")

for h in helpers:
    h.setPricingEngine(ql.TreeSwaptionEngine(BKmodel, grid))
calibrate(BKmodel, helpers, 0.05, "Black-Karasinski (numerical calibration)")


# price Bermudan swaptions on defined swaps

bermudanDates = [d for d in fixedSchedule][:-1]
exercise = ql.BermudanExercise(bermudanDates)

fmt = "%17s |%17s |%17s |%17s"
header = fmt % ("model", "in-the-money", "at-the-money", "out-of-the-money")
rule = "-" * len(header)
dblrule = "=" * len(header)

print("")
print(dblrule)
print("Pricing Bermudan swaptions...")
print(rule)
print(header)
print(rule)

atmSwaption = ql.Swaption(atmSwap, exercise)
otmSwaption = ql.Swaption(otmSwap, exercise)
itmSwaption = ql.Swaption(itmSwap, exercise)

atmSwaption.setPricingEngine(ql.TreeSwaptionEngine(G2model, 50))
otmSwaption.setPricingEngine(ql.TreeSwaptionEngine(G2model, 50))
itmSwaption.setPricingEngine(ql.TreeSwaptionEngine(G2model, 50))

print(
    fmt
    % ("G2 analytic", formatPrice(itmSwaption.NPV()), formatPrice(atmSwaption.NPV()), formatPrice(otmSwaption.NPV()))
)

atmSwaption.setPricingEngine(ql.TreeSwaptionEngine(HWmodel, 50))
otmSwaption.setPricingEngine(ql.TreeSwaptionEngine(HWmodel, 50))
itmSwaption.setPricingEngine(ql.TreeSwaptionEngine(HWmodel, 50))

print(
    fmt
    % ("HW analytic", formatPrice(itmSwaption.NPV()), formatPrice(atmSwaption.NPV()), formatPrice(otmSwaption.NPV()))
)

atmSwaption.setPricingEngine(ql.TreeSwaptionEngine(HWmodel2, 50))
otmSwaption.setPricingEngine(ql.TreeSwaptionEngine(HWmodel2, 50))
itmSwaption.setPricingEngine(ql.TreeSwaptionEngine(HWmodel2, 50))

print(
    fmt
    % ("HW numerical", formatPrice(itmSwaption.NPV()), formatPrice(atmSwaption.NPV()), formatPrice(otmSwaption.NPV()))
)

atmSwaption.setPricingEngine(ql.TreeSwaptionEngine(BKmodel, 50))
otmSwaption.setPricingEngine(ql.TreeSwaptionEngine(BKmodel, 50))
itmSwaption.setPricingEngine(ql.TreeSwaptionEngine(BKmodel, 50))

print(
    fmt
    % ("BK numerical", formatPrice(itmSwaption.NPV()), formatPrice(atmSwaption.NPV()), formatPrice(otmSwaption.NPV()))
)

print(dblrule)