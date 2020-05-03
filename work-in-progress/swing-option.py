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

todaysDate = ql.Date(1, ql.May, 2020)
ql.Settings.instance().evaluationDate = todaysDate
settlementDate = todaysDate
riskFreeRate = ql.FlatForward(settlementDate, 0.1, ql.Actual365Fixed())
dividendYield = ql.FlatForward(settlementDate, 0.0, ql.Actual365Fixed())
underlying = ql.SimpleQuote(1.9) #nymex spot price
volatility = ql.BlackConstantVol(todaysDate, ql.TARGET(), 0.20, ql.Actual365Fixed())

exerciseDates = [ql.Date(1, ql.January, 2021) + i for i in range(60)]

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

print("Black Scholes Price: %f" % swingOption.NPV())

x0 = 0.08
x1 =0.08

beta = 6.0 #market risk
eta = 5.0 #theta
jumpIntensity = 2.5
speed = 1.0 #kappa
volatility = 0.2 #sigma

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

swingOption.setPricingEngine(ql.FdSimpleExtOUJumpSwingEngine(jProcess, riskFreeRate, 25, 25, 60, curveShape))

print("Kluge Model Price  : %f" % swingOption.NPV())