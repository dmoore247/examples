# Databricks notebook source
# MAGIC %pip install scipy==1.7

# COMMAND ----------

from scipy.stats import qmc

# COMMAND ----------

# MAGIC %matplotlib inline
# MAGIC 
# MAGIC import pandas as pd
# MAGIC import numpy as np
# MAGIC import matplotlib.pyplot as plt
# MAGIC 
# MAGIC 
# MAGIC from scipy.stats import qmc    # quasi-Monte Carlo for latin hypercube sampling
# MAGIC from scipy.stats import mstats 
# MAGIC from scipy import stats as stats
# MAGIC from scipy.stats import rv_continuous, rv_histogram
# MAGIC import scipy.optimize as opt
# MAGIC 
# MAGIC from scipy.stats import norm, weibull_min, beta
# MAGIC 
# MAGIC 
# MAGIC import warnings
# MAGIC warnings.filterwarnings("ignore")
# MAGIC 
# MAGIC 
# MAGIC N = 10000                # random numbers to generate

# COMMAND ----------

# MAGIC %md
# MAGIC # Drawing Random Variates

# COMMAND ----------

# MAGIC %md
# MAGIC ## Latin Hypercube Sampling

# COMMAND ----------

# Latin Hypercube LHS sampling of uniform random numbers

sampler = qmc.LatinHypercube(d=1, seed=42)    # d = dimension
sample = sampler.random(n=10)
print(type(sampler))
sample

# COMMAND ----------

# quality of sample
qmc.discrepancy(sample)

# COMMAND ----------

# samples can be rescaled to fit between the defined bounds
seq = qmc.scale(sample, 0, 100)
print(type(seq))
seq

# COMMAND ----------

# MAGIC %md
# MAGIC ## PERT random variates

# COMMAND ----------

# define a new class pertm_gen: a generator for the PERT distribution

class pertm_gen(rv_continuous):
    '''modified beta_PERT distribution'''

 
    def _shape(self, min, mode, max, lmb):
        s_alpha = 1+ lmb*(mode - min)/(max-min)
        s_beta = 1 + lmb*(max - mode)/(max-min)
        return [s_alpha, s_beta]


    def _cdf(self, x, min, mode, max, lmb):
        s_alpha, s_beta = self._shape(min, mode, max, lmb)
        z = (x - min) / (max - min)
        cdf = beta.cdf(z, s_alpha, s_beta)
        return cdf

    def _ppf(self, p, min, mode, max, lmb):
        s_alpha, s_beta = self._shape(min, mode, max, lmb)
        ppf = beta.ppf(p, s_alpha, s_beta)
        ppf = ppf * (max - min) + min
        return ppf


    def _mean(self, min, mode, max, lmb):
        mean = (min + lmb * mode + max) / (2 + lmb)
        return mean

    def _var(self, min, mode, max, lmb):
        mean = self._mean(min, mode, max, lmb)
        var = (mean - min) * (max - mean) / (lmb + 3)
        return var

    def _skew(self, min, mode, max, lmb):
        mean = self._mean(min, mode, max, lmb)
        skew1 = (min + max - 2*mean) / 4
        skew2 = (mean - min) * (max  - mean)
        skew2 = np.sqrt(7 / skew2)
        skew = skew1 * skew2
        return skew

    def _kurt(self, min, mode, max, lmb):
        a1,a2 = self._shape(min, mode, max, lmb)
        kurt1 = a1 + a2 +1
        kurt2 = 2 * (a1 + a2)**2
        kurt3 = a1 * a2 * (a1 + a2 - 6)
        kurt4 = a1 * a2 * (a1 + a2 + 2) * (a1 + a2 + 3)
        kurt5 = 3 * kurt1 * (kurt2 + kurt3)
        kurt = kurt5 / kurt4 -  3                 # scipy defines kurtosis of std normal distribution as 0 instead of 3
        return kurt

    def _stats(self, min, mode, max, lmb):
        mean = self._mean(min, mode, max, lmb)
        var = self._var(min, mode, max, lmb)
        skew = self._skew(min, mode, max, lmb)
        kurt = self._kurt(min, mode, max, lmb)
        return mean, var, skew, kurt


# COMMAND ----------

# create a PERT instance 
# expected sales volume

min, mode, max, lmb = 8000.0, 12000.0, 18000.0, 4.0


# instantiate a PERT object
pertm = pertm_gen(name="pertm")
rvP = pertm(min,mode,max,lmb)
statsP = rvP.stats("mvsk")

moments = [np.asscalar(v) for v in statsP]
moment_names = ["mean", "var", "skew", "kurt"]
dict_moments = dict(zip(moment_names, moments))
_ = [print(k,":",f'{v:.2f}') for k,v in dict_moments.items()]

# COMMAND ----------

# PERT random variates with LHS
sampler01 = qmc.LatinHypercube(d=1, seed=42)    # d = dimension
sample01 = sampler01.random(n=N)
randP = rvP.ppf(sample01)

fig, ax = plt.subplots(1, 1)
ax.hist(randP, density=True, histtype='stepfilled', alpha=0.2)
ax.legend(loc='best', frameon=False)
plt.show()
pass


# COMMAND ----------

# MAGIC %md
# MAGIC ## Normal random variates

# COMMAND ----------

# normal random variates with LHS
# selling price

m1, s1 = 20.0, 2.0

sampler02 = qmc.LatinHypercube(d=1, seed=43)    # d = dimension
sample02 = sampler02.random(n=N)
rvN1 = norm(m1,s1)
randN1 = rvN1.ppf(sample02)

fig, ax = plt.subplots(1, 1)
ax.hist(randN1, density=True, histtype='stepfilled', alpha=0.2)
ax.legend(loc='best', frameon=False)
plt.show()
pass


# COMMAND ----------



# COMMAND ----------

""# Normal random variates with LHS: 
# raw material unit cost

m2, s2 = 13.0, 1.4

sampler02 = qmc.LatinHypercube(d=1, seed=44)    # d = dimension
sample02 = sampler02.random(n=N)
rvN2 = norm(m2,s2)
randN2 = rvN2.ppf(sample02)

fig, ax = plt.subplots(1, 1)
ax.hist(randN2, density=True, histtype='stepfilled', alpha=0.2)
ax.legend(loc='best', frameon=False)
plt.show()
pass


# COMMAND ----------

# MAGIC %md
# MAGIC # Simulation Model 1: Sums and Products of Random Variates

# COMMAND ----------

def dist_properties(data):

    # moments
    mean = data.mean()
    s = data.std()
    min = data.min()
    max = data.max()
    skew = np.asscalar(stats.skew(data))
    kurt = np.asscalar(stats.kurtosis(data))

    moment_names = ["mean", "std", "min", "max", "skew", "kurt"]
    moments = [mean,s,min,max,skew,kurt]
    dict_moments = dict(zip(moment_names, moments))
    _ = [print(k,":",f'{v:.3f}') for k,v in dict_moments.items()]


    # quantiles
    q1 = np.array([0.001, 0.01, 0.99, 0.999])
    q2 = np.arange(0.05, 0.95, 0.05)
    q = np.concatenate((q1,q2))
    q.sort()
    xq = np.quantile(data, q)
    
    qstr = [str(f'{v:.3f}') for v in q]
    dict_quantiles = dict(zip(qstr, xq))
    print("\nquantiles:")
    _ = [print(q,":",f'{xq:,.0f}') for q,xq in dict_quantiles.items()]


    # moments & quantiles
    metric_names = moment_names.extend(q)
    metrics = moments.extend(xq)
    dict_metrics = {**dict_moments, **dict_quantiles}
    return dict_metrics


# COMMAND ----------

v = randP           # array of random variates representing the sales volume
p = randN1          # random array for selling price
m = randN2          # random array for material unit cost
o = 3.0             #other unit cost, deterministic

# COMMAND ----------

# The target variable of the simulation mode
# gross profit = volume * (price - material unit cost - other unit cost) 

randGP = v * (p - m - o)

fig, ax = plt.subplots(1, 1)
ax.hist(randGP, density=True, histtype='stepfilled', alpha=0.2)
ax.legend(loc='best', frameon=False)
plt.show()
pass


randR = v * p


# COMMAND ----------

# secondary target variable of the simulation mode
# revenues

randR = v * p

fig, ax = plt.subplots(1, 1)
ax.hist(randGP, density=True, histtype='stepfilled', alpha=0.2)
ax.legend(loc='best', frameon=False)
plt.show()
pass

# COMMAND ----------

# get the properties of the GP array
GP_metrics = dist_properties(randGP)

# COMMAND ----------

# MAGIC %md
# MAGIC # Simulation 2: Nested Random Variables

# COMMAND ----------

# Weibull shape parameter:
shp_m, shp_s = 1.5, 0.1                 # Weibull shape parameter: normally distributed
scl_min, scl_mode, scl_max, scl_lmb = 45000, 50000, 60000, 8.0  # weibull scale parameter: PERT 3-point-estimate

# COMMAND ----------

# generate 10,000 normal random variates 

def wei_shp(mean,std):
    sampler = qmc.LatinHypercube(d=1, seed=42)    # d = dimension
    sample = sampler.random(n=N)

    rv = norm(mean, std)
    rand = rv.ppf(sample)
    return rand

# COMMAND ----------

# generate 10,000 PERT random numbers

def wei_charlife(min,mode,max,lmb):
    sampler = qmc.LatinHypercube(d=1, seed=42)    # d = dimension
    sample = sampler.random(n=N)
 
    pertm = pertm_gen(name="pertm")
    rv = pertm(min, mode, max, lmb)
    rand = rv.ppf(sample)
    return rand
    

# COMMAND ----------

# Weibull_Min random variates with LHS

loc = 0.0

sampler4 = qmc.LatinHypercube(d=1, seed=48)    # d = dimension
sample4 = sampler4.random(n=N)
rand_CL = weibull_min.ppf(sample4, wei_shp(shp_m, shp_s), loc, wei_charlife(scl_min, scl_mode, scl_max, scl_lmb))


fig, ax = plt.subplots(1, 1)
ax.hist(rand_CL, density=True, histtype='stepfilled', alpha=0.2)
ax.legend(loc='best', frameon=False)
plt.show()
pass


# COMMAND ----------

# properties of the simulation results
flexWeib = dist_properties(rand_CL)

# COMMAND ----------

# create a distribution object from the simulated output variable by using rv_histogram

data = rand_CL
hist = np.histogram(data, bins=100)
histdist = rv_histogram(hist)

X = np.linspace(data.min(), data.max(), 100)
plt.title("PDF from Template")
plt.hist(data, density=True, bins=100)
plt.plot(X, histdist.pdf(X), label='PDF')
plt.show()


# COMMAND ----------

# histogram-based probability distribution

def histdist_properties(histdist):
    
    # moments
    mean = histdist.mean()
    s = histdist.std()
    median = histdist.median()
    min = histdist.support()[0]
    max = histdist.support()[1]
    stats =histdist.stats()

    metric_names = ["mean", "std", "median", "min", "max"]
    metrics = [mean, s, median, min, max]
    dict_metrics = dict(zip(metric_names, metrics))
    print("histogram-based probability distribution:")
    _ = [print(k,":",f'{v:.3f}') for k,v in dict_metrics.items()]


    # choose some probabilities of interest
    q1 = np.array([0.001, 0.01, 0.99, 0.999])
    q2 = np.arange(0.05, 0.95, 0.05)
    q = np.concatenate((q1,q2))
    q.sort()
    q

    # we can generate some random variables
    randH = histdist.rvs(size=len(q))

    # calculate quantiles, cdf and pdf at selected points
    ppf_res = histdist.ppf(q)
    pdf_res = histdist.pdf(ppf_res)
    cdf_res = histdist.cdf(ppf_res)
    pd.options.display.float_format = '{:,.3f}'.format
    dfH = pd.DataFrame()
    dfH["metrics"] = q
    dfH["quantiles"] = ppf_res
    dfH["cdf"] = cdf_res
    dfH["rand.variates"] = randH
    
    return dfH


# COMMAND ----------

histdist_properties(histdist)

# COMMAND ----------

# Weibull_Min with fixed parameters

loc2 = loc
shp2 = shp_m
scl2 = rand_CL.mean()

rand_fix = weibull_min.ppf(sample4, shp2, loc2, scl2)


fig, ax = plt.subplots(1, 1)
ax.hist(rand_fix, density=True, histtype='stepfilled', alpha=0.2)
ax.legend(loc='best', frameon=False)
plt.show()
pass


# COMMAND ----------

# properties of the simulation results
fixWeib = dist_properties(rand_fix)

# COMMAND ----------

print("\nCompare the Weibull with fixed parameters and the Weibull with stochastic parameters:")
pd.options.display.float_format = '{:,.3f}'.format
df = pd.DataFrame(index=range(len(fixWeib)))
df["metrics"] = fixWeib.keys()
df["fix Weibull"] = fixWeib.values()
df["flex Weibull"] = flexWeib.values()
df

# COMMAND ----------


