#!/usr/bin/env python3

import numpy as np
from custom.viz_utils import plot_distribution

# Set up our variables
lambda_ = 4  # a constant ("rate parameter")
n = 10_000  # we'll increase this to mimic approaching infinity
p = lambda_ / n  # from condition stipulated earlier
num_sims = 100_000  # arbitrarily large number of trials

# Run num_sims using lambda_, n, and p from above **without loops**
data = (np.random.random_sample((n, num_sims)) < p).sum(axis=0)

# Compute empirical PMF, CDF for plotting
x, freq = np.unique(data, return_counts=True)
pmf = freq / num_sims
cdf = pmf.cumsum()

# Plot distribution
plot_distribution(x, pmf, cdf)
