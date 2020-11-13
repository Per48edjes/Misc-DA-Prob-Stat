import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator, MultipleLocator
import numpy as np


def plot_step(x, y, ax=None, where="post", **kwargs):
    """
    Returns step function plot on ax
    """
    assert where in ["post", "pre"]
    x = np.array(x)
    y = np.array(y)
    if where == "post":
        y_slice = y[:-1]
    if where == "pre":
        y_slice = y[1:]
    X = np.c_[x[:-1], x[1:], x[1:]]
    Y = np.c_[y_slice, y_slice, np.zeros_like(x[:-1]) * np.nan]
    if not ax:
        ax = plt.gca()
    return ax.plot(X.flatten(), Y.flatten(), **kwargs)


def plot_distribution(x, pmf, cdf, labels=["PMF of $X$", "CDF of $X$"]):
    """
    Plots PMF and CDF side-by-side. `x` is a numpy object that represents the
    support of the distribution passed in (as `pmf`, `cdf`)
    """
    fig, axes = plt.subplots(1, 2, figsize=(15, 8))

    for i, ax in enumerate(axes):

        # Plot the PMF
        if i == 0:
            ax.plot(x, pmf, "bo", ms=8, label=labels[0])
            ax.vlines(x, 0, pmf, colors="b", lw=5, alpha=0.5)
            ax.set_ylabel("Probability")
            ax.title.set_text(labels[0])
        else:
            plot_step(x, cdf, color="b", lw=5, alpha=0.5, label=labels[1])
            for i, prob in enumerate(cdf):
                ax.plot(x[i], prob, "bo", ms=8)
            ax.title.set_text(labels[1])

        # Styling
        ax.spines["right"].set_color("none")
        ax.spines["top"].set_color("none")
        ax.set_ylim(0, 1.1)

        # Axis ticks, labels
        ax.xaxis.set_major_locator(MaxNLocator(integer=True))
        ax.yaxis.set_minor_locator(MultipleLocator(0.1))
        ax.tick_params(which="minor", width=0.75, length=2.5, labelsize=10)

    plt.show()
