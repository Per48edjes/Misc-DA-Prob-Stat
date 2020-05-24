#!/usr/bin/env python
# coding: utf-8

import numpy as np
import pandas as pd

# This code replicates the example on P. 67 of *Introduction to Probability*
# (Blitzstein, Hwang 2015).


def rand_bin_array(p: float, N: int) -> np.array:
    """
    Returns an array of length N with K ones and N-K zeros
    """
    K = p * N

    # Handle floats
    if type(K) is float:
        K = int(round(K, 0))

    # Create array of zeros length N
    arr = np.zeros(N)

    # Assign elements to K, shuffle
    arr[:K] = 1
    np.random.shuffle(arr)

    return arr


def data_generator(N: int = 5_000_000, p_M: float = 0.0005) -> pd.DataFrame:
    """
    Generates dataframe with data consistent with the problem
    """
    # Initialize empty DataFrame
    data = np.empty((N, 3))
    data[:] = np.nan
    df = pd.DataFrame(data, columns=["M", "G", "A"])

    # Fill out dataframe
    keys = ("cond", "p", "col")
    params = (
        ("@df.isna()", p_M, "M"),
        ("M == 1", 0.2, "G"),
        ("M == 1 and G == 1", 0.5, "A"),
        ("M == 1 and G == 0", 0.1, "A"),
    )

    settings = [dict(zip(keys, param)) for param in params]
    for x in settings:
        temp = df.query(x["cond"])
        count = temp.shape[0]
        df.loc[temp.index, x["col"]] = rand_bin_array(x["p"], count)

    df = df.fillna(0).astype(int)

    # Calculate the number of total abusive husbands needed
    guilty_and_abusive_count = df.query("A == 1 and G == 1").shape[0]
    prior_abusive_count = df.query("A == 1").shape[0]
    posterior_abusive_count = prior_abusive_count
    while guilty_and_abusive_count / posterior_abusive_count > (1 / 2500):
        posterior_abusive_count += 1

    # Set additional abusive husbands among not guilty + not murdered wives
    additional_abusive_count = posterior_abusive_count - prior_abusive_count
    temp = df.query("M == 0 and G == 0")
    count = temp.shape[0]
    df.loc[temp.index[:additional_abusive_count], "A"] = 1

    return df


def data_tests(df):
    """
    Prints "No errors!" if the data meets the specs for the
    problem, otherwise an AssertionError is raised
    """
    assert df.query("M == 1")["G"].sum() / df.query("M == 1").shape[0] == 0.2
    assert (df.query("G == 0 and M == 1")["A"].sum() /
            df.query("G == 0 and M == 1").shape[0] == 0.1)
    assert (df.query("G == 1 and M == 1")["A"].sum() /
            df.query("G == 1 and M == 1").shape[0] == 0.5)
    print("No errors!")
