import numpy as np
import polars as pl

from .j import J, JType


def seed(seed: J):
    if seed.j_type == JType.INT:
        pl.set_random_seed(seed.data)
        np.random.seed(seed.data)
