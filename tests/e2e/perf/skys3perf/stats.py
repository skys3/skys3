"""Distributions of samples: percentiles, a histogram, and the samples.

Every distribution a result carries has the same shape (the schema's
`distribution`): the count, the minimum, maximum, and mean, the
percentiles in PERCENTILES, a histogram on a 1-2-5 scale, and the samples
themselves while there are at most `samples_max` of them.
"""

from __future__ import annotations

import math

PERCENTILES = (50.0, 90.0, 99.0, 99.9)


def percentile(ordered: list[float], p: float) -> float | None:
    """The p-th percentile (0 to 100) of sorted samples, interpolated
    linearly between the closest ranks, or None without samples."""
    if not 0.0 <= p <= 100.0:
        raise ValueError(f"a percentile is from 0 to 100, not {p}")
    if not ordered:
        return None
    rank = (len(ordered) - 1) * p / 100.0
    low = math.floor(rank)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (rank - low)


def percentile_key(p: float) -> str:
    """The key of a percentile in a result: p50, p99, p99.9."""
    return "p" + (f"{p:g}")


def scale_125(value: float) -> float:
    """The smallest value of the 1-2-5 series (..., 0.5, 1, 2, 5, 10, ...)
    that is at least `value`, which must be positive."""
    if value <= 0:
        raise ValueError("the 1-2-5 scale holds positive values only")
    decade = 10.0 ** math.floor(math.log10(value))
    for step in (1, 2, 5):
        # Rounded, so that bounds print as 0.2 rather than 0.20000000000000001.
        bound = float(f"{step * decade:.12g}")
        if bound >= value * (1 - 1e-12):
            return bound
    return float(f"{10 * decade:.12g}")


def histogram(samples: list[float]) -> dict:
    """A histogram of the samples on the 1-2-5 scale: `bounds` are the
    upper bounds of the buckets, increasing, and `counts[i]` the samples
    above `bounds[i - 1]` (above 0 for the first) and at most `bounds[i]`.
    Samples of 0 or less count in the first bucket. Empty without samples."""
    if not samples:
        return {"bounds": [], "counts": []}
    positive = [s for s in samples if s > 0]
    top = scale_125(max(positive)) if positive else 1.0
    bottom = scale_125(min(positive)) if positive else top
    bounds = [bottom]
    while bounds[-1] < top:
        bounds.append(scale_125(bounds[-1] * 1.5))
    counts = [0] * len(bounds)
    for sample in samples:
        counts[_bucket(bounds, sample)] += 1
    return {"bounds": bounds, "counts": counts}


def _bucket(bounds: list[float], sample: float) -> int:
    low, high = 0, len(bounds) - 1
    while low < high:
        middle = (low + high) // 2
        if sample <= bounds[middle]:
            high = middle
        else:
            low = middle + 1
    return low


def summarize(samples: list[float], unit: str, samples_max: int) -> dict:
    """The distribution of `samples` measured in `unit` (`seconds`,
    `bytes`, ...), keeping the samples themselves if there are at most
    `samples_max`."""
    ordered = sorted(samples)
    count = len(ordered)
    return {
        "unit": unit,
        "count": count,
        "min": ordered[0] if ordered else None,
        "max": ordered[-1] if ordered else None,
        "mean": math.fsum(ordered) / count if ordered else None,
        "percentiles": {percentile_key(p): percentile(ordered, p) for p in PERCENTILES},
        "histogram": histogram(ordered),
        "samples": list(samples) if count <= samples_max else None,
    }


def ratio(numerator: float | None, denominator: float | None) -> float | None:
    """numerator / denominator, or None if either is unknown or the
    denominator is 0."""
    if numerator is None or not denominator:
        return None
    return numerator / denominator
