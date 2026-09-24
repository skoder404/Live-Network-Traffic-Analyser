"""
streaming.analytics package — Lane B micro-batch algorithm plugins (foreachBatch).
"""

from streaming.analytics.decay import DecayAnalytic, DecayingCounter, DecayingKeyTable
from streaming.analytics.filters import (
    FilterSpec,
    apply_filter,
    parse_filters,
    validate_filters,
)
from streaming.analytics.moments import (
    AMSF2,
    MomentsAnalytic,
    compute_iat_from_timestamps,
    exact_f2,
)
from streaming.analytics.sampling import (
    ReservoirSampler,
    SamplingAnalytic,
    bernoulli_sample,
)

__all__ = [
    "DecayAnalytic",
    "DecayingCounter",
    "DecayingKeyTable",
    "FilterSpec",
    "parse_filters",
    "validate_filters",
    "apply_filter",
    "ReservoirSampler",
    "bernoulli_sample",
    "SamplingAnalytic",
    "MomentsAnalytic",
    "AMSF2",
    "exact_f2",
    "compute_iat_from_timestamps",
]
