"""
streaming.analytics package — Lane B micro-batch algorithm plugins (foreachBatch).
"""

from streaming.analytics.decay import DecayAnalytic, DecayingCounter, DecayingKeyTable

__all__ = ["DecayAnalytic", "DecayingCounter", "DecayingKeyTable"]
