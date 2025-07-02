from enum import Enum

"""
@author: cyan.jo
Summary:
Defines summary modes for IceCube data processing, including classic, geometric, and equinox modes.
(1) Classic: Thorsten's method including 32 features
(2) Geometric: Three event-level geometic features are added on top of the classic features, has 35 features
(3) Equinox: Later charge-time information added on top of the Geometric features, has 40 features
(4) Sankthans: Max pulse information added on top of the Geometric features, has 42 features
"""

class SummaryMode(Enum):
    CLASSIC = (0, 'thorsten', 5)
    SECOND = (1, 'geometric', 5)    # three geometric feature added
    EQUINOX = (2, 'equinox', 5)     # on top of the geometric features, later charge-time information added
    SANKTHANS = (3, 'sankthans', 5) # on top of the geometric features, max pulse information added
    
    def __init__(self, index: int, name: str, n_first_pulse_collect: int):
        self._index = index
        self._name = name
        self._n_first_pulse_collect = n_first_pulse_collect
    
    @property
    def index(self) -> int:
        """Return the index of the summary mode."""
        return self._index
    # example usage: print(SummaryMode.CLASSIC.index)
    
    def __str__(self) -> str:
        return self._name
    
    @property
    def n_collect(self) -> int:
        """Return the number of first pulses to collect."""
        return self._n_first_pulse_collect
    
    @staticmethod
    def from_index(index: int) -> 'SummaryMode':
        """Return the summary mode from the index."""
        for mode in SummaryMode:
            if mode.index == index:
                return mode
        raise ValueError(f"Invalid index: {index}")