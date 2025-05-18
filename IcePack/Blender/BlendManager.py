import os
from IcePack.Enum.Flavour import Flavour
from IcePack.Enum.EnergyRange import EnergyRange
from IcePack.Blender.TruthBlender import TruthBlender
from IcePack.Blender.ShardBlender import ShardBlender

"""
@author: cyan.jo
Summary:
The orchestrator for TruthBlender and ShardBlender.

(1)Loads blending parameters like flavour, energy ranges, and event counts
(2)Calls TruthBlender to mix low- and high-energy truth data
(3)Calls ShardBlender to rebuild PMTfied shards for the blended truth
"""


class BlendManager:
    def __init__(
        self,
        source_dir: str,
        energy_range_low: EnergyRange,
        energy_range_high: EnergyRange,
        energy_range_combined: EnergyRange,
        flavour: Flavour,
        n_events_per_part: int = 30_000,
        n_events_per_shard: int = 3000,
        energy_cutoff: float = 1e5,
    ):
        self.source_dir = source_dir
        self.energy_range_low = energy_range_low
        self.energy_range_high = energy_range_high
        self.energy_range_combined = energy_range_combined

        self.flavour = flavour
        self.energy_cutoff = energy_cutoff

        # Directories holding the original PMTfied files (low/high energy)
        self.subdir_low = os.path.join(
            source_dir, EnergyRange.get_subdir(self.energy_range_low, flavour)
        )
        self.subdir_high = os.path.join(
            source_dir, EnergyRange.get_subdir(self.energy_range_high, flavour)
        )
        # Combined output directory from truth blender
        self.subdir_combined = os.path.join(
            source_dir,
            EnergyRange.get_subdir(self.energy_range_combined, flavour),
        )

        self.n_events_per_part = n_events_per_part
        self.n_events_per_shard = n_events_per_shard

    def __call__(self):
        self.blend()

    def blend(self) -> None:
        # Step 1: Blend truth files
        truth_blender = TruthBlender(
            source_dir=self.source_dir,
            energy_range_low=self.energy_range_low,
            energy_range_high=self.energy_range_high,
            energy_range_combined=self.energy_range_combined,
            flavour=self.flavour,
            n_events_per_part=self.n_events_per_part,
            energy_cutoff=self.energy_cutoff,
        )
        truth_blender()  # Call the TruthBlender to perform blending

        # Step 2: Blend PMTfied shards
        shard_blender = ShardBlender(
            source_dir=self.source_dir,
            energy_range_low=self.energy_range_low,
            energy_range_high=self.energy_range_high,
            energy_range_combined=self.energy_range_combined,
            flavour=self.flavour,
            n_events_per_shard=self.n_events_per_shard,
        )
        shard_blender()  # Call the ShardBlender to perform blending
