import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from IcePack.Blender.BlendManager import BlendManager
from IcePack.Enum.EnergyRange import EnergyRange
from IcePack.Enum.Flavour import Flavour
import time


def main():
    start_time = time.time()
    root_dir = "/lustre/hpc/project/icecube/HE_Nu_Aske_Oct2024/PMTfied_filtered_second_round/Snowstorm/CC_CRclean_Contained"

    energy_range_low = EnergyRange.ER_10_TEV_1_PEV
    energy_range_high = EnergyRange.ER_1_PEV_100_PEV
    energy_range_combined = EnergyRange.ER_100_TEV_100_PEV

    # choose which flavour to blend or use batch processing below
    # flavour = Flavour.E
    # flavour = Flavour.MU
    # flavour = Flavour.TAU
    n_events_per_part = 30000
    n_events_per_shard = 3000
    energy_cutoff = 1e5

    # batch processing for all flavours
    for flavour in [Flavour.E, Flavour.MU, Flavour.TAU]:
        print("Starting blending process...")
        print(f"Source Directory: {root_dir}")
        print(f"Energy Range Low: {energy_range_low}")
        print(f"Energy Range High: {energy_range_high}")
        print(f"Energy Range Combined: {energy_range_combined}")
        print(f"Flavour: {flavour}")
        print(f"Number of Events per Part: {n_events_per_part}")
        print(f"Number of Events per Shard: {n_events_per_shard}")
        print(
            f"Energy Cutoff: {energy_cutoff:.2e}GeV = {energy_cutoff/1e3:.0f}TeV"
        )
        print("--------------------------------------------------")

        manager = BlendManager(
            source_dir=root_dir,
            energy_range_low=energy_range_low,
            energy_range_high=energy_range_high,
            energy_range_combined=energy_range_combined,
            flavour=flavour,
            n_events_per_part=n_events_per_part,
            n_events_per_shard=n_events_per_shard,
            energy_cutoff=energy_cutoff,
        )
        manager()
    end_time = time.time()
    # in hours, minutes, seconds
    hours, remainder = divmod(end_time - start_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    print(
        f"Total time taken: {int(hours):02}:{int(minutes):02}:{int(seconds):02}"
    )


if __name__ == "__main__":
    main()
