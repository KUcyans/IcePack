import sys
import argparse
import logging
import time
import os

from IcePack.EventFilter.EventFilterManager import EventFilterManager
from IcePack.EventFilter.PureNeutrinoEventFilter import PureNeutrinoEventFilter
from IcePack.EventFilter.Muon13Filter import Muon13Filter
from IcePack.EventFilter.CCFilter import CCFilter
from IcePack.EventFilter.ContainmentFilter import ContainmentFilter
from IcePack.EventFilter.IntraIceCubeSegmentFilter import (
    IntraIceCubeSegmentFilter,
)

# Available filter classes
FILTER_CLASSES = {
    "CRclean": PureNeutrinoEventFilter,
    "MuonLike": Muon13Filter,
    "CC": CCFilter,
    "Contained": ContainmentFilter,
    "IntraTravelDistance": IntraIceCubeSegmentFilter,
}

FILTER_KWARGS = {"IntraTravelDistance": {"min_travel_distance": 250}}


def run():
    start_time = time.time()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.info("Event filtering process starts...")

    # Define root paths
    source_root = "/lustre/hpc/project/icecube/HE_Nu_Aske_Oct2024/PMTfied/"
    dest_root = (
        "/lustre/hpc/project/icecube/HE_Nu_Aske_Oct2024/PMTfied_filtered/"
    )

    # Command-line arguments
    parser = argparse.ArgumentParser(
        description="Event filtering of PMT-fied data."
    )
    parser.add_argument(
        "Snowstorm_or_Corsika",
        type=str,
        help="Specify 'Snowstorm' or 'Corsika'.",
    )
    parser.add_argument(
        "subdirectory_in_number", type=str, help="Source subdirectory number."
    )
    parser.add_argument("part_number", type=str, help="Part number.")
    args = parser.parse_args()

    # Validate and convert numeric arguments
    try:
        subdir_no = int(args.subdirectory_in_number)
        part_no = int(args.part_number)
    except ValueError:
        logging.error("Subdirectory number and part number must be integers.")
        sys.exit(1)

    specific_source_dir = os.path.join(source_root, args.Snowstorm_or_Corsika)
    specific_dest_dir = os.path.join(dest_root, args.Snowstorm_or_Corsika)

    filter_manager = EventFilterManager(
        source_dir=specific_source_dir,
        output_dir=specific_dest_dir,
        subdir_no=subdir_no,
        part_no=part_no,
        filter_classes=FILTER_CLASSES,
        filter_kwargs=FILTER_KWARGS,
    )
    filter_manager()
    end_time = time.time()
    filter_manager.generate_receipt(start_time=start_time, end_time=end_time)
    logging.info("Filtering process completed.")


def main():
    run()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
        sys.exit(1)

# nohup python3.9 Filter_by_part.py Snowstorm 22010 1 > log/22010/[$(date +"%Y%m%d_%H%M%S")]log_Filter_22010_1.log 2>&1 &
# nohup python3.9 Filter_by_part.py Snowstorm 22011 1 > log/22011/[$(date +"%Y%m%d_%H%M%S")]log_Filter_22011_1.log 2>&1 &
# nohup python3.9 Filter_by_part.py Snowstorm 22012 1 > log/22012/[$(date +"%Y%m%d_%H%M%S")]log_Filter_22012_1.log 2>&1 &
