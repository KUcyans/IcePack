import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import os
from os import getenv
import argparse
import logging
import json
from IcePack.PMTfication.PMTfier import PMTfier
from IcePack.Enum.SummaryMode import SummaryMode
from IcePack.Enum.Flavour import Flavour
from IcePack.Enum.EnergyRange import EnergyRange
import time
import psutil
import socket

# import the proper source layout defined by the user
from IcePack.PMTfication.Layout.SnowstormLayout import SnowstormLayout
from IcePack.PMTfication.Layout.CorsikaLayout import CorsikaLayout


def pmtfy_wrap():
    # Initial logging configuration
    log_system_info()
    start_time = log_start_time()

    # NOTE 1. Command-line arguments : indicate which file to process, and what mode of processing to use
    args = parse_arguments()

    # ============= USER SETTINGS ==============
    # NOTE 2. Update source and destination directories: source_root, dest_root, layout, table config file, summary_mode, part_no
    # Layout specifies the structure of the source files and how they should be processed.
    # Table config file specifies the structure of the SQLite database tables.
    # summary mode specifies which set of PMT-wise features shall be extracted.
    #
    source_root = (
        "/lustre/hpc/project/icecube/HE_Nu_Aske_Oct2024/sqlite_pulses/"
    )
    dest_root_base = "/lustre/hpc/project/icecube/HE_Nu_Aske_Oct2024/PMTfied/"

    part_no = int(args.part_number)

    #   NOTE 2.1 method1: select the layout item directly
    # layout = SourceLayoutHESnowStorm.NU_MU_1PEV_100PEV

    #   NOTE 2.2 method2: select the layout item by flavour and energy range
    layout = SnowstormLayout.from_flavour_energy(
        flavour=Flavour.TAU, energy_range=EnergyRange.ER_1_PEV_100_PEV
    )

    summary_mode = SummaryMode.from_index(args.summary_mode)
    table_config_path = "/groups/icecube/cyan/factory/IcePACK/IcePack/PMTfication/Layout/TableConfig.json"
    # layout = CorsikaLayout.from_alias(2)
    # ===========================================

    # Log configuration
    log_cofiguration(
        source_root, dest_root_base, layout, summary_mode, part_no
    )

    # NOTE 3. instantiate and invoke the PMTfier class with a part number
    PMTfier(
        source_root=source_root,
        source_layout=layout,
        source_table_config_file=table_config_path,
        dest_root=dest_root_base,
        summary_mode=summary_mode,
    )(part_no=part_no)

    # NOTE Log the end time
    logging.info(
        f"PMTfication completed at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}"
    )
    log_end_time(start_time)


"""------Utility functions for logging and argument parsing-----"""


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="PMTfication of a single SQLite database into Parquet files."
    )
    parser.add_argument(
        "part_number", type=str, help="Part number (file index)."
    )
    parser.add_argument(
        "--summary_mode",
        type=int,
        choices=[0, 1, 2, 3],
        default=0,
        help="Summary mode: 0=Thorsten's 32, 1=geometric, 2=geometric + later, 3=geometric + max.",
    )
    return parser.parse_args()


def log_system_info():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.info(f"Host: {socket.gethostname()}")
    logging.info(f"CPU cores: {psutil.cpu_count(logical=True)}")
    mem = psutil.virtual_memory()
    logging.info(
        f"Memory: {mem.total / (1024 ** 3):.2f} GB total, {mem.available / (1024 ** 3):.2f} GB available"
    )


def log_start_time():
    start_time = time.time()
    logging.info(
        f"PMTfication started at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}"
    )
    return start_time


def log_cofiguration(
    source_root: str,
    dest_root_base: str,
    layout: str,
    summary_mode: SummaryMode,
    part_no: int,
):
    logging.info(f"Source root: {source_root}")
    logging.info(f"Destination root: {dest_root_base}")
    logging.info(f"Summary mode: {summary_mode}")
    logging.info(f"Layout: {layout}")
    logging.info(f"part_number: {part_no}")
    # disable this line if you want to run it locally
    logging.info(
        f"Using up to {int(getenv('SLURM_CPUS_PER_TASK', '1'))} workers."
    )


def log_end_time(start_time: float):
    end_time = time.time()
    elapsed_time = end_time - start_time
    hours, remainder = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    logging.info(
        f"Elapsed time: {int(hours)}h {int(minutes)}m {int(seconds)}s"
    )


if __name__ == "__main__":
    try:
        pmtfy_wrap()
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        sys.exit(1)
    finally:
        logging.info("PMTfication process completed.")
        sys.exit(0)
