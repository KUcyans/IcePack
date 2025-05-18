import pyarrow.parquet as pq
import numpy as np

from IcePack.EventFilter.EventFilter import EventFilter, override

"""
@author: cyan.jo
Summary:
Selects events fully contained within a defined IceCube volume

(1)Uses geometry boundaries to determine whether the event's start and end points are inside
(2)Allows custom padding to shrink the detection volume if needed
(3)Critical for avoiding edge effects in physics analyses
"""


class ContainmentFilter(EventFilter):
    def __init__(
        self,
        source_subdir: str,
        output_subdir: str,
        subdir_no: int,
        part_no: int,
    ):
        super().__init__(
            source_subdir=source_subdir,
            output_subdir=output_subdir,
            subdir_no=subdir_no,
            part_no=part_no,
        )

    @override
    def _set_valid_event_nos(self):
        truth_table = pq.read_table(self.source_truth_file)

        required_columns = {"isWithinIceCube", "N_doms", "event_no"}
        missing_columns = required_columns - set(truth_table.column_names)
        if missing_columns:
            self.logger.error(
                f"Missing required columns: {missing_columns}. Cannot proceed with filtering."
            )
            return

        is_within_icecube = truth_table.column("isWithinIceCube").to_numpy()
        valid_indices = np.where(is_within_icecube)[0]

        if len(valid_indices) == 0:
            self.logger.warning(
                f"No valid events found within containment region in {self.subdir_no}/{self.part_no}. Skipping filtering."
            )
            return

        self.valid_event_nos = set(
            truth_table.take(valid_indices).column("event_no").to_pylist()
        )
        self.logger.info(
            f"Extracted {len(self.valid_event_nos)} valid events within the IceCube body."
        )
