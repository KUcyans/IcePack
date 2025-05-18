import pyarrow as pa
import pyarrow.parquet as pq
import os
import abc
import pyarrow.compute as pc
import logging
from functools import wraps

"""
@author: cyan.jo
Summary:
Abstract base class for all event filters.

(1)Defines a unified interface by requiring subclasses to implement the callable
(2)Intended to be inherited by specific filters
(3)Encourages modular filtering of events based on various physics or geometry constraints
"""


class EventFilter(abc.ABC):
    def __init__(
        self,
        source_subdir: str,
        output_subdir: str,
        subdir_no: int,
        part_no: int,
        **kwargs,
    ):
        self.source_subdir = source_subdir
        self.output_subdir = output_subdir
        self.subdir_no = subdir_no
        self.part_no = part_no
        self.extra_params = kwargs

        self.source_truth_file = os.path.join(
            self.source_subdir, f"truth_{self.part_no}.parquet"
        )
        self.output_truth_file = os.path.join(
            self.output_subdir, f"truth_{self.part_no}.parquet"
        )

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        self.logger = logging.getLogger("EventFilter")
        self.logger.info(
            f"Initialized {self.__class__.__name__} filter for {self.subdir_no}/{self.part_no}"
        )
        self._set_valid_event_nos()

    def __call__(self):
        self.logger.info(
            f"Starting filtering process for {self.subdir_no}/{self.part_no}"
        )
        self._write_filtered_truth()
        self._write_filtered_shards()
        self.logger.info("Filtering process completed.")

    def get_valid_event_nos(self):
        return self.valid_event_nos

    @abc.abstractmethod
    def _set_valid_event_nos(self):
        pass

    def _write_filtered_truth(self):
        if not self.valid_event_nos:
            self.logger.warning(
                f"No valid events found. Skipping truth file writing for {self.part_no}."
            )
            return

        truth_table = pq.read_table(self.source_truth_file)
        valid_indices = [
            i
            for i, eno in enumerate(truth_table.column("event_no").to_pylist())
            if eno in self.valid_event_nos
        ]

        if not valid_indices:
            self.logger.warning(
                f"No valid events found. Skipping truth file writing for {self.part_no}."
            )
            return

        filtered_truth_table = truth_table.take(valid_indices)
        filtered_truth_table = self._recalculate_offset(filtered_truth_table)

        pq.write_table(filtered_truth_table, self.output_truth_file)
        self.logger.info(
            f"Filtered truth file saved to: {self.output_truth_file}"
        )

    def _write_filtered_shards(self):
        """Filters and writes PMTfied files using the extracted valid event numbers."""
        source_pmtfied_dir = os.path.join(
            self.source_subdir, str(self.part_no)
        )
        output_pmtfied_dir = os.path.join(
            self.output_subdir, str(self.part_no)
        )
        os.makedirs(output_pmtfied_dir, exist_ok=True)

        for file in os.listdir(source_pmtfied_dir):
            source_pmtfied_file = os.path.join(source_pmtfied_dir, file)
            output_pmtfied_file = os.path.join(output_pmtfied_dir, file)

            pmt_table = pq.read_table(source_pmtfied_file)
            pmt_event_nos = pmt_table.column("event_no").to_pylist()
            valid_indices = [
                i
                for i, eno in enumerate(pmt_event_nos)
                if eno in self.valid_event_nos
            ]

            if valid_indices:
                filtered_pmt_table = pmt_table.take(valid_indices)
                pq.write_table(filtered_pmt_table, output_pmtfied_file)
                self.logger.info(
                    f"Filtered PMTfied file saved to: {output_pmtfied_file}"
                )
            else:
                self.logger.warning(
                    f"Skipping {file}: No valid events found in PMTfied file."
                )

    def _recalculate_offset(self, truth_table: pa.Table) -> pa.Table:
        if (
            "N_doms" not in truth_table.column_names
            or "shard_no" not in truth_table.column_names
        ):
            self.logger.error(
                "Required columns 'N_doms' or 'shard_no' are missing from truth table. Cannot recalculate 'offset'."
            )
            return truth_table

        truth_data = {
            col: truth_table.column(col).to_pylist()
            for col in truth_table.column_names
        }

        shard_no = pa.array(truth_data["shard_no"])
        n_doms = pa.array(truth_data["N_doms"])

        offsets = []
        unique_shards = pc.unique(shard_no).to_pylist()

        for shard in unique_shards:
            mask = pc.equal(shard_no, shard)
            filtered_n_doms = pc.if_else(
                mask, n_doms, pa.scalar(0, n_doms.type)
            )  # Keep only values of the shard, set others to 0
            cumulative = pc.cumulative_sum(filtered_n_doms)
            offsets.append(pc.if_else(mask, cumulative, None))

        truth_data["offset"] = pc.coalesce(*offsets)

        self.logger.info(
            "Recalculated 'offset' column based on 'N_doms' values within each 'shard_no' group."
        )
        return pa.Table.from_pydict(truth_data)

    def get_receipt_info(self):
        if not os.path.isfile(self.source_truth_file):
            self.logger.error(
                f"Truth file not found: {self.source_truth_file}"
            )
            return {
                "filter": self.__class__.__name__,
                "error": "Source truth file missing",
            }

        # Get initial event count
        initial_event_count = pq.read_table(self.source_truth_file).num_rows
        reduced_event_count = 0

        if os.path.isfile(self.output_truth_file):
            reduced_event_count = pq.read_table(
                self.output_truth_file
            ).num_rows
        else:
            self.logger.warning(
                f"Output truth file not found for {self.__class__.__name__}. Assuming 0 surviving events."
            )

        # Compute survival ratio safely
        survival_ratio = (
            reduced_event_count / initial_event_count
            if initial_event_count > 0
            else 0
        )
        reduction_ratio = 1 - survival_ratio

        return {
            "selected_events": len(self.valid_event_nos),
            "initial_event_count": initial_event_count,
            "reduced_event_count": reduced_event_count,
            "survival_ratio": round(survival_ratio, 4),
            "reduction_ratio": round(reduction_ratio, 4),
        }


def override(method):
    """Decorator to indicate a method overrides a superclass method."""

    @wraps(method)
    def wrapper(*args, **kwargs):
        return method(*args, **kwargs)

    return wrapper
