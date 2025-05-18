import pyarrow.parquet as pq
import pyarrow.compute as pc
import pyarrow as pa

from IcePack.EventFilter.EventFilter import EventFilter, override

"""
@author: cyan.jo
Summary:
Filters for charged-current (CC) neutrino interactions

(1)Selects only events where the interaction type is charged-current
(2)Applies based on the `interaction_type` field in event metadata
"""


class CCFilter(EventFilter):
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
        required_columns = {"InteractionType", "N_doms", "event_no"}
        missing_columns = required_columns - set(truth_table.column_names)
        if missing_columns:
            self.logger.error(
                f"Missing required columns: {missing_columns}. Cannot proceed with filtering."
            )
            return

        interaction_type = truth_table.column("InteractionType")  # 1:CC, 2:NC
        selection_value = 1
        valid_indices = (
            pc.equal(
                interaction_type,
                pa.scalar(selection_value, type=interaction_type.type),
            )
            .to_numpy()
            .nonzero()[0]
        )
        if len(valid_indices) == 0:
            self.logger.warning(
                f"No valid CC events found in {self.subdir_no}/{self.part_no}. Skipping filtering."
            )
            return
        self.valid_event_nos = set(
            truth_table.take(valid_indices).column("event_no").to_pylist()
        )
        self.logger.info(
            f"Extracted {len(self.valid_event_nos)} valid events which are CC interactions."
        )
