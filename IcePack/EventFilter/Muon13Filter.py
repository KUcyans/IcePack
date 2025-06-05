import pyarrow.parquet as pq
import pyarrow.compute as pc
import pyarrow as pa

from EventFilter import EventFilter, override


class Muon13Filter(EventFilter):
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
        required_columns = {"MuonFilter_13", "N_doms", "event_no"}
        missing_columns = required_columns - set(truth_table.column_names)
        if missing_columns:
            self.logger.error(
                f"Missing required columns: {missing_columns}. Cannot proceed with filtering."
            )
            return

        muon_filter = truth_table.column("MuonFilter_13")
        valid_indices = (
            pc.equal(muon_filter, pa.scalar(1, type=muon_filter.type))
            .to_numpy()
            .nonzero()[0]
        )
        if len(valid_indices) == 0:
            self.logger.warning(
                f"No valid MuonFilter_13 events found in {self.subdir_no}/{self.part_no}. Skipping filtering."
            )
            return
        self.valid_event_nos = set(
            truth_table.take(valid_indices).column("event_no").to_pylist()
        )
        self.logger.info(
            f"Extracted {len(self.valid_event_nos)} valid events where MuonFilter_13==True."
        )
