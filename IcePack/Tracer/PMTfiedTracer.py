import pandas as pd
import os
import pyarrow.parquet as pq
from Tracer.Tracer import Tracer


class PMTfiedTracer(Tracer):
    def __init__(self, source_root: str):
        super().__init__(source_root)

    def event_tracer(self, event_no: int) -> pd.DataFrame:
        """
        Given an enhanced event number, trace back the file and return the corresponding
        PMTfied and truth tables for the short event_no.
        """
        disintegrated_event_receipt = Tracer.disintegrate_enhanced_event_no(
            event_no
        )
        truth_file = self._build_truth_path(disintegrated_event_receipt)
        this_event_truth_df = self._get_truth_df_of_this_event(
            truth_file, event_no
        )
        shard_file = self._build_shard_path(
            disintegrated_event_receipt, this_event_truth_df
        )
        this_event_shard_df = self._get_event_df_from_table(
            shard_file, event_no
        )
        df = pd.merge(
            this_event_shard_df,
            this_event_truth_df,
            on="event_no",
            how="inner",
        )
        return df

    def _build_truth_path(self, disintegrated_event_receipt: dict) -> str:
        subdir_tag = disintegrated_event_receipt["subdir_tag"]
        subdir_path = os.path.join(self.source_root, f"220{subdir_tag}")
        part = disintegrated_event_receipt["part_no"]
        truth_file = os.path.join(subdir_path, f"truth_{part}.parquet")

        return truth_file

    def _get_truth_df_of_this_event(
        self, truth_file: str, event_no: int
    ) -> pd.DataFrame:
        truth_df = pq.read_table(truth_file).to_pandas()
        return truth_df[truth_df["event_no"] == event_no]

    def _build_shard_path(
        self,
        disintegrated_event_receipt: dict,
        this_event_truth_df: pd.DataFrame,
    ) -> str:
        shard_no = this_event_truth_df["shard_no"].values[0]
        subdir_tag = disintegrated_event_receipt["subdir_tag"]
        subdir_path = os.path.join(self.source_root, f"220{subdir_tag}")
        part = disintegrated_event_receipt["part_no"]
        shard_file = os.path.join(
            subdir_path, str(part), f"PMTfied_{int(shard_no)}.parquet"
        )
        return shard_file

    def _get_event_df_from_table(
        self, shard_file: str, event_no: int
    ) -> pd.DataFrame:
        shard_df = pq.read_table(shard_file).to_pandas()
        return shard_df[shard_df["event_no"] == event_no]
