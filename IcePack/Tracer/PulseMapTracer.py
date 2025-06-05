import pandas as pd
import sqlite3 as sql
import os
from Tracer.Tracer import Tracer


class PulseMapTracer(Tracer):
    def __init__(self, source_root: str):
        super().__init__(source_root)

    def event_tracer(self, event_no: int) -> pd.DataFrame:
        """
        Given an enhanced event number, trace back the file and return the corresponding
        SRTInIcePulses and truth tables for the short event_no.
        """
        disintegrated_event_receipt = Tracer.disintegrate_enhanced_event_no(
            event_no
        )
        db_path = self._build_db_path(disintegrated_event_receipt)
        short_event_no = disintegrated_event_receipt["original_event_no"]

        srt_df = self._get_event_df_from_table(
            db_path, "SRTInIcePulses", short_event_no
        )
        truth_df = self._get_event_df_from_table(
            db_path, "truth", short_event_no
        )
        df = pd.merge(srt_df, truth_df, on="event_no", how="inner")

        return df

    def _build_db_path(self, disintegrated_event_receipt: dict) -> str:
        """
        Build the path to the database file based on the disintegrated event information.
        """
        signal_type = disintegrated_event_receipt["signal_type"]
        subdir_tag = disintegrated_event_receipt["subdir_tag"]
        part_no = disintegrated_event_receipt["part_no"]

        # Construct the path
        db_file_path = os.path.join(
            self.source_root,
            signal_type,
            f"220{subdir_tag}",
            f"merged_part_{part_no}.db",
        )
        return db_file_path

    def _get_event_df_from_table(
        self, db_file: str, table: str, event_no: int
    ) -> pd.DataFrame:
        query = f"SELECT * FROM {table} WHERE event_no = ?"
        with sql.connect(db_file) as con:
            df = pd.read_sql_query(query, con, params=(event_no,))
        return df
