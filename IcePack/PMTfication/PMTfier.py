"""
@author: cyan.jo
Summary:
Transforms IceCube pulsemap data into PMT-wise representations. 
As of 2025.05.17, this only considers a single-PMT-at-a-single-DOM, perhaps can be extended to multi-PMTs in the future.
The script is highly source file storage layout dependent,
and the user is expected to provide the source layout and destination root directory.

The source layout should be created by the user, based on the source data layout.
A layout can be created by inheriting SourceLayout, TableNameMixin
    ```python
    from .Base import SourceLayout, TableNameMixin
    class SourceLayoutExample(SourceLayout, TableNameMixin):
    ```

If the data files are distributed according to the flavours,
    ```python
    from .Base import FlavouredSourceLayout, TableNameMixin
    class SourceLayoutFlavouredExample(FlavouredSourceLayout, TableNameMixin):
    ```

See SourceLayoutHESnowStorm with figures in README.md for example.

Usage:
    pmtfier = PMTfier(
        source_root="path/to/source",
        source_layout=SourceLayout(),
        dest_root="path/to/destination",
        N_events_per_shard=1000,
        summary_mode=SummaryMode.CLASSIC,
    )

pmtfy_part:  processes a single database file.
divide_and_conquer_part:  divides the event numbers into batches and processes each batch separately.
pmtfy_shard:  processes a single shard of data.
truth_maker:  creates the truth table for the PMTfied data.
pmtfy_shard:  processes a single shard of data.

"""

import sqlite3 as sql
import os
import time

from typing import List

import pyarrow as pa
import pyarrow.parquet as pq

import logging

from IcePack.PMTfication.PMTSummariser import PMTSummariser
from IcePack.PMTfication.PMTTruthMaker import PMTTruthMaker
from IcePack.PMTfication.PMTTruthFromSummary import PMTTruthFromSummary
from tabulate import tabulate

from IcePack.PMTfication.Layout.SourceLayout import SourceLayout
from IcePack.Enum.SummaryMode import SummaryMode


class PMTfier:
    def __init__(
        self,
        source_root: str,
        source_layout: SourceLayout,
        dest_root: str,
        summary_mode: SummaryMode = SummaryMode.CLASSIC,
    ) -> None:
        self.source_root = source_root
        self.dest_root = dest_root
        self.source_layout = source_layout
        self.family = source_layout.family
        self.source_subdirectory = source_layout.subdir
        self.subdir_tag = self._get_subdir_tag()

        self.pulsemap_table_name = source_layout.pulsemap_table_name
        self.truth_table_name = source_layout.truth_table_name
        self.HighestEInIceParticle_table_name = (
            source_layout.highest_E_in_ice_particle_table_name
        )
        self.HE_dauther_table_name = (
            source_layout.highest_E_daughter_table_name
        )
        self.MC_weight_dict_table_name = source_layout.mc_weight_table_name
        self.N_events_per_shard = source_layout.get_N_events_per_shard()
        self.summary_mode = summary_mode

    def __call__(self, part_no: int) -> None:
        source_part_file = os.path.join(
            self.source_root,
            self.family,
            self.source_subdirectory,
            self.source_layout.get_db_file_name(part_no),
        )
        self.pmtfy_part(source_part_file)

    def _get_table_event_count(self, conn: sql.Connection, table: str) -> int:
        cursor = conn.cursor()
        cursor.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table}_event_no ON {table}(event_no)"
        )
        cursor.execute(f"SELECT COUNT(DISTINCT event_no) FROM {table}")
        event_count = cursor.fetchone()[0]
        return event_count

    def _get_event_no_batches(
        self, conn: sql.Connection, table: str, batch_size: int
    ):
        last_event_no = None
        while True:
            query = f"""
            SELECT DISTINCT event_no
            FROM {table}
            {f"WHERE event_no > {last_event_no}" if last_event_no else ""}
            ORDER BY event_no
            LIMIT {batch_size}
            """
            cursor = conn.cursor()
            cursor.execute(query)
            batch = [row[0] for row in cursor.fetchall()]
            if not batch:
                break
            last_event_no = batch[-1]
            yield batch

    def _get_subdir_tag(self) -> int:
        if self.family not in ["Snowstorm", "Corsika"]:
            raise ValueError(f"Invalid data family: {self.family}.")
        if self.family == "Snowstorm":
            # "22012" -> 12
            # "22017" -> 17
            subdir_tag = int(self.source_subdirectory[-2:])
        elif self.family == "Corsika":
            # "0002000-0002999" -> 20
            # "0003000-0003999" -> 30
            range_start = self.source_subdirectory.split("-")[0]
            range_end = self.source_subdirectory.split("-")[1]
            first_half_subdir_no = range_start[3:5]
            second_half_subdir_no = range_end[3:5]
            combined_subdir_no = int(
                first_half_subdir_no + second_half_subdir_no
            )
            subdir_tag = combined_subdir_no
        else:
            raise ValueError(f"Invalid destination root: {self.dest_root}.")
        return subdir_tag

    def _add_enhance_event_no(
        self, pa_table: pa.Table, part_no: int
    ) -> pa.Table:
        """
        (1) (2)  (3)  (4)
        1   12  0027 00012345

        (1)snowstorm(1) or corsika(2)
        (2)subdir_tag
        (3)part_no
        (4)original event_no
        """
        if self.family not in ["Snowstorm", "Corsika"]:
            raise ValueError(f"Invalid data family: {self.family}.")
        if self.family == "Snowstorm":
            family_tag = "1"
        elif self.family == "Corsika":
            family_tag = "2"
        else:
            family_tag = "0"

        if "event_no" in pa_table.schema.names:
            original_event_no = pa_table["event_no"]  # .cast(pa.int32())
            enhanced_event_no = [
                int(
                    f"{family_tag}{self.subdir_tag:02}{part_no:04}{event_no.as_py():08}"
                )
                for event_no in original_event_no
            ]

            pa_table = pa_table.remove_column(
                pa_table.schema.get_field_index("event_no")
            )
            pa_table = pa_table.append_column(
                "original_event_no",
                pa.array(original_event_no, type=pa.int32()),
            )
            pa_table = pa_table.append_column(
                "event_no", pa.array(enhanced_event_no, type=pa.int64())
            )

            reordered_columns = ["event_no", "original_event_no"] + [
                name
                for name in pa_table.schema.names
                if name not in ("event_no", "original_event_no")
            ]
            pa_table = pa_table.select(reordered_columns)

        return pa_table

    def pmtfy_shard(
        self,
        con_source: sql.Connection,
        part_no: int,
        shard_no: int,
        truth_maker: PMTTruthMaker,
        event_batch: List[int],
    ) -> pa.Table:
        dest_dir = os.path.join(
            self.dest_root, self.source_subdirectory, str(part_no)
        )
        os.makedirs(dest_dir, exist_ok=True)

        # NOTE
        # PMTSummariser is the core class to be called for PMTfication
        pa_pmtfied = PMTSummariser(
            con_source=con_source,
            pulsemap_table_name=self.pulsemap_table_name,
            event_no_subset=event_batch,
            summary_mode=self.summary_mode,
        )()
        pa_pmtfied = self._add_enhance_event_no(pa_pmtfied, part_no)
        dest_dir = os.path.join(
            self.dest_root, self.source_subdirectory, str(part_no)
        )
        pmtfied_file = os.path.join(dest_dir, f"PMTfied_{shard_no}.parquet")
        pq.write_table(pa_pmtfied, pmtfied_file)

        summary_derived_truth = PMTTruthFromSummary(pa_pmtfied)()

        # NOTE
        # PMT truth table for this shard is created by PMTTruthMaker and returned
        pa_truth_shard = truth_maker(
            subdirectory_no=int(self.subdir_tag),
            part_no=part_no,
            shard_no=shard_no,
            event_no_subset=event_batch,
            summary_derived_truth_table=summary_derived_truth,
        )
        pa_truth_shard = self._add_enhance_event_no(pa_truth_shard, part_no)

        return pa_truth_shard

    def _divide_and_conquer_part(
        self,
        con_source: sql.Connection,
        part_no: int,
        truth_maker: PMTTruthMaker,
    ) -> pa.Table:
        truth_shards = []

        event_no_batches = self._get_event_no_batches(
            con_source, self.pulsemap_table_name, self.N_events_per_shard
        )
        for shard_no, event_batch in enumerate(event_no_batches, start=1):
            start_time = time.time()
            pa_truth_shard = self.pmtfy_shard(
                con_source=con_source,
                part_no=part_no,
                shard_no=shard_no,
                truth_maker=truth_maker,
                event_batch=event_batch,
            )
            truth_shards.append(pa_truth_shard)
            end_time = time.time()
            elapsed_time = end_time - start_time
            hours, remainder = divmod(elapsed_time, 3600)
            minutes, seconds = divmod(remainder, 60)
            logging.info(
                f"Elapsed time for shard {shard_no}: {int(hours)}h {int(minutes)}m {int(seconds)}s"
            )

        return pa.concat_tables(truth_shards)

    def pmtfy_part(self, source_part_file: str) -> None:
        """
        the primary function that operates on a single database file.
        (1) read the event_no from the database
        (2) create a new directory for the PMTfied files
        (3) create a new database file for the PMTfied data
        (4) create a new database file for the truth data
        (5) write the PMTfied data to the new database file
        (6) write the truth data to the new database file
        (7) log the size of the PMTfied data and truth data
        (9) close the database connection
        Args:
            source_part_file (str): _description_
        """
        part_no = self.source_layout.extract_part_no(source_part_file)
        source_size_MB = self.get_file_size_MB(source_part_file)
        logging.info(f"Source part file size: {source_size_MB:.2f} MB")

        con_source = sql.connect(source_part_file)

        truth_maker = PMTTruthMaker(
            con_source=con_source,
            pulsemap_table_name=self.pulsemap_table_name,
            truth_table_name=self.truth_table_name,
            HighestEInIceParticle_table_name=self.HighestEInIceParticle_table_name,
            HE_dauther_table_name=self.HE_dauther_table_name,
            MC_weight_dict_table_name=self.MC_weight_dict_table_name,
        )

        consolidated_truth = self._divide_and_conquer_part(
            con_source=con_source, part_no=part_no, truth_maker=truth_maker
        )

        dest_subdirectory_path = os.path.join(
            self.dest_root, self.source_subdirectory
        )
        os.makedirs(dest_subdirectory_path, exist_ok=True)
        consolidated_file = os.path.join(
            dest_subdirectory_path, f"truth_{part_no}.parquet"
        )
        pq.write_table(consolidated_truth, consolidated_file)

        truth_file_size_MB = self.get_file_size_MB(consolidated_file)

        dest_dir = os.path.join(
            self.dest_root, self.source_subdirectory, str(part_no)
        )
        shard_files = [
            f
            for f in os.listdir(dest_dir)
            if os.path.isfile(os.path.join(dest_dir, f))
            and f.endswith(".parquet")
            and "PMTfied" in f
        ]

        shard_sizes = []
        for f in shard_files:
            full_path = os.path.join(dest_dir, f)
            size_MB = self.get_file_size_MB(full_path)
            shard_sizes.append((f, f"{size_MB:.2f} MB"))

        total_shards_MB = sum(
            float(size.replace(" MB", "")) for _, size in shard_sizes
        )
        avg_size_MB = total_shards_MB / len(shard_sizes) if shard_sizes else 0

        # Construct and log the table
        table_data = shard_sizes + [
            ("Total PMTfied Size", f"{total_shards_MB:.2f} MB"),
            ("Avg PMTfied Shard Size", f"{avg_size_MB:.2f} MB"),
            ("Truth File Size", f"{truth_file_size_MB:.2f} MB"),
        ]
        table_str = tabulate(
            table_data, headers=["File", "Size"], tablefmt="pretty"
        )
        logging.info(f"\n{table_str}")

        con_source.close()

    def get_file_size_MB(self, path):
        return os.path.getsize(path) / (1024 * 1024)

    def get_total_dir_size_MB(self, dir_path: str) -> float:
        total = 0
        for file in os.listdir(dir_path):
            full_path = os.path.join(dir_path, file)
            if os.path.isfile(full_path) and file.endswith(".parquet"):
                total += os.path.getsize(full_path)
        return total / (1024 * 1024)
