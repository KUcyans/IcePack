import os
from tqdm import tqdm

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

from typing import List, Tuple
from IcePack.Enum.Flavour import Flavour
from IcePack.Enum.EnergyRange import EnergyRange
import json

"""
@author: cyan.jo
Summary:
Combines low-energy and high-energy truth data into balanced datasets, filters by energy, and generates merged truth parts.

(1)Filters low-energy events by a threshold (energy_cutoff).
(2)Balances both energy domains by selecting the same number of events from each.
(3)Interleaves them in alternating order to ensure a mixed dataset.
(4)Splits the combined truth table into evenly sized parts and saves them.
(5)Produces a receipt (.json) describing how many events came from each domain.
"""


class TruthBlender:
    def __init__(
        self,
        source_dir: str,
        energy_range_low: EnergyRange,
        energy_range_high: EnergyRange,
        energy_range_combined: EnergyRange,
        flavour: Flavour,
        n_events_per_part: int = 30_000,
        energy_cutoff: float = 1e5,
    ):
        self.source_dir = source_dir
        self.flavour = flavour
        self.energy_cutoff = energy_cutoff
        self.subdir_low = os.path.join(
            source_dir, EnergyRange.get_subdir(energy_range_low, flavour)
        )
        self.subdir_high = os.path.join(
            source_dir, EnergyRange.get_subdir(energy_range_high, flavour)
        )
        self.subdir_combined = os.path.join(
            source_dir, EnergyRange.get_subdir(energy_range_combined, flavour)
        )
        self.n_events_per_part = n_events_per_part
        self.n_events_combined = None  # will be set later
        self.n_parts = None

    def __call__(self):
        self.blend()

    def blend(self) -> None:
        truth_files_low = self._get_truth_file_list(self.subdir_low)
        truth_files_high = self._get_truth_file_list(self.subdir_high)

        # 10Tev-1PeV needs filtering
        tables_low = [
            self._filter_truth_table(self._get_truth_table(f))
            for f in truth_files_low
        ]
        tables_high = [self._get_truth_table(f) for f in truth_files_high]

        # Concatenate all truth tables from each group
        full_table_low = pa.concat_tables(tables_low)
        full_table_high = pa.concat_tables(tables_high)

        # set the number of events to be combined
        # This determines the total event count of the resulting table
        self._set_combined_n_events(full_table_low, full_table_high)
        combined_table = self._update_subdir_and_combine(
            full_table_low, full_table_high
        )
        table_parts = self._split_table_into_parts(combined_table)

        for part_no, table_part in table_parts:
            self._write_combined_table(table_part, part_no)

        self._generate_receipt(
            n_low=self._get_n_events_truth(full_table_low),
            n_high=self._get_n_events_truth(full_table_high),
            n_total=self.n_events_combined,
        )

    def _get_truth_file_list(self, subdir: str) -> List[str]:
        return [
            os.path.join(subdir, f)
            for f in os.listdir(subdir)
            if f.endswith(".parquet")
        ]

    def _get_truth_table(self, truth_file: str) -> pa.Table:
        return pq.read_table(truth_file)

    def _filter_truth_table(self, table: pa.Table) -> pa.Table:
        return table.filter(pc.greater(table["energy"], self.energy_cutoff))

    def _get_n_events_truth(self, table: pa.Table) -> int:
        return table.num_rows

    def _set_combined_n_events(
        self, table_low: pa.Table, table_high: pa.Table
    ) -> None:
        n_low = self._get_n_events_truth(table_low)
        n_high = self._get_n_events_truth(table_high)
        n_combined = 2 * min(n_low, n_high)
        self.n_events_combined = n_combined

    # concatenate the two pyarrow tables
    def _update_subdir_and_combine(
        self, table_low: pa.Table, table_high: pa.Table
    ) -> pa.Table:
        n = self.n_events_combined // 2
        sliced_low = table_low.slice(0, n)
        sliced_high = table_high.slice(0, n)

        subdir_tag = int(os.path.basename(self.subdir_combined)[-2:])

        arrays = []
        for col in tqdm(
            table_low.schema.names, desc="Interleaving truth columns"
        ):
            if col == "subdirectory_no":
                col_low = pa.array([subdir_tag] * n)
                col_high = pa.array([subdir_tag] * n)
            else:
                col_low = sliced_low[col].combine_chunks()
                col_high = sliced_high[col].combine_chunks()

            interleaved = pa.chunked_array(
                [
                    pa.concat_arrays([col_low[i : i + 1], col_high[i : i + 1]])
                    for i in range(n)
                ]
            )
            arrays.append(interleaved)

        return pa.Table.from_arrays(arrays, schema=table_low.schema)

    def _split_table_into_parts(
        self, table: pa.Table
    ) -> List[Tuple[int, pa.Table]]:
        parts = []
        total = table.num_rows
        for i, start in enumerate(
            range(0, total, self.n_events_per_part), start=1
        ):
            part = table.slice(start, self.n_events_per_part)
            # replace part_no column here
            part_no_array = pa.array([i] * part.num_rows)
            arrays = []
            for col in part.schema.names:
                if col == "part_no":
                    arrays.append(part_no_array)
                else:
                    arrays.append(part[col])
            part_with_correct_partno = pa.Table.from_arrays(
                arrays, schema=part.schema
            )
            parts.append((i, part_with_correct_partno))
        return parts

    def _write_combined_table(
        self, combined_table: pa.Table, part_no: int
    ) -> None:
        output_dir = os.path.join(self.source_dir, self.subdir_combined)
        os.makedirs(output_dir, exist_ok=True)

        output_file = os.path.join(output_dir, f"truth_{part_no}.parquet")
        pq.write_table(combined_table, output_file)
        print(f"[✔] Wrote part {part_no}: {output_file}")

    def _generate_receipt(self, n_low: int, n_high: int, n_total: int) -> None:
        receipt = {
            "subdir_no": int(os.path.basename(self.subdir_combined)),
            "events_from_low_energy": n_low,
            "events_from_high_energy": n_high,
            "total_events_combined": n_total,
        }

        out_dir = os.path.join(self.source_dir, self.subdir_combined)
        os.makedirs(out_dir, exist_ok=True)
        out_file = os.path.join(
            out_dir, f"[Receipt]{receipt['subdir_no']}.json"
        )

        with open(out_file, "w") as f:
            json.dump(receipt, f, indent=4)
        print(f"[✔] Receipt written: {out_file}")
