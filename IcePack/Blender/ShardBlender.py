import os
from typing import Dict, List, Tuple

import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from IcePack.Enum.Flavour import Flavour
from IcePack.Enum.EnergyRange import EnergyRange

"""
@author: cyan.jo
Summary:
Blends PMTfied shard files from high- and low-energy directories into unified, reorganised shards for downstream usage
(1)Gathers truth metadata from truth_*.parquet files
(2) Retrieves corresponding PMTfied slices using an internal lookup
(3) Groups events into shards, each containing n_events_per_shard entries
(4) Saves new PMTfied shard files and updates the associated truth metadata with shard_no and offset
"""


class ShardBlender:
    def __init__(
        self,
        source_dir: str,
        energy_range_low: EnergyRange,
        energy_range_high: EnergyRange,
        energy_range_combined: EnergyRange,
        flavour: Flavour,
        n_events_per_shard: int = 3000,
    ):
        self.source_dir = source_dir
        self.flavour = flavour
        self.n_events_per_shard = n_events_per_shard

        self.subdir_low = os.path.join(
            source_dir, EnergyRange.get_subdir(energy_range_low, flavour)
        )
        self.subdir_high = os.path.join(
            source_dir, EnergyRange.get_subdir(energy_range_high, flavour)
        )
        self.subdir_combined = os.path.join(
            source_dir, EnergyRange.get_subdir(energy_range_combined, flavour)
        )

        self.event_slice_lookup = self._build_event_lookup()

    def __call__(self):
        self.blend()

    def blend(self):
        truth_files = self._get_truth_file_list(self.subdir_combined)
        for truth_file in tqdm(truth_files, desc="Processing truth parts"):
            new_part_no = int(
                os.path.basename(truth_file).split("_")[1].split(".")[0]
            )
            print(f"------------ Blending Part {new_part_no} ------------")

            truth_table = pq.read_table(truth_file)
            event_nos = truth_table["event_no"].to_pylist()
            n_doms = truth_table["N_doms"].to_pylist()

            shard_tables, shard_assignment = self._generate_shards(event_nos)

            for shard_no, shard_table in shard_tables:
                self._write_new_shard(new_part_no, shard_no, shard_table)

            self._update_truth_table(truth_file, shard_assignment)

    def _get_truth_file_list(self, subdir: str) -> List[str]:
        return [
            os.path.join(subdir, f)
            for f in os.listdir(subdir)
            if f.startswith("truth_") and f.endswith(".parquet")
        ]

    def _build_event_lookup(self) -> Dict[int, Tuple[str, int, int]]:
        """
        Returns mapping: event_no → (file_path, slice_start, slice_length)
        """
        lookup = {}
        for subdir in [self.subdir_low, self.subdir_high]:
            for fname in os.listdir(subdir):
                if not fname.startswith("truth_") or not fname.endswith(
                    ".parquet"
                ):
                    continue
                part_no = int(fname.split("_")[1].split(".")[0])
                path = os.path.join(subdir, fname)
                table = pq.read_table(path)
                for e, s, o, n in zip(
                    table["event_no"],
                    table["shard_no"],
                    table["offset"],
                    table["N_doms"],
                ):
                    shard_no = s.as_py()
                    offset = o.as_py()
                    n_dom = n.as_py()
                    shard_path = os.path.join(
                        subdir, str(part_no), f"PMTfied_{shard_no}.parquet"
                    )
                    start = offset - n_dom
                    lookup[e.as_py()] = (shard_path, start, n_dom)
        return lookup

    def _generate_shards(
        self, event_nos: List[int]
    ) -> Tuple[List[Tuple[int, pa.Table]], Dict[int, Tuple[int, int]]]:
        """
        Returns:
            - list of new shards (index, table)
            - event_no → (new_shard_no, N_doms)
        """
        cache = {}
        shard_list = []
        shard_assignment = {}

        buffer_rows = []
        buffer_meta = []  # (event_no, n_dom)
        shard_index = 1

        for event_no in event_nos:
            fpath, start, length = self.event_slice_lookup[event_no]
            if fpath not in cache:
                cache[fpath] = pq.read_table(fpath)
            rows = cache[fpath].slice(start, length)

            buffer_rows.append(rows)
            buffer_meta.append((event_no, length))

            if len(buffer_meta) >= self.n_events_per_shard:
                shard_table = pa.concat_tables(buffer_rows)
                for e, n in buffer_meta:
                    shard_assignment[e] = (shard_index, n)
                shard_list.append((shard_index, shard_table))
                shard_index += 1
                buffer_rows.clear()
                buffer_meta.clear()

        if buffer_rows:
            shard_table = pa.concat_tables(buffer_rows)
            for e, n in buffer_meta:
                shard_assignment[e] = (shard_index, n)
            shard_list.append((shard_index, shard_table))

        return shard_list, shard_assignment

    def _write_new_shard(
        self, new_part_no: int, new_shard_no: int, table: pa.Table
    ) -> None:
        subdir_no = os.path.basename(self.subdir_combined)
        out_dir = os.path.join(self.source_dir, subdir_no, str(new_part_no))
        os.makedirs(out_dir, exist_ok=True)
        out_file = os.path.join(out_dir, f"PMTfied_{new_shard_no}.parquet")
        pq.write_table(table, out_file)
        print(f"[✔] Wrote PMTfied shard {new_shard_no} to {out_file}")

    def _update_truth_table(
        self, truth_file: str, shard_assignment: Dict[int, Tuple[int, int]]
    ) -> None:
        table = pq.read_table(truth_file)
        event_nos = table["event_no"].to_pylist()

        shard_nos = []
        offsets = []

        current_offset = 0
        prev_shard = None

        for e in event_nos:
            shard_no, n_dom = shard_assignment.get(e, (None, None))
            shard_nos.append(shard_no)

            if shard_no != prev_shard:
                current_offset = 0
            current_offset += n_dom
            offsets.append(current_offset)

            prev_shard = shard_no

        updated_dict = table.to_pydict()
        updated_dict["shard_no"] = shard_nos
        updated_dict["offset"] = offsets

        updated_table = pa.Table.from_pydict(updated_dict)
        pq.write_table(updated_table, truth_file)
        print(f"[✔] Updated offsets and shard_no in {truth_file}")
