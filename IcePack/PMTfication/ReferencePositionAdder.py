import numpy as np
import sqlite3 as sql
from typing import List
import pyarrow as pa
import os
import logging

"""
@author: cyan.jo
Summary:
Adds reference PMT positions to existing event records.
The purpose of this is:
if the DOM position of a single DOM is not consistent across events or different data sets,
The PMT-wise summarisation will break as the identification of the DOM is not consistent.
ReferencePositionAdder temporarily assigns a string and dom_number to each DOM in the event 
to ensure unique combinations of(string, dom_number) are used to process the data in PMTSummariser.

If the DOM position is within a certain tolerance of the reference position, it is assigned the string and dom_number

The reference position is taken from a CSV file containing the string and dom_number of each DOM.
```bash
string,dom_number,dom_x,dom_y,dom_z
1.0,1.0,-256.14,-521.08,496.03
1.0,2.0,-256.14,-521.08,479.01
1.0,3.0,-256.14,-521.08,461.99
...
```
These positions are extracted from multiple Corsika files, and missing positions are filled in using linear interpolation.
"""


class ReferencePositionAdder:
    def __init__(
        self,
        con_source: sql.Connection,
        pulsemap_table_name: str,
        event_no_subset: List[int],
        tolerance_xy: float = 10,
        tolerance_z: float = 2,
    ) -> None:
        self.con_source = con_source
        self.pulsemap_table_name = pulsemap_table_name
        self.event_no_subset = event_no_subset

        # HACK:
        # should be replaced by a general reference dom position
        # a GEO file may be able to be used
        reference_path = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__),
                "..",
                "ReferencePosition",
                "unique_string_dom_completed.csv",
            )
        )

        self.reference_data = self._load_reference_data(reference_path)
        self.tolerance_xy = tolerance_xy
        self.tolerance_z = tolerance_z

    def __call__(self) -> None:
        self._add_columns_if_missing()
        self._update_string_dom_number()

    def _load_reference_data(self, filepath: str) -> np.ndarray:
        return np.loadtxt(filepath, delimiter=",", skiprows=1)

    def _add_columns_if_missing(self) -> None:
        cur_source = self.con_source.cursor()
        cur_source.execute(f"PRAGMA table_info({self.pulsemap_table_name})")
        cur_source.execute(
            f"ALTER TABLE {self.pulsemap_table_name} ADD COLUMN string INTEGER"
        )
        cur_source.execute(
            f"ALTER TABLE {self.pulsemap_table_name} ADD COLUMN dom_number INTEGER"
        )
        self.con_source.commit()
        self._create_indexes()

    def _create_indexes(self) -> None:
        cur_source = self.con_source.cursor()
        indexes = [
            ("idx_event_no", "event_no"),
            ("idx_dom_position", "dom_x, dom_y, dom_z"),
            ("idx_string_dom_number", "string, dom_number"),
        ]
        for idx_name, columns in indexes:
            try:
                cur_source.execute(
                    f"CREATE INDEX IF NOT EXISTS {idx_name} ON {self.pulsemap_table_name} ({columns})"
                )
            except sql.Error as e:
                logging.error(f"Failed to create index {idx_name}: {e}")
        self.con_source.commit()

    def _update_string_dom_number(self) -> None:
        """
        Update the string and dom_number columns based on reference data matching.
        """
        cur_source = self.con_source.cursor()

        # Compute bounding box for filtering reference data
        event_filter = ",".join(map(str, self.event_no_subset))
        query = f"""
            SELECT MIN(dom_x), MAX(dom_x), MIN(dom_y), MAX(dom_y)
            FROM {self.pulsemap_table_name}
            WHERE event_no IN ({event_filter})
        """
        cur_source.execute(query)
        bounds = cur_source.fetchone()

        relevant_data = self._filter_relevant_reference_data(bounds)
        rows_to_update_df = self._fetch_rows_to_update()

        if rows_to_update_df.empty:
            return

        rows_to_update = rows_to_update_df.to_records(index=False).tolist()
        row_ids, dom_xs, dom_ys, dom_zs = np.array(rows_to_update).T

        # Vectorised matching for XY tolerances
        xy_matches = (
            np.abs(relevant_data[:, 2][:, None] - dom_xs) <= self.tolerance_xy
        ) & (
            np.abs(relevant_data[:, 3][:, None] - dom_ys) <= self.tolerance_xy
        )

        # Filter relevant_data for matches
        matching_relevant_data = relevant_data[np.any(xy_matches, axis=1)]

        # Vectorised matching for Z tolerance
        z_matches = (
            np.abs(matching_relevant_data[:, 4][:, None] - dom_zs)
            <= self.tolerance_z
        )

        # Get matching rows
        matched_rows = np.where(z_matches)

        matched_strings = matching_relevant_data[matched_rows[0], 0]
        matched_dom_numbers = matching_relevant_data[matched_rows[0], 1]

        updates = list(
            zip(matched_strings, matched_dom_numbers, row_ids[matched_rows[1]])
        )

        # batch updates executed
        cur_source.executemany(
            f"UPDATE {self.pulsemap_table_name} SET string = ?, dom_number = ? WHERE rowid = ?",
            updates,
        )
        self.con_source.commit()

    def _filter_relevant_reference_data(self, bounds: tuple) -> np.ndarray:
        """
        Filter reference data based on bounding box and tolerances.
        """
        min_dom_x, max_dom_x, min_dom_y, max_dom_y = bounds
        tol_xy = self.tolerance_xy
        return self.reference_data[
            np.logical_and.reduce(
                (
                    self.reference_data[:, 2] >= min_dom_x - tol_xy,
                    self.reference_data[:, 2] <= max_dom_x + tol_xy,
                    self.reference_data[:, 3] >= min_dom_y - tol_xy,
                    self.reference_data[:, 3] <= max_dom_y + tol_xy,
                )
            )
        ]

    def _fetch_rows_to_update(self) -> pa.Table:
        event_filter = ",".join(map(str, self.event_no_subset))
        query = f"""
            SELECT rowid, dom_x, dom_y, dom_z 
            FROM {self.pulsemap_table_name}
            WHERE event_no IN ({event_filter}) 
            AND (string IS NULL OR dom_number IS NULL) 
        """
        try:
            cursor = self.con_source.cursor()
            cursor.execute(query)
            rows_to_update = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            table = pa.Table.from_arrays(
                [
                    pa.array([row[i] for row in rows_to_update])
                    for i in range(len(column_names))
                ],
                names=column_names,
            )
            return table
        except Exception as e:
            logging.error(f"Error fetching rows to update: {e}")
