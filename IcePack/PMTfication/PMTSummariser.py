import pyarrow as pa
import numpy as np
from collections import defaultdict
from typing import List, Dict, Tuple
import sqlite3 as sql
from scipy.spatial import ConvexHull
from scipy.spatial.distance import cdist
from sklearn.decomposition import PCA

from IcePack.PMTfication.ReferencePositionAdder import ReferencePositionAdder
from IcePack.Enum.SummaryMode import SummaryMode

"""
@author: cyan.jo
Summary:
The main feature summariser of the pulemap data.

Q25, Q75, Qtotal: accumulated charge after 25ns, 75ns, and total charge
T10, T50: time elapsed until 10% and 50% of the total charge is reached
sigmaT: standard deviation of the time of all pulses

t_qmax: time at the highest charge pulse
t_qmax_50: time at the highest charge pulse within t[:-50%]
T70, T90: time elapsed until 60% and 80% of the total charge is reached
Q_50t : accumulated charge until t_middle = (t[-1] - t[0]) / 2

TODO The core processing functions may be replaced by a class
TODO The pulses data format can be replaced by a class instead of List[List[float]]
TODO Consider adding these features:
    - t at where q is max. #  wait, there already is t_qmax ;;;
    - ...
    
NOTE data structure of events_doms_pulses
```python
    events_doms_pulses  : {
        event_no: {
            string: {
                dom_number: [pulse, ...],
                ...
            },
            ...
        },
        ...
    }
```
"""


class PMTSummariser:
    _SCHEMA = None
    _DEFAULT_ARRAYS = None

    def __init__(
        self,
        con_source: sql.Connection,
        pulsemap_table_name: str,
        event_no_subset: List[int],
        summary_mode: SummaryMode = SummaryMode.CLASSIC,
        Q_adj_cut_second_round: float = 0,
    ) -> None:
        self.con_source = con_source
        self.pulsemap_table_name = pulsemap_table_name
        self.event_no_subset = event_no_subset
        self.summary_mode = summary_mode
        self.n_pulse_collect = summary_mode.n_collect
        self.Q_adj_cut_second_round = Q_adj_cut_second_round

        query = f"SELECT * FROM {self.pulsemap_table_name} LIMIT 1"
        cur_source = self.con_source.cursor()
        cur_source.execute(query)
        columns = [description[0] for description in cur_source.description]
        need_string_dom_number = (
            "string" not in columns or "dom_number" not in columns
        )

        if need_string_dom_number:
            # NOTE
            # ReferencePositionAdder adds string and dom_number based on
            # the reference data
            # HACK The reference is hardcoded in PMT_ref_pos_adder.py
            ref_pos_adder = ReferencePositionAdder(
                con_source=con_source,
                pulsemap_table_name=pulsemap_table_name,
                event_no_subset=event_no_subset,
                tolerance_xy=10,
                tolerance_z=2,
            )
            ref_pos_adder()

        self.event_no_idx = columns.index("event_no")
        self.dom_string_idx = columns.index("string")
        self.dom_number_idx = columns.index("dom_number")
        self.dom_x_idx = columns.index("dom_x")
        self.dom_y_idx = columns.index("dom_y")
        self.dom_z_idx = columns.index("dom_z")
        self.dom_time_idx = columns.index("dom_time")
        self.dom_hlc_idx = columns.index("hlc")
        self.dom_charge_idx = columns.index("charge")
        self.pmt_area_idx = columns.index("pmt_area")
        self.rde_idx = columns.index("rde")
        self.saturation_status_idx = columns.index("is_saturated_dom")
        self.bad_dom_status_idx = columns.index("is_bad_dom")
        self.bright_dom_status_idx = columns.index("is_bright_dom")

        if PMTSummariser._SCHEMA is None:
            PMTSummariser._SCHEMA = self._build_schema()
        if PMTSummariser._DEFAULT_ARRAYS is None:
            PMTSummariser._DEFAULT_ARRAYS = self._build_empty_arrays()

    def __call__(self) -> pa.Table:
        return self._get_PMTfied_pa()

    def _get_PMTfied_pa(self) -> pa.Table:
        event_filter = ",".join(map(str, self.event_no_subset))
        query = f"""SELECT * 
                    FROM {self.pulsemap_table_name}
                    WHERE event_no IN ({event_filter})
                """
        cur_source = self.con_source.cursor()
        cur_source.execute(query)
        rows = cur_source.fetchall()

        events_doms_pulses = self._build_events_doms_pulses(rows)
        processed_data = []

        for (
            event_no,
            strings_doms_pulses,
        ) in events_doms_pulses.items():  # event level loop
            avg_dom_position = self._get_Q_weighted_DOM_position(
                strings_doms_pulses
            )  # one per event

            # Compute additional features if needed
            if (
                self.summary_mode == SummaryMode.GEOMETRIC
                or self.summary_mode == SummaryMode.EQUINOX
            ):
                eccentricity_PCA, aspect_contrast_PCA, hypotenuse = (
                    self._get_second_round_event_wise_features(
                        strings_doms_pulses, self.Q_adj_cut_second_round
                    )
                )

            for (
                string,
                doms_pulses,
            ) in strings_doms_pulses.items():  # string level loop
                for dom_no, pulses in doms_pulses.items():  # dom level loop
                    if (
                        self.summary_mode == SummaryMode.GEOMETRIC
                        or self.summary_mode == SummaryMode.EQUINOX
                    ):
                        dom_data = self._process_DOM(
                            pulses=pulses,
                            avg_dom_position=avg_dom_position,
                            eccentricity_PCA=eccentricity_PCA,
                            aspect_contrast_PCA=aspect_contrast_PCA,
                            hypotenuse=hypotenuse,
                        )
                    else:
                        dom_data = self._process_DOM(
                            pulses=pulses, avg_dom_position=avg_dom_position
                        )

                    processed_data.append(
                        np.hstack(([event_no], dom_data))
                    )  # Convert to NumPy row

        # Convert list of arrays into a single NumPy array (efficient bulk processing)
        processed_data = (
            np.vstack(processed_data)
            if processed_data
            else np.empty(
                (0, len(PMTSummariser._SCHEMA.names)), dtype=np.float32
            )
        )

        # Build PyArrow Table
        pa_arrays = {
            field: pa.array(processed_data[:, idx])
            for idx, field in enumerate(PMTSummariser._SCHEMA.names)
        }
        return pa.Table.from_pydict(pa_arrays, schema=PMTSummariser._SCHEMA)

    def _build_events_doms_pulses(
        self, rows: List[tuple]
    ) -> defaultdict[int, defaultdict[int, defaultdict[int, np.ndarray]]]:
        events_doms_pulses = defaultdict(
            lambda: defaultdict(lambda: defaultdict(list))
        )

        for row in rows:
            event_no = row[self.event_no_idx]
            string = row[self.dom_string_idx]
            dom_number = row[self.dom_number_idx]
            events_doms_pulses[event_no][string][dom_number].append(row)

        for event_no, string_dict in events_doms_pulses.items():
            for string, dom_dict in string_dict.items():
                for dom_no, pulse_list in dom_dict.items():
                    events_doms_pulses[event_no][string][dom_no] = np.array(
                        pulse_list, dtype=np.float32
                    )

        return events_doms_pulses

    def _get_Q_weighted_DOM_position(
        self, strings_doms_pulses: Dict[int, Dict[int, np.ndarray]]
    ) -> np.ndarray:
        total_weighted_position = np.zeros(3, dtype=np.float32)
        Q_event = 0.0
        for doms_pulses in strings_doms_pulses.values():
            for pulses in doms_pulses.values():
                q = pulses[:, self.dom_charge_idx]
                total_weighted_position += np.sum(
                    pulses[:, [self.dom_x_idx, self.dom_y_idx, self.dom_z_idx]]
                    * q[:, None],
                    axis=0,
                )
                Q_event += np.sum(q)
        return (
            total_weighted_position / Q_event
            if Q_event > 0
            else np.zeros(3, dtype=np.float32)
        )

    def _process_DOM(
        self,
        pulses: np.ndarray,
        avg_dom_position: np.ndarray,
        eccentricity_PCA: float = None,
        aspect_contrast_PCA: float = None,
        hypotenuse: float = None,
    ) -> np.ndarray:
        dom_position = self._get_DOM_position(pulses)
        rel_dom_pos = self._get_relative_DOM_position(
            dom_position, avg_dom_position
        )

        pmt_area = self._get_pmt_area(pulses)
        rde = self._get_rde(pulses)
        saturation_status = self._get_saturation_status(pulses)
        bad_dom_status = self._get_bad_dom_status(pulses)
        bright_dom_status = self._get_bright_dom_status(pulses)

        first_charge_readout = self._get_first_charge_readout(pulses)
        accumulated_charge_after_nc = self._get_accumulated_charge_after_ns(
            pulses
        )
        first_pulse_time = self._get_first_pulse_time(pulses)
        first_hlc = self._get_first_hlc(pulses)
        elapsed_time_until_charge_fraction = (
            self._get_elapsed_time_until_charge_fraction(pulses)
        )
        standard_deviation = self._get_time_standard_deviation(pulses)

        dom_data = np.hstack(
            (
                dom_position,
                rel_dom_pos,
                [
                    pmt_area,
                    rde,
                    saturation_status,
                    bad_dom_status,
                    bright_dom_status,
                ],
                first_charge_readout,
                accumulated_charge_after_nc,
                first_hlc,
                first_pulse_time,
                elapsed_time_until_charge_fraction,
                [standard_deviation],
            )
        ).astype(np.float32)

        # Append extra features depending on the summary mode
        if self.summary_mode == SummaryMode.GEOMETRIC:
            dom_data = np.hstack(
                (dom_data, [eccentricity_PCA, aspect_contrast_PCA, hypotenuse])
            )
        elif self.summary_mode == SummaryMode.EQUINOX:
            t_max_q = self._get_time_at_highest_charge_pulse(pulses)
            t_max_q_late_half = (
                self._get_time_at_highest_charge_pulse_in_the_second_half(
                    pulses
                )
            )
            Q_halftime = self._get_accumulated_charge_in_the_first_half(pulses)
            elapsed_time_until_charge_fraction_late = (
                self._get_elapsed_time_until_charge_fraction(
                    pulses, percentile1=70, percentile2=90
                )
            )
            dom_data = np.hstack(
                (
                    dom_data,
                    [t_max_q, t_max_q_late_half, Q_halftime],
                    elapsed_time_until_charge_fraction_late,
                    [eccentricity_PCA, aspect_contrast_PCA, hypotenuse],
                )
            )

        return dom_data

    def _get_relative_DOM_position(
        self, dom_position: np.ndarray, avg_dom_position: np.ndarray
    ) -> np.ndarray:
        rel_position = dom_position - avg_dom_position
        return rel_position

    def _get_DOM_position(self, pulses: np.ndarray) -> np.ndarray:
        return pulses[0, [self.dom_x_idx, self.dom_y_idx, self.dom_z_idx]]

    def _get_DOM_string(self, pulses: np.ndarray) -> int:
        return pulses[0, self.dom_string_idx]

    def _get_DOM_number(self, pulses: np.ndarray) -> int:
        return pulses[0, self.dom_number_idx]

    def _get_pmt_area(self, pulses: np.ndarray) -> float:
        return pulses[0, self.pmt_area_idx]

    def _get_rde(self, pulses: np.ndarray) -> float:
        return pulses[0, self.rde_idx]

    def _get_saturation_status(self, pulses: np.ndarray) -> int:
        return pulses[0, self.saturation_status_idx]

    def _get_bad_dom_status(self, pulses: np.ndarray) -> int:
        return pulses[0, self.bad_dom_status_idx]

    def _get_bright_dom_status(self, pulses: np.ndarray) -> int:
        return pulses[0, self.bright_dom_status_idx]

    def _get_first_hlc(self, pulses: np.ndarray) -> np.ndarray:
        _fillIncomplete = -1
        if pulses.shape[0] < self.n_pulse_collect:
            hlc = np.pad(
                pulses[:, self.dom_hlc_idx],
                (0, self.n_pulse_collect - pulses.shape[0]),
                constant_values=_fillIncomplete,
            )
        else:
            hlc = pulses[: self.n_pulse_collect, self.dom_hlc_idx]
        return hlc.astype(np.int32)

    def _get_first_charge_readout(self, pulses: np.ndarray) -> np.ndarray:
        _fillIncomplete = -1
        if pulses.shape[0] < self.n_pulse_collect:
            charge_readouts = np.pad(
                pulses[:, self.dom_charge_idx],
                (0, self.n_pulse_collect - pulses.shape[0]),
                constant_values=_fillIncomplete,
            )
        else:
            charge_readouts = pulses[
                : self.n_pulse_collect, self.dom_charge_idx
            ]

        return charge_readouts.astype(np.float32)

    def _get_accumulated_charge_after_ns(
        self, pulses: np.ndarray, interval1=25, interval2=75
    ) -> np.ndarray:
        _fillIncomplete = -1
        if pulses.shape[0] < 1:
            return np.full(3, _fillIncomplete, dtype=np.float32)

        t_0 = pulses[0, self.dom_time_idx]
        time_offsets = pulses[:, self.dom_time_idx] - t_0
        charges = pulses[:, self.dom_charge_idx]

        Qinterval1 = np.sum(charges[time_offsets < interval1])
        Qinterval2 = np.sum(charges[time_offsets < interval2])
        Qtotal = np.sum(charges)

        return np.array([Qinterval1, Qinterval2, Qtotal], dtype=np.float32)

    def _get_first_pulse_time(self, pulses: np.ndarray) -> np.ndarray:
        _fillIncomplete = -1
        if pulses.shape[0] < self.n_pulse_collect:
            pulse_times = np.pad(
                pulses[:, self.dom_time_idx],
                (0, self.n_pulse_collect - pulses.shape[0]),
                constant_values=_fillIncomplete,
            )
        else:
            pulse_times = pulses[: self.n_pulse_collect, self.dom_time_idx]
        return pulse_times.astype(np.float32)

    def _get_elapsed_time_until_charge_fraction(
        self, pulses: np.ndarray, percentile1=10, percentile2=50
    ) -> np.ndarray:
        _fillIncomplete = -1
        if pulses.shape[0] < 2:
            return np.full(2, _fillIncomplete, dtype=np.float32)

        charges = pulses[:, self.dom_charge_idx]
        times = pulses[:, self.dom_time_idx]
        Qtotal = np.sum(charges)
        cumulated_charge = np.cumsum(charges)

        # Use np.searchsorted() to avoid index errors
        idx1 = np.searchsorted(
            cumulated_charge, percentile1 / 100 * Qtotal, side="right"
        )
        idx2 = np.searchsorted(
            cumulated_charge, percentile2 / 100 * Qtotal, side="right"
        )

        T_1 = (
            times[idx1] - times[0]
            if idx1 < pulses.shape[0]
            else _fillIncomplete
        )
        T_2 = (
            times[idx2] - times[0]
            if idx2 < pulses.shape[0]
            else _fillIncomplete
        )

        return np.array([T_1, T_2], dtype=np.float32)

    def _get_time_standard_deviation(self, pulses: np.ndarray) -> float:
        _fillIncomplete = -1
        return (
            np.std(pulses[:, self.dom_time_idx])
            if pulses.shape[0] > 1
            else _fillIncomplete
        )

    # ---------the equinox additional features----------
    def _get_time_at_highest_charge_pulse(self, pulses: np.ndarray) -> float:
        _fillIncomplete = -1
        if pulses.shape[0] < 1:
            t_qmax = _fillIncomplete
        else:
            t_qmax = pulses[
                np.argmax(pulses[:, self.dom_charge_idx]), self.dom_time_idx
            ]
        return t_qmax

    def _get_time_at_highest_charge_pulse_in_the_second_half(
        self, pulses: np.ndarray
    ) -> float:
        _fillIncomplete = -1
        if pulses.shape[0] < 6:
            t_qmax_secondhalf = _fillIncomplete
        else:
            t_middle = (
                pulses[-1, self.dom_time_idx] + pulses[0, self.dom_time_idx]
            ) / 2
            second_half_mask = pulses[:, self.dom_time_idx] > t_middle
            t_qmax_secondhalf = pulses[
                np.argmax(pulses[:, self.dom_charge_idx][second_half_mask]),
                self.dom_time_idx,
            ]
        return t_qmax_secondhalf

    def _get_accumulated_charge_in_the_first_half(
        self, pulses: np.ndarray
    ) -> float:
        _fillIncomplete = -1
        if pulses.shape[0] < 2:
            Q_halftime = _fillIncomplete
        else:
            t_middle = (
                pulses[-1, self.dom_time_idx] + pulses[0, self.dom_time_idx]
            ) / 2
            first_half_mask = pulses[:, self.dom_time_idx] < t_middle
            Q_halftime = np.sum(
                pulses[:, self.dom_charge_idx][first_half_mask]
            )
        return Q_halftime

    def _build_schema(self) -> pa.Schema:
        base_schema = [
            ("event_no", pa.int32()),
            ("dom_x", pa.float32()),
            ("dom_y", pa.float32()),
            ("dom_z", pa.float32()),
            ("dom_x_rel", pa.float32()),
            ("dom_y_rel", pa.float32()),
            ("dom_z_rel", pa.float32()),
            ("pmt_area", pa.float32()),
            ("rde", pa.float32()),
            ("saturation_status", pa.int32()),
            ("bad_dom_status", pa.int32()),
            ("bright_dom_status", pa.int32()),
        ]
        # q1, q2, q3 (,q4, q5, ...)
        charge_columns = [
            (f"q{i+1}", pa.float32()) for i in range(self.n_pulse_collect)
        ]

        accumulated_charge_columns = [
            ("Q25", pa.float32()),
            ("Q75", pa.float32()),
            ("Qtotal", pa.float32()),
        ]

        hlc_columns = [
            (f"hlc{i+1}", pa.int32()) for i in range(self.n_pulse_collect)
        ]
        # t1, t2, t3 (,t4, t5, ...)
        time_columns = [
            (f"t{i+1}", pa.float32()) for i in range(self.n_pulse_collect)
        ]

        accumulated_time_columns = [
            ("T10", pa.float32()),
            ("T50", pa.float32()),
            ("sigmaT", pa.float32()),
        ]
        schema_fields = (
            base_schema
            + charge_columns
            + accumulated_charge_columns
            + hlc_columns
            + time_columns
            + accumulated_time_columns
        )

        if self.summary_mode == SummaryMode.GEOMETRIC:
            schema_fields += [
                ("eccentricity_PCA", pa.float32()),
                ("aspect_contrast_PCA", pa.float32()),
                ("hypotenuse", pa.float32()),
            ]
        elif self.summary_mode == SummaryMode.EQUINOX:
            schema_fields += [
                ("t_qmax", pa.float32()),
                ("t_qmax_secondhalf", pa.float32()),
                ("Q_halftime", pa.float32()),
                ("T70", pa.float32()),
                ("T90", pa.float32()),
                ("eccentricity_PCA", pa.float32()),
                ("aspect_contrast_PCA", pa.float32()),
                ("hypotenuse", pa.float32()),
            ]

        return pa.schema(schema_fields)

    @classmethod
    def _build_empty_arrays(cls) -> Dict[str, List[float]]:
        if cls._SCHEMA is None:
            raise ValueError(
                "Schema must be defined before building empty arrays."
            )
        return {field: [] for field in cls._SCHEMA.names}

    ## ----------------- Second round features ----------------- ##
    def _get_second_round_event_wise_features(
        self,
        event_doms_pulses: Dict[int, Dict[int, np.ndarray]],
        Q_adj_cut_second_round: float,
    ) -> Tuple[float, float, float]:
        Q_threshold = 10 ** (Q_adj_cut_second_round + 2)
        collected_data = []

        for doms_pulses in event_doms_pulses.values():
            for pulses in doms_pulses.values():
                Q_tot_dom = np.sum(pulses[:, self.dom_charge_idx])
                if Q_tot_dom >= Q_threshold:
                    collected_data.append(
                        pulses[
                            0,
                            [
                                self.dom_string_idx,
                                self.dom_x_idx,
                                self.dom_y_idx,
                                self.dom_z_idx,
                            ],
                        ]
                    )
        if not collected_data:
            eccentricity_PCA, aspect_contrast_PCA, hypotenuse = -1, -1, -1
        else:
            collected_data = np.array(collected_data, dtype=np.float32)

            xy_boundary = self._get_XY_boundary(collected_data)
            max_Z_stretch = self._get_max_Z_stretch(collected_data)
            major_PCA, minor_PCA = self._get_PCA(
                xy_boundary
            )  # ✅ Returns tuple
            eccentricity_PCA = self._get_eccentricity(major_PCA, minor_PCA)
            aspect_contrast_PCA = self._get_aspect_contrast(
                major_PCA, minor_PCA
            )
            xy_extent = self._get_max_xy_extent(xy_boundary)
            hypotenuse = self._get_hypotenuse(xy_extent, max_Z_stretch)

        return eccentricity_PCA, aspect_contrast_PCA, hypotenuse

    def _get_XY_boundary(self, high_charge_doms: np.ndarray) -> np.ndarray:
        if high_charge_doms.shape[0] == 0:
            return np.empty((0, 2), dtype=np.float32)

        unique_strings, unique_indices = np.unique(
            high_charge_doms[:, 0], return_index=True
        )
        xy_points = high_charge_doms[unique_indices, 1:3]

        if xy_points.shape[0] >= 3:
            try:
                hull = ConvexHull(xy_points)
                xy_points = xy_points[hull.vertices]
            except Exception:
                pass
        return xy_points

    def _get_max_xy_extent(self, xy_boundary: np.ndarray) -> float:
        max_extent = -1
        if xy_boundary.shape[0] >= 2:
            try:
                distances = cdist(xy_boundary, xy_boundary, metric="euclidean")
                max_extent = np.max(distances)
            except Exception:
                pass
        return max_extent

    def _get_PCA(self, xy_boundary: np.ndarray) -> Tuple[float, float]:
        major_axis_length, minor_axis_length = -1, -1

        if xy_boundary.shape[0] >= 2 and not np.all(
            xy_boundary == xy_boundary[0]
        ):
            try:
                pca = PCA(n_components=2)
                pca.fit(xy_boundary)
                eigenvalues = pca.explained_variance_
                major_axis_length, minor_axis_length = np.sqrt(eigenvalues)
            except Exception:
                pass

        return major_axis_length, minor_axis_length

    def _get_eccentricity(
        self, major_axis_length: float, minor_axis_length: float
    ) -> float:
        eccentricity = -1
        if major_axis_length > 0 and minor_axis_length > 0:
            eccentricity = np.sqrt(
                1 - (minor_axis_length / major_axis_length) ** 2
            )
        return eccentricity

    def _get_aspect_contrast(
        self, major_axis_length: float, minor_axis_length: float
    ) -> float:
        aspect_contrast = -1
        if major_axis_length > 0 and minor_axis_length > 0:
            aspect_contrast = (major_axis_length - minor_axis_length) / (
                major_axis_length + minor_axis_length
            )
        return aspect_contrast

    def _get_hypotenuse(self, xy_extent: float, z_stretch: float) -> float:
        hypotenuse = -1
        if xy_extent > 0 and z_stretch > 0:
            hypotenuse = np.sqrt(xy_extent**2 + z_stretch**2)
        elif xy_extent > 0:  #
            hypotenuse = xy_extent
        elif z_stretch > 0:
            hypotenuse = z_stretch
        return hypotenuse

    def _get_max_Z_stretch(self, high_charge_doms: np.ndarray) -> float:
        max_z_stretch = -1
        if high_charge_doms.shape[0] > 0:
            unique_strings = np.unique(high_charge_doms[:, 0])
            for string in unique_strings:
                z_values = high_charge_doms[
                    high_charge_doms[:, 0] == string, 3
                ]
                max_z_stretch = max(
                    max_z_stretch, np.max(z_values) - np.min(z_values)
                )
        return max_z_stretch
