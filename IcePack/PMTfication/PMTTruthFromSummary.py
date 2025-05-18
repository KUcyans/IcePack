import pyarrow as pa
import pyarrow.compute as pc
from scipy.spatial.distance import pdist
import numpy as np


class PMTTruthFromSummary:
    """
    Extract event-wise features (e.g., max inter-PMT distance) from a PMTfied PyArrow Table.
    callable() returns a PyArrow Table with 'event_no' and computed features.
    To add new features,
    1. Add a new private function to compute the feature.
    2. Add the feature to the _build_truth_sub_pa() function.
    """

    def __init__(self, pa_pmtfied_shard: pa.Table) -> None:
        self.pa_pmtfied_shard = pa_pmtfied_shard

    def __call__(self) -> pa.Table:
        return self._build_truth_sub_pa()

    def _build_truth_sub_pa(self) -> pa.Table:
        unique_events = pc.unique(
            self.pa_pmtfied_shard.column("original_event_no")
        ).to_numpy()
        max_distances = [
            self._get_max_interPMT_distance(event) for event in unique_events
        ]
        # add new features here

        truth_table = pa.Table.from_arrays(
            [
                pa.array(unique_events, type=pa.int32()),
                pa.array(max_distances, type=pa.float32()),
            ],  # add new features here
            names=[
                "event_no",
                "max_interPMT_distance",
            ],  # add new features here
        )

        return truth_table

    def _get_max_interPMT_distance(self, event_no: int) -> float:
        """
        Compute the maximum distance among (dom_x, dom_y, dom_z) for a given event.
        """
        event_mask = pc.equal(
            self.pa_pmtfied_shard.column("original_event_no"), event_no
        )
        event_table = self.pa_pmtfied_shard.filter(event_mask)

        # Extract (dom_x, dom_y, dom_z) coordinates
        xyz = np.column_stack(
            [
                event_table.column("dom_x").to_numpy(),
                event_table.column("dom_y").to_numpy(),
                event_table.column("dom_z").to_numpy(),
            ]
        )

        if xyz.shape[0] < 2:  # If only one DOM position exists, distance is 0
            return 0.0

        return np.max(
            pdist(xyz, metric="euclidean")
        )  # max of  pairwise distances
