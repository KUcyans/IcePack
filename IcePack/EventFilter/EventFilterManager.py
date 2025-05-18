import os
import json
import time
import logging
from IcePack.EventFilter.EventFilter import EventFilter
from IcePack.EventFilter.CompositeEventFilter import CompositeEventFilter

"""
@author: cyan.jo
Summary:
Manages configuration and execution of multiple event filters

(1)Initialises filters based on user-defined parameters or configuration files
(2)Applies the assembled filter chain to event tables
(3)Serves as the high-level interface for filtering IceCube event datasets
"""


class EventFilterManager:
    def __init__(
        self,
        source_dir: str,
        output_dir: str,
        subdir_no: int,
        part_no: int,
        filter_classes: dict,
        filter_kwargs: dict = None,
    ):
        self.source_dir = source_dir
        self.output_dir = output_dir
        self.subdir_no = subdir_no
        self.part_no = part_no
        self.logger = logging.getLogger(self.__class__.__name__)
        self.filters = filter_classes
        self.filter_kwargs = filter_kwargs or {}
        self.filter_keyword = self._build_filter_keyword()
        self._make_subdir()
        self.filter_instances = (
            self._instantiate_filters()
        )  # dict[str, EventFilter]
        self.filter_obj = self._build_Filter_object()

    def __call__(self):
        self.logger.info(
            f"Starting filtering process for {self.subdir_no}/{self.part_no}"
        )
        self.filter_obj()
        self.logger.info("Filtering process completed.")

    def _build_filter_keyword(self) -> str:
        names = []
        for alias in sorted(self.filters.keys()):
            suffix = ""
            if alias in self.filter_kwargs:
                values = list(self.filter_kwargs[alias].values())
                if values:
                    suffix = "_" + "_".join(str(v) for v in values)
            names.append(f"{alias}{suffix}")
        return "_".join(names)

    def _make_subdir(self):
        self.source_subdir = os.path.join(self.source_dir, str(self.subdir_no))
        self.output_subdir = os.path.join(
            self.output_dir, self.filter_keyword, str(self.subdir_no)
        )
        os.makedirs(self.output_subdir, exist_ok=True)

    def _build_Filter_object(self) -> CompositeEventFilter:
        if not self.filters:
            self.logger.error(
                "No filters provided. Returning CompositeEventFilter with empty events."
            )
            filter_object = CompositeEventFilter(
                source_subdir=self.source_subdir,
                output_subdir=self.output_subdir,
                subdir_no=self.subdir_no,
                part_no=self.part_no,
                valid_event_nos=set(),
                filter_keyword="None",
            )
        if len(self.filters) == 1:
            single_filter = next(iter(self.filter_instances.values()))
            filter_object = self.build_CompositeEventFilter(single_filter)

        elif len(self.filters) > 1:
            valid_event_nos = self._synthesize_valid_event_nos()
            filter_object = CompositeEventFilter(
                source_subdir=self.source_subdir,
                output_subdir=self.output_subdir,
                subdir_no=self.subdir_no,
                part_no=self.part_no,
                valid_event_nos=valid_event_nos,
                filter_keyword=self.filter_keyword,
            )
        return filter_object

    # builds individual filter instances
    def _instantiate_filters(self) -> dict[str, EventFilter]:
        filter_instances = {}
        for alias, filter_class in self.filters.items():
            filter_instances[alias] = filter_class(
                source_subdir=self.source_subdir,
                output_subdir=self.output_subdir,
                subdir_no=self.subdir_no,
                part_no=self.part_no,
                **self.filter_kwargs.get(alias, {}),
            )
        return filter_instances

    def _synthesize_valid_event_nos(self) -> set:
        event_sets = {
            alias: f.get_valid_event_nos()
            for alias, f in self.filter_instances.items()
        }
        return set.intersection(*event_sets.values())

    def build_CompositeEventFilter(
        self, eventFilter: EventFilter
    ) -> CompositeEventFilter:
        valid_event_nos = eventFilter.get_valid_event_nos()
        return CompositeEventFilter(
            source_subdir=self.source_subdir,
            output_subdir=self.output_subdir,
            subdir_no=self.subdir_no,
            part_no=self.part_no,
            valid_event_nos=valid_event_nos,
            filter_keyword=self.filter_keyword,
        )

    def generate_receipt(self, start_time: float, end_time: float) -> None:
        """Generates a unified receipt for all applied filters."""
        receipt_file = os.path.join(
            self.output_subdir,
            f"[Receipt]{self.subdir_no}_{self.part_no}.json",
        )

        receipt_data = {
            "subdir_no": self.subdir_no,
            "part_no": self.part_no,
            "filter_applied": self.filter_keyword,
            "reduction_results": self.filter_obj.get_receipt_info(),
            "start_time": time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(start_time)
            ),
            "end_time": time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(end_time)
            ),
            "execution_duration": round(end_time - start_time, 4),
        }

        with open(receipt_file, "w") as f:
            json.dump(receipt_data, f, indent=4)

        self.logger.info(f"Unified receipt generated: {receipt_file}")
