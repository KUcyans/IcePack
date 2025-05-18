from IcePack.EventFilter.EventFilter import EventFilter

"""
@author: cyan.jo
Summary:
Chains multiple event filters together as a composite

(1)Accepts a list of filters and applies them sequentially to the same event
(2)Only passes events that satisfy all individual filter conditions
(3)Provides a flexible mechanism for building complex filter pipelines
"""


class CompositeEventFilter(EventFilter):
    def __init__(
        self,
        source_subdir,
        output_subdir,
        subdir_no,
        part_no,
        valid_event_nos,
        filter_keyword: str,
    ):
        self.filter_keyword = filter_keyword
        super().__init__(source_subdir, output_subdir, subdir_no, part_no)
        self.valid_event_nos = valid_event_nos

        self.logger.info(f"Initialized ({self.filter_keyword})")

    def _set_valid_event_nos(self):
        pass
