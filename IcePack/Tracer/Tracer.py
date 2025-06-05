import abc
import pandas as pd


class Tracer(abc.ABC):
    def __init__(self, source_root: str):
        self.source_root = source_root

    def __call__(self, event_no: int):
        """
        callable returns a dataframe of the event of that event_no
        """
        return self.event_tracer(event_no)

    @staticmethod
    def disintegrate_enhanced_event_no(event_no: int) -> dict:
        event_str = str(event_no).zfill(
            15
        )  # pad if necessary to match full structure

        signal_code = event_str[0]
        subdir_tag = int(event_str[1:3])
        part_no = int(event_str[3:7])
        original_event_no = int(event_str[7:])

        if signal_code == "1":
            signal_type = "Snowstorm"
        elif signal_code == "2":
            signal_type = "Corsika"
        else:
            signal_type = "Unknown"

        # 117000303017984
        # /lustre/hpc/project/icecube/HE_Nu_Aske_Oct2024/sqlite_pulses/Snowstorm/22017/merged_part_3.db
        disintegrated_event_receipt = {
            "signal_type": signal_type,
            "subdir_tag": subdir_tag,
            "part_no": part_no,
            "original_event_no": original_event_no,
        }
        return disintegrated_event_receipt

    @abc.abstractmethod
    def event_tracer(self, event_no: int) -> pd.DataFrame:
        """
        Given an enhanced event number, trace back the file and return the corresponding
        event's information as a pandas DataFrame.
        """
        pass
