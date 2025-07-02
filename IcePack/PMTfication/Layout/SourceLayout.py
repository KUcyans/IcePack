from pydantic import BaseModel
from typing import Optional
from IcePack.Enum.EnergyRange import EnergyRange
from IcePack.Enum.Flavour import Flavour


class SourceLayout(BaseModel):
    family: str

    def get_db_file_name(self, file_idx: int) -> str:
        return f"merged_part_{int(file_idx)}.db"

    def extract_part_no(self, filename: str) -> int:
        try:
            return int(filename.split("_")[-1].split(".")[0])
        except (IndexError, ValueError):
            raise ValueError(f"Invalid file name format: {filename}")

    def get_N_events_per_shard(self) -> int:
        raise NotImplementedError(
            "Subclasses must implement get_N_events_per_shard()"
        )


class FlavouredLayoutInfo(BaseModel):
    subdir: str
    flavour: Optional[Flavour] = None
    energy_range: Optional[EnergyRange] = None


class FlavouredSourceLayout(SourceLayout):
    layout_info: FlavouredLayoutInfo

    @property
    def subdir(self) -> str:
        return self.layout_info.subdir

    @property
    def flavour(self) -> Optional[Flavour]:
        return self.layout_info.flavour

    @property
    def energy_range(self) -> Optional[EnergyRange]:
        return self.layout_info.energy_range

    @classmethod
    def from_flavour_energy(
        cls, flavour: Flavour, energy_range: EnergyRange
    ) -> Optional["FlavouredSourceLayout"]:
        raise NotImplementedError("Implement this in subclass")
