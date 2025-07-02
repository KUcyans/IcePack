from IcePack.PMTfication.Layout.SourceLayout import (
    FlavouredSourceLayout,
    FlavouredLayoutInfo,
)
from IcePack.Enum.Flavour import Flavour
from IcePack.Enum.EnergyRange import EnergyRange
from typing import ClassVar, List


class SnowstormLayout(FlavouredSourceLayout):
    family: str = "Snowstorm"
    layout_info: FlavouredLayoutInfo  # must be this, not layout_data

    layouts: ClassVar[List["SnowstormLayout"]] = []

    @property
    def subdir(self) -> str:
        return self.layout_info.subdir

    @property
    def flavour(self) -> Flavour:
        return self.layout_info.flavour

    @property
    def energy_range(self) -> EnergyRange:
        return self.layout_info.energy_range

    def get_N_events_per_shard(self) -> int:
        er = self.energy_range
        if er == EnergyRange.ER_100_GEV_10_TEV:
            return 200_000
        elif er == EnergyRange.ER_10_TEV_1_PEV:
            return 20_000
        elif er == EnergyRange.ER_1_PEV_100_PEV:
            return 2_000
        else:
            raise ValueError(f"Unknown energy range: {er}")

    @classmethod
    def from_flavour_energy(cls, flavour: Flavour, energy_range: EnergyRange):
        for layout in cls.layouts:
            if (
                layout.flavour == flavour
                and layout.energy_range == energy_range
            ):
                return layout
        return None


# Now create layouts using FlavouredLayoutInfo instances:
SnowstormLayout.layouts = [
    SnowstormLayout(
        layout_info=FlavouredLayoutInfo(
            subdir="22013",
            flavour=Flavour.E,
            energy_range=EnergyRange.ER_100_GEV_10_TEV,
        )
    ),
    SnowstormLayout(
        layout_info=FlavouredLayoutInfo(
            subdir="22010",
            flavour=Flavour.MU,
            energy_range=EnergyRange.ER_100_GEV_10_TEV,
        )
    ),
    SnowstormLayout(
        layout_info=FlavouredLayoutInfo(
            subdir="22016",
            flavour=Flavour.TAU,
            energy_range=EnergyRange.ER_100_GEV_10_TEV,
        )
    ),
    SnowstormLayout(
        layout_info=FlavouredLayoutInfo(
            subdir="22014",
            flavour=Flavour.E,
            energy_range=EnergyRange.ER_10_TEV_1_PEV,
        )
    ),
    SnowstormLayout(
        layout_info=FlavouredLayoutInfo(
            subdir="22011",
            flavour=Flavour.MU,
            energy_range=EnergyRange.ER_10_TEV_1_PEV,
        )
    ),
    SnowstormLayout(
        layout_info=FlavouredLayoutInfo(
            subdir="22017",
            flavour=Flavour.TAU,
            energy_range=EnergyRange.ER_10_TEV_1_PEV,
        )
    ),
    SnowstormLayout(
        layout_info=FlavouredLayoutInfo(
            subdir="22015",
            flavour=Flavour.E,
            energy_range=EnergyRange.ER_1_PEV_100_PEV,
        )
    ),
    SnowstormLayout(
        layout_info=FlavouredLayoutInfo(
            subdir="22012",
            flavour=Flavour.MU,
            energy_range=EnergyRange.ER_1_PEV_100_PEV,
        )
    ),
    SnowstormLayout(
        layout_info=FlavouredLayoutInfo(
            subdir="22018",
            flavour=Flavour.TAU,
            energy_range=EnergyRange.ER_1_PEV_100_PEV,
        )
    ),
]
