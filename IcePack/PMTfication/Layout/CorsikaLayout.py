from typing import ClassVar, List, Tuple
from IcePack.PMTfication.Layout.SourceLayout import SourceLayout


class CorsikaLayout(SourceLayout):
    family: str = "Corsika"

    # list of (alias_int, subdir) pairs
    subdirs: ClassVar[List[Tuple[int, str]]] = [
        (2, "0002000-0002999"),
        (3, "0003000-0003999"),
        (4, "0004000-0004999"),
        (5, "0005000-0005999"),
        (6, "0006000-0006999"),
        (7, "0007000-0007999"),
        (8, "0008000-0008999"),
        (9, "0009000-0009999"),
    ]

    alias: int  # instance attribute
    subdir: str  # instance attribute

    def get_N_events_per_shard(self) -> int:
        return 20_000

    @classmethod
    def from_alias_and_subdir(cls, alias: int, subdir: str) -> "CorsikaLayout":
        if (alias, subdir) not in cls.subdirs:
            raise ValueError(
                f"Unknown alias-subdir pair ({alias}, '{subdir}') for CorsikaLayout"
            )
        return cls(alias=alias, subdir=subdir)

    @classmethod
    def from_alias(cls, alias: int) -> "CorsikaLayout":
        for a, s in cls.subdirs:
            if a == alias:
                return cls(alias=a, subdir=s)
        raise ValueError(f"No subdir found for alias {alias}")


# create all instances
CorsikaLayout.layouts = [
    CorsikaLayout.from_alias(a) for a, _ in CorsikaLayout.subdirs
]
