from enum import Enum

"""
@author: cyan.jo
Summary:
Defines the flavour of neutrinos in IceCube data processing, including electron, muon, and tau flavours.

Usage:
    flavour = Flavour.E
    flavour.id  # Returns the ID of the flavour
    flavour.alias  # Returns the alias of the flavour
    flavour.latex  # Returns the LaTeX representation of the flavour
    flavour.pdg  # Returns the PDG code of the flavour
"""


class Flavour(Enum):
    E = (0, "nu_e", r"$\nu_e$", 12)
    MU = (1, "nu_mu", r"$\nu_\mu$", 14)
    TAU = (2, "nu_tau", r"$\nu_\tau$", 16)

    def __init__(self, id: int, alias: str, latex: str, pdg: int):
        self.id = id
        self.alias = alias
        self.latex = latex
        self.pdg = pdg
