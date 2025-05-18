import pyarrow as pa

"""
@autor: cyan.jo
Summary:
Combines PMT signal data with its corresponding ground truth label.

(1)Pairs each processed PMTfied shard with its label metadata.
(2)Prepares the dataset for supervised learning.
(3)Supports structured dataset creation across events and shards.
"""


class PMTTruthEntity:
    TRUTH = {
        "event_no": (pa.int32(), None),
        "subdirectory_no": (pa.int32(), None),
        "part_no": (pa.int32(), None),
        "shard_no": (pa.int32(), None),
        "N_doms": (pa.int32(), None),
        "offset": (pa.int32(), None),
        "energy": (pa.float32(), None),
        "azimuth": (pa.float32(), None),
        "zenith": (pa.float32(), None),
        "pid": (pa.int32(), None),
        "position_x": (pa.float32(), None),
        "position_y": (pa.float32(), None),
        "position_z": (pa.float32(), None),
        "event_time": (pa.float32(), None),
        "interaction_type": (pa.int32(), None),
        "elasticity": (pa.float32(), None),
        "RunID": (pa.int64(), None),
        "SubrunID": (pa.int64(), None),
        "EventID": (pa.int32(), None),
        "SubEventID": (pa.int32(), None),
        "dbang_decay_length": (pa.float32(), None),
        "track_length": (pa.float32(), None),
        "stopped_muon": (pa.int32(), None),
        "energy_track": (pa.float32(), None),
        "energy_cascade": (pa.float32(), None),
        "inelasticity": (pa.float32(), None),
        "DeepCoreFilter_13": (pa.int32(), None),
        "CascadeFilter_13": (pa.int32(), None),
        "MuonFilter_13": (pa.int32(), None),
        "OnlineL2Filter_17": (pa.int32(), None),
        "L3_oscNext_bool": (pa.int32(), None),
        "L4_oscNext_bool": (pa.int32(), None),
        "L5_oscNext_bool": (pa.int32(), None),
        "L6_oscNext_bool": (pa.int32(), None),
        "L7_oscNext_bool": (pa.int32(), None),
        "Homogenized_QTot": (pa.float32(), None),
        "MCLabelClassification": (pa.int32(), None),
        "MCLabelCoincidentMuons": (pa.int32(), None),
        "MCLabelBgMuonMCPE": (pa.int32(), None),
        "MCLabelBgMuonMCPECharge": (pa.int32(), None),
    }

    GNLabel = {
        "GNLabel_event_no": (pa.int32(), None),
        "GNLabelTrackEnergyDeposited": (pa.float32(), -1),
        "GNLabelTrackEnergyOnEntrance": (pa.float32(), -1),
        "GNLabelTrackEnergyOnEntrancePrimary": (pa.float32(), -1),
        "GNLabelTrackEnergyDepositedPrimary": (pa.float32(), -1),
        "GNLabelEnergyPrimary": (pa.float32(), -1),
        "GNLabelCascadeEnergyDepositedPrimary": (pa.float32(), -1),
        "GNLabelCascadeEnergyDeposited": (pa.float32(), -1),
        "GNLabelEnergyDepositedTotal": (pa.float32(), -1),
        "GNLabelEnergyDepositedPrimary": (pa.float32(), -1),
        "GNLabelHighestEInIceParticleIsChild": (pa.int32(), -1),
        "GNLabelHighestEInIceParticleDistance": (pa.float32(), -1e8),
        "GNLabelHighestEInIceParticleEFraction": (pa.float32(), -1),
        "GNLabelHighestEDaughterDistance": (pa.float32(), -1e8),
        "GNLabelHighestEDaughterEFraction": (pa.float32(), -1),
    }

    HighestEInIceParticle = {
        "HighestEInIceParticle_event_no": (pa.int32(), None),
        "zenith_GNHighestEInIceParticle": (pa.float32(), -1),
        "azimuth_GNHighestEInIceParticle": (pa.float32(), -1),
        "dir_x_GNHighestEInIceParticle": (pa.float32(), 0),
        "dir_y_GNHighestEInIceParticle": (pa.float32(), 0),
        "dir_z_GNHighestEInIceParticle": (pa.float32(), 0),
        "pos_x_GNHighestEInIceParticle": (pa.float32(), -1e8),
        "pos_y_GNHighestEInIceParticle": (pa.float32(), -1e8),
        "pos_z_GNHighestEInIceParticle": (pa.float32(), -1e8),
        "time_GNHighestEInIceParticle": (pa.float32(), -1),
        "speed_GNHighestEInIceParticle": (pa.float32(), -1),
        "energy_GNHighestEInIceParticle": (pa.float32(), -1),
    }

    HE_DAUGHTER = {
        "HE_daughter_event_no": (pa.int32(), None),
        "zenith_GNHighestEDaughter": (pa.float32(), -1),
        "azimuth_GNHighestEDaughter": (pa.float32(), -1),
        "dir_x_GNHighestEDaughter": (pa.float32(), 0),
        "dir_y_GNHighestEDaughter": (pa.float32(), 0),
        "dir_z_GNHighestEDaughter": (pa.float32(), 0),
        "pos_x_GNHighestEDaughter": (pa.float32(), -1e8),
        "pos_y_GNHighestEDaughter": (pa.float32(), -1e8),
        "pos_z_GNHighestEDaughter": (pa.float32(), -1e8),
        "time_GNHighestEDaughter": (pa.float32(), -1),
        "speed_GNHighestEDaughter": (pa.float32(), -1),
        "energy_GNHighestEDaughter": (pa.float32(), -1),
    }

    MCWeightDict = {
        "MCWeightDict_event_no": (pa.int32(), None),
        "BjorkenX": (pa.float32(), -1e8),
        "BjorkenY": (pa.float32(), -1e8),
        "CylinderHeight": (pa.float32(), -1),
        "CylinderRadius": (pa.float32(), -1),
        "DirectionWeight": (pa.float32(), -1),
        "EnergyLost": (pa.float32(), -1),
        "ImpactParam": (pa.float32(), -1),
        "InIceNeutrinoEnergy": (pa.float32(), -1),
        "InIceNeutrinoType": (pa.int32(), -1),
        "InjectionAreaCGS": (pa.float32(), -1),
        "InjectionCylinderHeight": (pa.float32(), -1),
        "InjectionCylinderRadius": (pa.float32(), -1),
        "InjectionOrigin_x": (pa.float32(), -1),
        "InjectionOrigin_y": (pa.float32(), -1),
        "InjectionOrigin_z": (pa.float32(), -1),
        "InteractionColumnDepthCGS": (pa.float32(), -1),
        "InteractionPositionWeight": (pa.float32(), -1),
        "InteractionType": (pa.int32(), -1),
        "InteractionTypeWeight": (pa.float32(), -1),
        "InteractionWeight": (pa.float32(), -1),
        "InteractionXsectionCGS": (pa.float32(), -1),
        "LengthInVolume": (pa.float32(), -1),
        "MaxAzimuth": (pa.float32(), -1),
        "MaxEnergyLog": (pa.float32(), -1),
        "MaxZenith": (pa.float32(), -1),
        "MinAzimuth": (pa.float32(), -1),
        "MinEnergyLog": (pa.float32(), -1),
        "MinZenith": (pa.float32(), -1),
        "NEvents": (pa.float32(), -1),
        "NInIceNus": (pa.float32(), -1),
        "OneWeight": (pa.float32(), -1),
        "OneWeightPerType": (pa.float32(), -1),
        "PowerLawIndex": (pa.float32(), -1),
        "PrimaryNeutrinoAzimuth": (pa.float32(), -1),
        "PrimaryNeutrinoEnergy": (pa.float32(), -1),
        "PrimaryNeutrinoType": (pa.int32(), -1),
        "PrimaryNeutrinoZenith": (pa.float32(), -1),
        "PropagationWeight": (pa.float32(), -1),
        "RangeInMWE": (pa.float32(), -1),
        "RangeInMeter": (pa.float32(), -1),
        "SelectionWeight": (pa.float32(), -1),
        "SimMode": (pa.int32(), -1),
        "SolidAngle": (pa.float32(), -1),
        "TotalColumnDepthCGS": (pa.float32(), -1),
        "TotalPrimaryWeight": (pa.float32(), -1),
        "TotalWeight": (pa.float32(), -1),
        "TotalXsectionCGS": (pa.float32(), -1),
        "TrueActiveLengthAfter": (pa.float32(), -1),
        "TrueActiveLengthBefore": (pa.float32(), -1),
        "TypeWeight": (pa.float32(), -1),
    }
    # the length of the MCWeightDict is 52 # copilot said that

    SCHEMAS = {
        "TRUTH": TRUTH,
        "GNLabel": GNLabel,
        "HighestEInIceParticle": HighestEInIceParticle,
        "HE_DAUGHTER": HE_DAUGHTER,
        "MCWeightDict": MCWeightDict,
    }

    TRUTH_EXCLUDED_COLUMNS = {
        "TRUTH": {"subdirectory_no", "part_no", "shard_no", "N_doms", "offset"}
    }

    # Precompute PyArrow schemas for better performance
    PYARROW_SCHEMAS = {
        name: pa.schema([(col, dtype) for col, (dtype, _) in schema.items()])
        for name, schema in SCHEMAS.items()
    }

    # Static methods (no need for cls)
    @staticmethod
    def get_nan_replacements(schema_name: str) -> dict:
        """Return dictionary of NaN replacements for a given schema, except for TRUTH."""
        schema_dict = PMTTruthEntity.SCHEMAS.get(schema_name)
        if schema_dict is None:
            raise ValueError(f"Schema '{schema_name}' not found.")
        # Exclude TRUTH from having replacements
        return (
            {}
            if schema_name == "TRUTH"
            else {
                col: replacement
                for col, (_, replacement) in schema_dict.items()
                if replacement is not None
            }
        )

    @staticmethod
    def get_schema(schema_name: str) -> pa.Schema:
        """Return precomputed PyArrow schema for a given schema name."""
        schema = PMTTruthEntity.PYARROW_SCHEMAS.get(schema_name)
        if schema is None:
            raise ValueError(f"Schema '{schema_name}' not found.")
        return schema

    @staticmethod
    def get_excluded_columns(schema_name: str) -> set:
        """Return a set of excluded columns for a given schema."""
        return PMTTruthEntity.TRUTH_EXCLUDED_COLUMNS.get(schema_name, set())
