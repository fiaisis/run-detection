import json
from pathlib import Path

DISABLED_INSTRUMENTS = [
    "SANDALS",
    "NIMROD",
    "INES",
    "EMMA",
    "ChipIP",
    "CRISP",
    "SURF",
    "SXD",
    "IMAT",
    "EMMA-A",
    "CHRONUS",
    "ARGUS",
]

ALL_INSTRUMENTS = [
    "ALF",
    "ARGUS",
    "CHIPIR",
    "CHRONUS",
    "CRISP",
    "DEVA",
    "EMMAA",
    "EMU",
    "ENGINX",
    "EVS",
    "GEM",
    "HET",
    "HIFI",
    "HRPD",
    "IMAT",
    "INES",
    "INTER",
    "IRIS",
    "LAD",
    "LARMOR",
    "LET",
    "LOQ",
    "MAPS",
    "MARI",
    "MERLIN",
    "MUSR",
    "NILE",
    "NIMROD",
    "OFFSPEC",
    "OSIRIS",
    "PEARL",
    "PEARL (HIPR)",
    "POLARIS",
    "POLREF",
    "PRISMA",
    "ROTAX",
    "SANDALS",
    "SANS2D",
    "SURF",
    "SXD",
    "TFXA",
    "TOSCA",
    "VESUVIO",
    "WISH",
    "ZOOM",
]


def generate_specification(instrument: str, enabled: bool = True) -> None:
    """
    Generate an empty specification file for the given instrument
    :param enabled: Whether or not reduction should be enabled
    :param instrument: the instrument to generate for
    :return: None
    """
    default_rules = {
        "enabled": enabled,
    }
    with open(Path(f"../rundetection/specifications/{instrument.lower()}_specification.json"), "w") as file:
        file.write(json.dumps(default_rules))


for instrument in ALL_INSTRUMENTS:
    generate_specification(instrument)

for instrument in DISABLED_INSTRUMENTS:
    generate_specification(instrument, enabled=False)
