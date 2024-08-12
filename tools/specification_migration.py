"""
This script moves the all json specifications from this repository to the database via PUT calls to the FIA_API

It should be run from the root directory of this repository, and expects 1 command line arg the FIA_API_API_KEY
"""

import json
import sys
from pathlib import Path

import requests

FIA_API_API_KEY = sys.argv[1]

FIA_API_PATH: str = "http://localhost:8000"
successful_update: int = 200
auth_headers: json = {"Authorization": f"Bearer {FIA_API_API_KEY}", "accept": "application/json"}
# store names and cod
failed_instruments: list[list[str, int]] = []


# locate specifications
specifications_path = Path("rundetection/specifications")
specifications_list = sorted(specifications_path.glob("*.json"))


for file_name in specifications_list:
    instrument_name = file_name.stem.split("_")[0].upper()
    with file_name.open(mode="r", encoding="utf-8") as specification:
        json_spec = json.load(specification)

        response = requests.put(
            url=f"{FIA_API_PATH}/instrument/{instrument_name}/specification",
            json=json_spec,
            headers=auth_headers,
            timeout=2000,
        )

        if response.status_code != successful_update:
            failed_instruments.append([instrument_name, response.status_code])
        print(f"Updated {instrument_name} via PUT ", response.status_code, response.text)

print("The following instruments failed to update", failed_instruments)
