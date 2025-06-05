"""
Script that moves all json specifications from this repository to the database via PUT calls to the FIA_API.

It should be run from the root directory of this repository.
It expects 2 command line args, the FIA_API_URL and the FIA_API_API_KEY
"""

import json
import sys
from pathlib import Path

import requests

FIA_API_URL = sys.argv[1]
"""
The API key doesn't work when running in a bash terminal (due to the non
alpha-numeric characters). So it needs to be temporarily added below when running,
and removed after (DO NOT COMMIT the hardcoded FIA_API_API_KEY)
"""
FIA_API_API_KEY = sys.argv[2]

successful_update: int = 200
auth_headers: json = {"Authorization": f"Bearer {FIA_API_API_KEY}", "accept": "application/json"}

# locate specifications
specifications_path = Path("rundetection/specifications")
specifications_list = sorted(specifications_path.glob("*.json"))


for file_name in specifications_list:
    instrument_name = file_name.stem.split("_")[0].upper()
    with file_name.open(mode="r", encoding="utf-8") as specification:
        json_spec = json.load(specification)

        response = requests.put(
            url=f"{FIA_API_URL}/instrument/{instrument_name}/specification",
            json=json_spec,
            headers=auth_headers,
            timeout=2000,
        )
        print(f"PUT result, {instrument_name} - {response.status_code}")  # noqa: T201
