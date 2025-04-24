import json
from pathlib import Path
from typing import Any, Literal

from fastapi import APIRouter

ROUTER = APIRouter()


def get_specification_from_file(instrument: str) -> Any:
    """
    Given an instrument, return the specification
    :param instrument: The instrument for which specification to get
    :return: The specification file contents
    """
    path = Path(f"/data/{instrument.lower()}_specification.json")
    with path.open(encoding="utf-8") as fle:
        return json.load(fle)


@ROUTER.get("/instrument/{instrument_name}/specification", response_model=None)
async def get_instrument_specification(instrument_name: str) -> Any:
    """
    This is a fake API for the e2e tests to be deployed to provide specifications for rundetection e2e tests
    """
    return get_specification_from_file(instrument_name)


@ROUTER.get("/healthz")
async def get() -> Literal["ok"]:
    """Health Check endpoint."""
    return "ok"
