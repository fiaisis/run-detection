"""
Rules for Osiris
"""
from rundetection.ingest import JobRequest
from rundetection.rules.rule import Rule


class OsirisPanadiumRule(Rule[int]):
    """
    Inserts the cycles panadium number into the request. This value is manually calcuated once per cycle
    """

    def verify(self, job_request: JobRequest) -> None:
        job_request.additional_values["panadium"] = self._value


class OsirisStitchRule(Rule[bool]):
    """
    Enables Osiris Run stitching
    """

    def verify(self, job_request: JobRequest) -> None:
        pass
