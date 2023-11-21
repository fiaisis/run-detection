"""
Rules for Iris
"""
from rundetection.ingest import JobRequest
from rundetection.rules.rule import Rule


class IrisPanadiumRule(Rule[int]):
    """
    Inserts the cycles panadium number. This is calculated once per cycle and manually configured into the spec.
    """

    def verify(self, job_request: JobRequest) -> None:
        job_request.additional_values["panadium"] = self._value
