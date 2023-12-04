"""
RunDetection Exceptions
"""


class IngestError(Exception):
    """
    An unrecoverable error occurred when attempting to ingest a nexus file
    """


class ReductionMetadataError(IngestError):
    """
    When metadata is incomplete or out out the expected form such that a reduction is not possible
    """
