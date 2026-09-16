"""Rules for GEM."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rundetection.job_requests import JobRequest
from rundetection.ingestion.extracts import get_cycle_string_from_path
from rundetection.rules.rule import Rule


class GEMModeRule(Rule[str]):
    """Rule to set the GEM mode in the job request's additional values."""

    def verify(self, job_request: JobRequest) -> None:
        """
        Verify the rule against the job request. Sets the GEM mode in the job request's additional values.

        :param job_request: The job request to verify.
        :return: None.
        """
        job_request.additional_values["mode"] = self._value


class GEMInputModeRule(Rule[str]):
    """Rule to set the GEM input mode in the job request's additional values."""

    def verify(self, job_request: JobRequest) -> None:
        """
        Verify the rule against the job request. Sets the GEM input mode in the job request's additional values.

        :param job_request: The job request to verify.
        :return: None.
        """
        job_request.additional_values["input_mode"] = self._value


class GEMVanNormRule(Rule[bool]):
    """Rule to set the GEM vanadium normalization flag in the job request's additional values."""

    def verify(self, job_request: JobRequest) -> None:
        """
        Verify the rule against the job request.
        Sets the GEM vanadium normalization flag in the job request's additional values.

        :param job_request: The job request to verify.
        :return: None.
        """
        job_request.additional_values["van_norm"] = self._value


class GEMDoAbsorbCorrectionsRule(Rule[bool]):
    """Rule to set the GEM absorb corrections flag in the job request's additional values."""

    def verify(self, job_request: JobRequest) -> None:
        """
        Verify the rule against the job request.
        Sets the GEM absorb corrections flag in the job request's additional values.

        :param job_request: The job request to verify.
        :return: None.
        """
        job_request.additional_values["do_absorb_corrections"] = self._value


class GEMMultipleScatteringRule(Rule[bool]):
    """Rule to set the GEM multiple scattering flag in the job request's additional values."""

    def verify(self, job_request: JobRequest) -> None:
        """
        Verify the rule against the job request.
        Sets the GEM multiple scattering flag in the job request's additional values.

        :param job_request: The job request to verify.
        :return: None.
        """
        job_request.additional_values["multiple_scattering"] = self._value


class GEMRietveldPDFVanEmptyRule(Rule[dict[str, dict[str, str]]]):
    """Rule to set the GEM Rietveld PDF vanadium and empty run numbers in the job request's additional values."""

    def verify(self, job_request: JobRequest) -> None:
        """
        Verify the rule against the job request.
        Sets the GEM Rietveld PDF vanadium and empty run numbers in the job request's additional values.

        :param job_request: The job request to verify.
        :return: None.
        """
        job_request.additional_values["rietveldvanrunnumbers"] = self._value["rietveld"]["vanadium_run_numbers"]
        job_request.additional_values["rietveldemptyrunnumbers"] = self._value["rietveld"]["empty_run_numbers"]
        job_request.additional_values["pdfvanrunnumbers"] = self._value["pdf"]["vanadium_run_numbers"]
        job_request.additional_values["pdfemptyrunnumbers"] = self._value["pdf"]["empty_run_numbers"]


class GEMOffsetFileRule(Rule[str]):
    """Rule to set the GEM offset file in the job request's additional values."""

    def verify(self, job_request: JobRequest) -> None:
        """
        Verify the rule against the job request.
        Sets the GEM offset file in the job request's additional values.

        :param job_request: The job request to verify.
        :return: None.
        """
        job_request.additional_values["offset_file"] = self._value


class GEMCycleRule(Rule[str]):
    """Rule to set the current cycle in the job request's filepath"""

    def verify(self, job_request: JobRequest) -> None:
        """
        Verify the rule against the job request.

        :param job_request: The job request to verify.
        :return: None.
        """
        cycle = get_cycle_string_from_path(job_request.filepath)
        job_request.additional_values["cycle"] = cycle
