"""Rules for GEM."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rundetection.job_requests import JobRequest
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


class GEMCalibrationMappingFileRule(Rule[str]):
    """Adds the calibration mapping file to JobRequest."""

    def verify(self, job_request: JobRequest) -> None:
        """
        Add the calibration mapping file to the job request's additional values.

        :param job_request: The job request to update with the calibration file.
        """
        job_request.additional_values["cal_mapping_file"] = self._value


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


class GEMReitveldPDFVanEmptyRunNumbersRule(Rule[dict[str, dict[str, str]]]):
    """Rule to set the GEM Reitveld & PDF vanadium & empty run numbers in the job request's additional values."""

    def verify(self, job_request: JobRequest) -> None:
        """
        Verify the rule against the job request.
        Sets the GEM Reitveld & PDF vanadium & empty run numbers in the job request's additional values.

        :param job_request: The job request to verify.
        :return: None.
        """
        job_request.additional_values["reitveld_pdf_van_empty_run_numbers"] = self._value


class GEMOffsetFileRule(Rule[str]):
    """Rule to set the GEM offset file in the job request's additional values."""

    def verify(self, job_request: JobRequest) -> None:
        """
        Verify the rule against the job request.
        Sets the GEM offset file in the job request's additional values.

        :param job_request: The job request to verify.
        :return: None.
        """
        job_request.additional_values["offsets_2023_cycle231.cal"] = self._value


class GEMCycleRule(Rule[str]):
    """Rule to set the current cycle in the job request's additional values."""

    def verify(self, job_request: JobRequest) -> None:
        """
        Verify the rule against the job request.
        Sets the current cycle in the job request's additional values.

        :param job_request: The job request to verify.
        :return: None.
        """
        job_request.additional_values["cycle"] = self._value