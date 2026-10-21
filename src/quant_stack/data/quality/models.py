from __future__ import annotations

from typing import Literal

from pydantic import Field

from ..models import DataModel, DatasetQualityMetadata


class DatasetQualityReport(DataModel):
    dataset_id: str = Field(min_length=1)
    provider: str = Field(min_length=1)
    checked_rows: int = Field(ge=0)
    passed_checks: list[str] = Field(default_factory=list)
    failed_checks: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    quality_status: Literal["passed", "warning", "failed"]

    def manifest_metadata(self) -> DatasetQualityMetadata:
        return DatasetQualityMetadata(
            status=self.quality_status,
            checks=self.passed_checks,
            failed_checks=self.failed_checks,
            warnings=self.warnings,
        )


class DataQualityError(RuntimeError):
    """Quality-gate failure carrying a serializable report for operators."""

    def __init__(self, report: DatasetQualityReport, report_uri: str | None = None) -> None:
        self.report = report
        self.report_uri = report_uri
        failed = ", ".join(report.failed_checks)
        super().__init__(f"Dataset quality validation failed: {failed}")
