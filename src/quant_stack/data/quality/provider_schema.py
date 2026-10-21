from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from pydantic import Field

from ..models import DataModel

type NativeType = type[object] | tuple[type[object], ...]


@dataclass(frozen=True)
class ProviderNativeSchema:
    required_fields: Mapping[str, NativeType]


class ProviderSchemaValidationReport(DataModel):
    provider: str = Field(min_length=1)
    checked_rows: int = Field(ge=0)
    required_fields: list[str]
    missing_fields: list[str] = Field(default_factory=list)
    invalid_types: list[str] = Field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.missing_fields and not self.invalid_types


class ProviderSchemaDriftError(RuntimeError):
    def __init__(self, report: ProviderSchemaValidationReport) -> None:
        self.report = report
        details = [*report.missing_fields, *report.invalid_types]
        super().__init__(
            f"Provider-native schema validation failed for {report.provider}: " + "; ".join(details)
        )


class ProviderSchemaValidator:
    """Validates exact provider-native fields and types without coercion or aliases."""

    def validate(
        self,
        *,
        provider: str,
        records: Sequence[Mapping[str, object]],
        schema: ProviderNativeSchema,
    ) -> ProviderSchemaValidationReport:
        missing: list[str] = []
        invalid_types: list[str] = []
        for index, record in enumerate(records):
            for field, expected_type in schema.required_fields.items():
                if field not in record:
                    missing.append(f"row {index}: missing required provider field {field}")
                    continue
                value = record[field]
                if isinstance(value, bool) or not isinstance(value, expected_type):
                    type_names = self._type_names(expected_type)
                    invalid_types.append(
                        f"row {index}: provider field {field} must be {type_names}, "
                        f"got {type(value).__name__}"
                    )
        report = ProviderSchemaValidationReport(
            provider=provider,
            checked_rows=len(records),
            required_fields=list(schema.required_fields),
            missing_fields=missing,
            invalid_types=invalid_types,
        )
        if not report.passed:
            raise ProviderSchemaDriftError(report)
        return report

    @staticmethod
    def _type_names(expected_type: NativeType) -> str:
        types = expected_type if isinstance(expected_type, tuple) else (expected_type,)
        return " or ".join(item.__name__ for item in types)
