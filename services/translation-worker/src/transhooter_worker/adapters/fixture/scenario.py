from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID


@dataclass(frozen=True, slots=True)
class FixtureScenario:
    meeting_id: UUID
    path: Path | None

    @classmethod
    def configured(cls, meeting_id: UUID) -> FixtureScenario:
        if os.environ.get("APP_ENV") != "test":
            raise RuntimeError("fixture scenarios require APP_ENV=test")
        value = os.environ.get("FIXTURE_SCENARIO_FILE")
        return cls(meeting_id, Path(value) if value else None)

    def section(self, name: str) -> dict[str, Any]:
        if self.path is None:
            return {}
        try:
            document = json.loads(self.path.read_text("utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError("fixture scenario file is unreadable or invalid") from exc
        consultations = document.get("consultations", {}) if isinstance(document, dict) else {}
        selected_consultation = (
            consultations.get(
                str(self.meeting_id),
                consultations.get("*", {}),
            )
            if isinstance(consultations, dict)
            else {}
        )
        section = (
            selected_consultation.get(name, {}) if isinstance(selected_consultation, dict) else {}
        )
        if not isinstance(section, dict):
            raise RuntimeError(f"fixture scenario section {name} must be an object")
        return section

    def optional_nonnegative_int(self, section: str, field: str) -> int | None:
        config = self.section(section)
        if field not in config:
            return None
        value = config[field]
        if isinstance(value, bool):
            raise RuntimeError(f"fixture scenario {section}.{field} must be a non-negative integer")
        if not isinstance(value, int) or value < 0:
            raise RuntimeError(f"fixture scenario {section}.{field} must be a non-negative integer")
        return value
