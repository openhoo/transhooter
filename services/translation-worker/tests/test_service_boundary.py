from __future__ import annotations

import ast
import tomllib
from pathlib import Path

PROJECT_ROOT = Path(__file__).parents[1]
SOURCE_ROOT = PROJECT_ROOT / "src" / "transhooter_worker"


def test_manifest_exposes_only_worker_commands() -> None:
    manifest = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text("utf-8"))
    assert manifest["project"]["scripts"] == {
        "transhooter-translation-worker": "transhooter_worker.runtime.worker:main",
        "transhooter-provider": "transhooter_worker.provider_cli:main",
        "transhooter-worker-healthcheck": "transhooter_worker.runtime.healthcheck:main",
    }


def test_worker_uses_only_public_spool_facade_and_no_drainer_imports() -> None:
    violations: list[str] = []
    for path in SOURCE_ROOT.rglob("*.py"):
        tree = ast.parse(path.read_text("utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                if node.module.startswith("transhooter_spool."):
                    violations.append(f"{path}: deep spool import {node.module}")
                if node.module.startswith("transhooter_spool_drainer"):
                    violations.append(f"{path}: drainer import {node.module}")
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith("transhooter_spool."):
                        violations.append(f"{path}: deep spool import {alias.name}")
                    if alias.name.startswith("transhooter_spool_drainer"):
                        violations.append(f"{path}: drainer import {alias.name}")
    assert violations == []


def test_worker_production_runtime_has_no_archive_delivery_dependency() -> None:
    production = "\n".join(
        path.read_text("utf-8")
        for path in SOURCE_ROOT.rglob("*.py")
        if path.name != "diagnostic_archive.py" and path.name != "provider_cli.py"
    )
    for forbidden in (
        "boto3",
        "S3Archive",
        "PcmCompactor",
        "ArchiveDeliveryExecutor",
        "transhooter_spool_drainer",
    ):
        assert forbidden not in production
