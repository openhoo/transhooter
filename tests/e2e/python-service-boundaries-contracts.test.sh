#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
python3 - "$ROOT" <<'PY'
from __future__ import annotations

import ast
import pathlib
import sys
import tomllib

root = pathlib.Path(sys.argv[1])
projects = {
    "spool": root / "packages/spool-store",
    "worker": root / "services/translation-worker",
    "drainer": root / "services/spool-drainer",
}
legacy = (
    root / "apps/translation-worker",
    root / "apps/spool-drainer",
    root / "packages/translation-runtime",
    root / "deploy/docker/translation-runtime.Dockerfile",
)
for path in legacy:
    if path.exists():
        raise SystemExit(f"legacy Python boundary path remains: {path.relative_to(root)}")

for name, path in projects.items():
    if not path.is_dir():
        raise SystemExit(f"missing Python project: {path.relative_to(root)}")
    for required in ("pyproject.toml", "uv.lock", "src"):
        if not (path / required).exists():
            raise SystemExit(f"{name} project is missing {required}")

expected = {
    "spool": ("transhooter-spool-store", {}),
    "worker": (
        "transhooter-translation-worker",
        {
            "transhooter-translation-worker": "transhooter_worker.runtime.worker:main",
            "transhooter-provider": "transhooter_worker.provider_cli:main",
            "transhooter-worker-healthcheck": "transhooter_worker.runtime.healthcheck:main",
        },
    ),
    "drainer": (
        "transhooter-spool-drainer",
        {
            "transhooter-spool-drainer": "transhooter_spool_drainer.runtime:main",
            "transhooter-spool-drainer-healthcheck": "transhooter_spool_drainer.healthcheck:main",
        },
    ),
}
manifests = {
    name: tomllib.loads(path.joinpath("pyproject.toml").read_text())
    for name, path in projects.items()
}
for name, (distribution, scripts) in expected.items():
    manifest = tomllib.loads((projects[name] / "pyproject.toml").read_text())
    project = manifest.get("project", {})
    if project.get("name") != distribution:
        raise SystemExit(f"{name} distribution must be {distribution!r}")
    if project.get("scripts", {}) != scripts:
        raise SystemExit(
            f"{name} console scripts differ: {project.get('scripts', {})!r} != {scripts!r}"
        )

forbidden_by_owner = {
    "spool": {"transhooter_worker", "transhooter_spool_drainer"},
    "worker": {"transhooter_spool_drainer"},
    "drainer": {"transhooter_worker"},
}
shared_forbidden = {
    "boto3", "botocore", "httpx", "jsonschema", "livekit", "opentelemetry",
    "transhooter_worker", "transhooter_spool_drainer", "websockets",
}
legacy_roots = {"apps", "transhooter_translation_runtime"}

def imported_modules(path: pathlib.Path) -> list[tuple[pathlib.Path, str, int]]:
    imports: list[tuple[pathlib.Path, str, int]] = []
    for source in sorted(path.rglob("*.py")):
        tree = ast.parse(source.read_text(), filename=str(source))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.extend((source, alias.name, node.lineno) for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.append((source, node.module, node.lineno))
    return imports

dependency_modules = {
    "boto3": "boto3", "cryptography": "cryptography", "google-cloud-speech": "google",
    "grpcio": "grpc", "httpx": "httpx", "jsonschema": "jsonschema",
    "livekit": "livekit", "opentelemetry-api": "opentelemetry",
    "pydantic": "pydantic", "websockets": "websockets",
}

def dependency_names(manifest: dict) -> set[str]:
    names = set()
    for requirement in manifest.get("project", {}).get("dependencies", []):
        name = requirement.split(";", 1)[0].strip().split("[", 1)[0]
        for separator in ("==", ">=", "<=", "~=", "!=", ">", "<"):
            name = name.split(separator, 1)[0]
        names.add(name.strip().lower())
    return names

for owner, manifest in manifests.items():
    dependencies = dependency_names(manifest)
    if "awscrt" in dependencies:
        raise SystemExit(f"{owner} restored forbidden awscrt dependency")
    imported = {module.split(".", 1)[0] for _, module, _ in imported_modules(projects[owner] / "src")}
    for dependency, module in dependency_modules.items():
        if module in imported and dependency not in dependencies:
            raise SystemExit(f"{owner} imports {module} without direct dependency {dependency}")
    if "botocore" in imported and "boto3" not in dependencies:
        raise SystemExit(f"{owner} imports botocore without the boto3 ownership exception")

for owner, project in projects.items():
    for source, module, line in imported_modules(project / "src"):
        top = module.split(".", 1)[0]
        if top in legacy_roots or top in forbidden_by_owner[owner]:
            raise SystemExit(f"forbidden {owner} import {module} at {source.relative_to(root)}:{line}")
        if owner == "spool" and top in shared_forbidden:
            raise SystemExit(f"shared spool imports runtime dependency {module} at {source.relative_to(root)}:{line}")
        if owner in {"worker", "drainer"} and module.startswith("transhooter_spool."):
            raise SystemExit(
                f"{owner} deep-imports spool internals at {source.relative_to(root)}:{line}: {module}"
            )

facade = projects["spool"] / "src/transhooter_spool/__init__.py"
tree = ast.parse(facade.read_text(), filename=str(facade))
all_value = None
for statement in tree.body:
    if isinstance(statement, ast.Assign) and any(
        isinstance(target, ast.Name) and target.id == "__all__" for target in statement.targets
    ):
        all_value = tuple(ast.literal_eval(statement.value))
        break
expected_all = (
    "CapacityProbe", "ConsultationProducerAuthority", "ConsultationRecoveryAuthority",
    "EncryptedSpool", "ObjectRecord", "RawRef", "SampleRange", "SpoolCapacity",
    "SpoolCheckpointDelivery", "SpoolCheckpointStore", "SpoolConsultationSeal",
    "SpoolDrainer", "SpoolProducer", "SpoolRecordContext", "SpoolRecordDelivery",
    "SpoolUnavailable", "TerminalCheckpointIntent", "deterministic_roomy_capacity",
    "statvfs_capacity",
)
if all_value != expected_all:
    raise SystemExit(f"transhooter_spool.__all__ differs: {all_value!r}")
for forbidden in ("ArchiveStore", "ControlClient", "PcmCompactor", "S3Archive", "TelemetryHandle"):
    if forbidden in all_value:
        raise SystemExit(f"service orchestration leaked through spool facade: {forbidden}")
sys.path.insert(0, str(projects["spool"] / "src"))
import transhooter_spool  # noqa: E402
if tuple(transhooter_spool.__all__) != expected_all:
    raise SystemExit("imported transhooter_spool facade differs from its declared AST contract")
for symbol in expected_all:
    if not hasattr(transhooter_spool, symbol):
        raise SystemExit(f"transhooter_spool facade does not export {symbol}")

images = {
    "worker": root / "deploy/docker/translation-worker.Dockerfile",
    "drainer": root / "deploy/docker/spool-drainer.Dockerfile",
}
for owner, dockerfile in images.items():
    if not dockerfile.is_file():
        raise SystemExit(f"missing production Dockerfile: {dockerfile.relative_to(root)}")
    text = dockerfile.read_text()
    sibling = "services/spool-drainer" if owner == "worker" else "services/translation-worker"
    sibling_root = "transhooter_spool_drainer" if owner == "worker" else "transhooter_worker"
    if sibling in text or sibling_root in text:
        raise SystemExit(f"{owner} image copies or names sibling service")
    if "packages/spool-store" not in text or f"services/{'translation-worker' if owner == 'worker' else 'spool-drainer'}" not in text:
        raise SystemExit(f"{owner} image does not install its service plus spool-store")
contract_path = root / "tests/e2e/python-service-boundaries-contracts.test.sh"
repository_text = "\n".join(
    path.read_text(errors="replace")
    for base in (root / "tests", root / "README.md")
    for path in ([base] if base.is_file() else base.rglob("*"))
    if path.is_file() and path != contract_path
)
for rejected in (
    "transhooter-translation-runtime",
    "transhooter_translation_runtime",
    "python apps/translation-worker/main.py",
    "python apps/spool-drainer/main.py",
):
    if rejected in repository_text:
        raise SystemExit(f"legacy Python compatibility reference returned: {rejected}")

print("python service boundary contracts passed")
PY
