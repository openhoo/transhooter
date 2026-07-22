import base64
import json
import os
import subprocess
import sys
import wave
from pathlib import Path


def test_benchmark_cli_accepts_versioned_language_manifest(tmp_path: Path) -> None:
    audio = tmp_path / "accent.wav"
    with wave.open(str(audio), "wb") as target:
        target.setnchannels(1)
        target.setsampwidth(2)
        target.setframerate(48000)
        target.writeframes(b"\1\0\2\0\3\0" * 1600)
    manifest = tmp_path / "provider-benchmark.json"
    manifest_payload = {
        "schemaVersion": 1,
        "fixtures": [
            {
                "id": "accent-a",
                "audio": audio.name,
                "sourceLanguage": "en-US",
                "targetLanguages": ["de-DE", "fr-FR"],
                "referenceText": "Good morning",
            }
        ],
    }
    manifest.write_text(json.dumps(manifest_payload), "utf-8")
    environment = os.environ.copy()
    environment.update(
        APP_ENV="test",
        SPOOL_PATH=str(tmp_path / "spool"),
        SPOOL_KEY_B64=base64.b64encode(b"k" * 32).decode(),
    )
    command = [
        str(Path(sys.executable).with_name("transhooter-provider")),
        "benchmark",
        "--profiles",
        "fixture",
        "--fixtures",
        str(manifest),
    ]
    completed = subprocess.run(
        command,
        text=True,
        capture_output=True,
        env=environment,
        check=True,
    )
    result = json.loads(completed.stdout)
    assert result["schemaVersion"] == 1
    assert len(result["runs"]) == 2
    assert {row["target"] for row in result["runs"]} == {"de-DE", "fr-FR"}
    assert result["p50Ms"] >= 0
    assert result["p95Ms"] >= result["p50Ms"]
    assert all(len(row["attemptIds"]) == 3 for row in result["rawAttemptIds"])
    assert all(row["units"]["sttAudioSeconds"] > 0 for row in result["usageUnits"])
