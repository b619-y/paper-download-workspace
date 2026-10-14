from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "init_download_workspace.py"


def run_init(root: Path) -> None:
    subprocess.run(
        [sys.executable, str(SCRIPT), "--root", str(root)],
        check=True,
        capture_output=True,
        text=True,
    )


def workspace_identity(root: Path) -> None:
    control = root / "00_control"
    control.mkdir(parents=True)
    (control / "WORKSPACE.json").write_text(
        json.dumps({"workspace_id": "paper-download-workspace"}) + "\n",
        encoding="utf-8",
    )


def test_fresh_public_clone_reconciles_missing_runtime_skeleton(tmp_path: Path) -> None:
    root = tmp_path / "clone"
    workspace_identity(root)

    run_init(root)

    assert (root / "00_control/BATCH_INDEX.csv").is_file()
    assert (root / "04_library_index/PAPER_INDEX.csv").is_file()
    assert (root / "00_control/DECISIONS.md").is_file()
    assert (root / "runtime").is_dir()

    decisions = root / "00_control/DECISIONS.md"
    decisions.write_text("# Decisions\n\nkeep me\n", encoding="utf-8")
    run_init(root)
    assert decisions.read_text(encoding="utf-8") == "# Decisions\n\nkeep me\n"


def test_reconcile_appends_known_paper_index_columns_without_losing_rows(
    tmp_path: Path,
) -> None:
    root = tmp_path / "legacy"
    workspace_identity(root)
    library = root / "04_library_index"
    library.mkdir()
    legacy_fields = [
        "paper_id", "title", "doi", "pdf_path", "supplement_paths", "sha256",
        "source_batch_id", "identity_verified", "accepted_at", "notes",
        "mineru_md_path", "xml_md_path", "source_item_id", "document_id",
        "source_kind", "processed_text_sha256", "processing_method",
        "processing_version", "processing_accepted_at",
    ]
    index = library / "PAPER_INDEX.csv"
    with index.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=legacy_fields)
        writer.writeheader()
        writer.writerow({"paper_id": "P-1", "title": "Preserved"})

    run_init(root)

    with index.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
    assert reader.fieldnames is not None
    assert reader.fieldnames[-4:] == [
        "section_chunks_path", "section_chunks_sha256",
        "section_chunks_count", "section_chunks_schema_version",
    ]
    assert rows == [{**{field: "" for field in reader.fieldnames},
                     "paper_id": "P-1", "title": "Preserved"}]
