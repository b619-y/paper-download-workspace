from __future__ import annotations

import csv
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/update_batch_status.py"


class UpdateBatchStatusTests(unittest.TestCase):
    def test_direct_acceptance_is_rejected_without_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            batch_id = "DL-1"
            status_path = root / "02_batches" / batch_id / "STATUS.json"
            status_path.parent.mkdir(parents=True)
            status_path.write_text(json.dumps({
                "batch_id": batch_id, "status": "REVIEW", "claimed_by": "worker",
            }), encoding="utf-8")
            index = root / "00_control/BATCH_INDEX.csv"
            index.parent.mkdir(parents=True)
            with index.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=[
                    "batch_id", "status", "claimed_by", "updated_at", "accepted_at",
                ])
                writer.writeheader()
                writer.writerow({"batch_id": batch_id, "status": "REVIEW", "claimed_by": "worker"})

            completed = subprocess.run([
                sys.executable, str(SCRIPT), "--root", str(root),
                "--actor-role", "coordinator", batch_id, "ACCEPTED",
            ], capture_output=True, text=True)

            self.assertNotEqual(completed.returncode, 0)
            self.assertIn("accept_batch.py", completed.stderr)
            self.assertEqual(json.loads(status_path.read_text())["status"], "REVIEW")
            with index.open(encoding="utf-8", newline="") as handle:
                self.assertEqual(next(csv.DictReader(handle))["status"], "REVIEW")


if __name__ == "__main__":
    unittest.main()
