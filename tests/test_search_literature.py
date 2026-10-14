from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from search_literature import run_pubmed


class _ZeroHitPubMedClient:
    mailto = ""

    def json(self, url: str, *, params: dict[str, object]) -> dict[str, object]:
        self.url = url
        self.params = params
        return {"esearchresult": {"count": "0", "webenv": "", "querykey": ""}}

    def request(self, *args: object, **kwargs: object) -> bytes:
        raise AssertionError("zero-hit PubMed search must not call EFetch")


class PubMedSearchTests(unittest.TestCase):
    def test_zero_hits_still_records_one_auditable_raw_page(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            raw_dir = Path(temporary)
            result = run_pubmed(
                _ZeroHitPubMedClient(),
                "soil chromium",
                "soil chromium",
                raw_dir,
                {},
                0,
            )

            self.assertTrue(result["complete"])
            self.assertEqual(result["reported_total"], 0)
            self.assertEqual(result["fetched_count"], 0)
            self.assertEqual(result["pages"], 1)
            self.assertEqual(result["records"], [])
            self.assertEqual(
                [path.name for path in raw_dir.glob("page-*.json")],
                ["page-000001.json"],
            )


if __name__ == "__main__":
    unittest.main()
