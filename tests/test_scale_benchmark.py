from __future__ import annotations

import unittest

from frontier_ingest.scale_benchmark import run_scale_benchmark


class ScaleBenchmarkTests(unittest.TestCase):
    def test_streaming_benchmark_counts_partial_batch(self) -> None:
        report = run_scale_benchmark(records=401, batch_size=200)
        self.assertEqual(report.records, 401)
        self.assertEqual(report.batches, 3)
        self.assertGreater(report.records_per_second, 0)
        self.assertTrue(report.checksum)


if __name__ == "__main__":
    unittest.main()
