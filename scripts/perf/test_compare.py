#!/usr/bin/env python3
"""Synthetic artifact checks only: no Cargo, services, sockets, or load generation."""

import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location("comparison_under_review", Path(__file__).with_name("compare.py"))
comparison = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(comparison)


def write(path, value):
    path.write_text(json.dumps(value))


class ComparisonArtifacts(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="gproxy-compare-fixture-")
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.before = self.directory / "before"
        self.after = self.directory / "after"
        self.arguments = {"scenarios": ["buffered"], "concurrency": [1], "storage_rtt_ms": [0.0],
                          "summary_rows": [10000], "rounds": 3}
        for side, sha in ((self.before, "a" * 64), (self.after, "b" * 64)):
            side.mkdir()
            write(side / "metadata.json", {"arguments": self.arguments, "binary_sha256": sha,
                                          "finished_at_unix": 1, "valid_for_comparison": True})
            write(side / "failures.json", {"failures": []})

    def metadata(self, side, mutate):
        record = comparison.read_metadata(side)
        mutate(record)
        write(side / "metadata.json", record)

    def gateway(self, side, round_number, *, ok=True, reconciled=True, errors=0, rps=100, warmup=100):
        data = {"errors": 0, "completed": 1000, "successful_rps": rps,
                "successful_latency_ms": {"p95": 10, "p99": 20}}
        report = {"mode": "gateway", "scenario": "buffered", "concurrency": 1,
                  "storage_rtt_ms": 0.0, "round": round_number, "requests": 1000,
                  "warmup_requests": warmup, "ok": ok, "reconciliation": {"ok": reconciled},
                  "direct": copy.deepcopy(data), "gateway": copy.deepcopy(data)}
        report["gateway"]["errors"] = errors
        path = side / f"buffered-c1-r{round_number}-rtt0.json"
        write(path, report)
        return path

    def summary(self, side, *, rows=10000, ms=10):
        write(side / f"summary-{rows}.json", {"mode": "summary", "rows": rows, "ok": True,
              "rounds": [{"round": round_number, "totals": {"requests": rows}, "summary_ms": ms,
                          "first_page_ms": ms / 2, "last_page_ms": ms * 2} for round_number in (1, 2, 3)]})

    def failure(self, round_number, *, phase="gateway", result=None):
        record = {"phase": phase, "case": f"buffered-c1-r{round_number}-rtt0", "round": round_number,
                  "scenario": "buffered", "concurrency": 1, "storage_rtt_ms": 0.0, "rows": 10000,
                  "error": "synthetic warmup or reconciliation failure", "return_code": 1,
                  "log": f"fixture-r{round_number}.log", "result": result}
        return record

    def results(self):
        return comparison.compare(comparison.load(self.before), comparison.load(self.after), 3,
                                  comparison.build_reasons(self.before, self.after),
                                  comparison.load_failures(self.before), comparison.load_failures(self.after))

    def one(self, results, *, target="gateway", kind="gateway"):
        return next(row for row in results if row["kind"] == kind and row["target"] == (target if kind == "gateway" else None))

    def successful(self):
        for side, rps, ms in ((self.before, 100, 10), (self.after, 120, 8)):
            for round_number in (1, 2, 3):
                self.gateway(side, round_number, rps=rps)
            self.summary(side, ms=ms)

    def test_full_planned_matrix_is_present_without_any_result_files(self):
        args = {"scenarios": ["buffered", "stream", "convert"], "concurrency": [1, 32, 128, 512, 1024],
                "storage_rtt_ms": [0.0, 2.0, 10.0], "summary_rows": [10000, 100000, 1000000], "rounds": 3}
        for side in (self.before, self.after):
            self.metadata(side, lambda record: record.update(arguments=args))
        results = self.results()
        self.assertEqual(len(results), 45)
        self.assertEqual(sum(row["kind"] == "gateway" for row in results), 42)
        self.assertTrue(all(row["concurrency"] == 128 for row in results if row["storage_rtt_ms"] in (2, 10)))
        self.assertTrue(all(not row["comparable"] for row in results))
        self.assertTrue(all(row["before"]["missing_round_ids"] == [1, 2, 3] for row in results))

    def test_successful_gateway_and_summary_compare_independently(self):
        self.successful()
        results = self.results()
        self.assertEqual(len(results), 3)
        self.assertTrue(all(row["comparable"] for row in results))
        self.assertAlmostEqual(self.one(results)["improvement_pct"]["rps"], 20)
        self.assertAlmostEqual(self.one(results, kind="summary")["improvement_pct"]["summary_ms"], 20)

    def test_both_sides_warmup_failures_remain_visible_without_metrics(self):
        for side in (self.before, self.after):
            self.summary(side)
            write(side / "failures.json", {"failures": [self.failure(r) for r in (1, 2, 3)]})
        results = self.results()
        for row in (row for row in results if row["kind"] == "gateway"):
            self.assertFalse(row["comparable"])
            self.assertTrue(all(value is None for value in row["improvement_pct"].values()))
            for side in ("before", "after"):
                self.assertEqual(row[side]["observed_rounds"], 0)
                self.assertEqual(row[side]["failed_cases"], 3)
                self.assertEqual(row[side]["missing_round_ids"], [1, 2, 3])
                self.assertIsNone(row[side]["errors"])
                self.assertIsNone(row[side]["medians"]["rps"])
                self.assertEqual(row[side]["failures"][0]["log"], "fixture-r1.log")
        self.assertTrue(self.one(results, kind="summary")["comparable"])

    def test_one_missing_round_excludes_partial_median(self):
        self.successful()
        (self.before / "buffered-c1-r3-rtt0.json").unlink()
        write(self.before / "failures.json", {"failures": [self.failure(3)]})
        row = self.one(self.results())
        self.assertEqual(row["before"]["valid_rounds"], 2)
        self.assertEqual(row["before"]["missing_round_ids"], [3])
        self.assertEqual(row["before"]["medians"]["rps"], 100)
        self.assertFalse(row["comparable"])
        self.assertIsNone(row["improvement_pct"]["rps"])

    def test_all_http_success_but_failed_reconciliation_excludes_7001_rps(self):
        self.successful()
        path = self.gateway(self.before, 1, ok=False, reconciled=False, errors=0, rps=7001)
        report = json.loads(path.read_text())
        report["reconciliation"].update(pending_admissions=913, drain_ms=60000)
        write(path, report)
        row = self.one(self.results())
        self.assertEqual(row["before"]["errors"], 0)
        self.assertEqual(row["before"]["reconciliation_failures"], 1)
        self.assertEqual(row["before"]["valid_rounds"], 2)
        self.assertEqual(row["before"]["medians"]["rps"], 100)
        self.assertIn(str(path), row["before"]["invalid_artifacts"])
        self.assertFalse(row["comparable"])
        self.assertIsNone(row["improvement_pct"]["rps"])
        self.assertEqual(json.loads(path.read_text())["gateway"]["successful_rps"], 7001)

    def test_http_errors_with_successful_ledger_remain_invalid(self):
        self.successful()
        self.gateway(self.before, 1, ok=False, errors=47)
        results = self.results()
        row = self.one(results)
        self.assertEqual(row["before"]["errors"], 47)
        self.assertEqual(row["before"]["reconciliation_failures"], 0)
        self.assertFalse(row["comparable"])
        self.assertFalse(self.one(results, target="direct")["comparable"])
        self.assertTrue(self.one(results, kind="summary")["comparable"])

    def test_summary_init_failures_exclude_only_summary(self):
        self.successful()
        for side in (self.before, self.after):
            (side / "summary-10000.json").unlink()
            write(side / "failures.json", {"failures": [self.failure(1, phase="init")]})
        results = self.results()
        self.assertTrue(self.one(results)["comparable"])
        summary = self.one(results, kind="summary")
        self.assertFalse(summary["comparable"])
        self.assertEqual(summary["before"]["failures"][0]["phase"], "init")

    def test_recorded_failure_overrides_apparently_successful_result(self):
        self.successful()
        write(self.before / "failures.json", {"failures": [self.failure(1)]})
        row = self.one(self.results())
        self.assertEqual(row["before"]["valid_rounds"], 3)
        self.assertFalse(row["comparable"])
        self.assertEqual(row["before"]["failed_cases"], 1)

    def test_duplicate_round_is_not_a_complete_set(self):
        self.successful()
        path = self.before / "buffered-c1-r3-rtt0.json"
        record = json.loads(path.read_text())
        record["round"] = 2
        write(path, record)
        row = self.one(self.results())
        self.assertFalse(row["comparable"])
        self.assertEqual(row["before"]["missing_round_ids"], [3])

    def test_configuration_mismatch_does_not_invalidate_summary(self):
        self.successful()
        self.gateway(self.before, 1, warmup=0)
        results = self.results()
        self.assertIn("request/runtime configurations differ", self.one(results)["reasons"])
        self.assertTrue(self.one(results, kind="summary")["comparable"])

    def test_metadata_guards_reject_unverifiable_whole_run(self):
        for problem, mutate in (
            ("completion", lambda record: record.pop("finished_at_unix")),
            ("planned matrix", lambda record: record["arguments"].pop("concurrency")),
            ("planned rounds", lambda record: record["arguments"].update(rounds=4)),
            ("identical", lambda record: record.update(binary_sha256="b" * 64)),
            ("marked invalid", lambda record: record.update(valid_for_comparison=False)),
        ):
            with self.subTest(problem=problem):
                original = comparison.read_metadata(self.before)
                self.metadata(self.before, mutate)
                self.assertTrue(any(problem in reason for reason in comparison.build_reasons(self.before, self.after)))
                write(self.before / "metadata.json", original)

    def test_cli_writes_missing_groups_and_returns_nonzero(self):
        for side in (self.before, self.after):
            self.summary(side)
            write(side / "failures.json", {"failures": [self.failure(r) for r in (1, 2, 3)]})
        output = self.directory / "output"
        process = subprocess.run([sys.executable, str(Path(__file__).with_name("compare.py")),
                                  "--before", str(self.before), "--after", str(self.after),
                                  "--output", str(output)], capture_output=True, text=True)
        self.assertEqual(process.returncode, 2, process.stderr)
        counts = json.loads(process.stdout)
        self.assertEqual((counts["groups"], counts["comparable"], counts["excluded"]), (3, 1, 2))
        self.assertIn("before_missing_round_ids", (output / "comparison.csv").read_text())
        report = json.loads((output / "comparison.json").read_text())
        self.assertEqual(len(self.one(report["groups"])["before"]["failures"]), 3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
