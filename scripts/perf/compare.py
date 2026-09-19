#!/usr/bin/env python3
"""Compare sequential before/after run directories; emit median JSON and CSV.

python3 scripts/perf/compare.py --before target/perf-review/before \
    --after target/perf-review/after --output target/perf-review/comparison

Three complete, successful rounds per side are required by default. Planned
groups come from metadata.arguments, including groups with no result files.
Failed or incomplete groups retain their diagnostics and have no improvement percentages.
Positive improvement means higher throughput or lower latency. Medians of round
p95/p99 values are not pooled request percentiles. Review hardware/build metadata
separately; matching request/runtime configuration is checked here.
"""

import argparse
import csv
import json
import math
from pathlib import Path
import re
from statistics import median


METRICS = ("rps", "p95_ms", "p99_ms", "summary_ms", "first_page_ms", "last_page_ms")
CONFIG = ("requests", "warmup_requests", "workers", "pg_pool", "observer_pg_pool", "max_in_flight", "usage_persistence", "stream_chunk_delay_us", "quota_limit")


def finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value >= 0


def read_metadata(directory):
    path = directory / "metadata.json"
    return json.loads(path.read_text()) if path.is_file() else {}


def planned_groups(record):
    args = record.get("arguments")
    fields = ("scenarios", "concurrency", "storage_rtt_ms", "summary_rows")
    if not isinstance(args, dict) or any(not isinstance(args.get(field), list) for field in fields):
        raise ValueError("metadata.arguments must contain the complete planned matrix")
    if type(args.get("rounds")) is not int or args["rounds"] < 1:
        raise ValueError("metadata.arguments.rounds must be a positive integer")
    if any(value not in ("buffered", "stream", "convert") for value in args["scenarios"]):
        raise ValueError("metadata.arguments.scenarios is invalid")
    if any(type(value) is not int or value < 1 for value in args["concurrency"] + args["summary_rows"]):
        raise ValueError("metadata.arguments concurrency and summary rows must be positive integers")
    if any(not finite(value) for value in args["storage_rtt_ms"]):
        raise ValueError("metadata.arguments.storage_rtt_ms is invalid")
    groups = set()
    for rtt in args["storage_rtt_ms"]:
        for scenario in args["scenarios"]:
            for concurrency in args["concurrency"] if rtt == 0 else [128]:
                for target in ("gateway", "direct"):
                    groups.add(("gateway", scenario, concurrency, rtt, target, None))
    groups.update(("summary", None, None, None, None, count) for count in args["summary_rows"])
    return groups


def load_failures(directory):
    path = directory / "failures.json"
    records = json.loads(path.read_text()).get("failures", []) if path.is_file() else []
    groups = {}
    for failure in records:
        if failure.get("phase") == "gateway":
            keys = [("gateway", failure["scenario"], failure["concurrency"], failure["storage_rtt_ms"], target, None)
                    for target in ("gateway", "direct")]
        elif failure.get("phase") in ("init", "summary"):
            keys = [("summary", None, None, None, None, failure["rows"])]
        else:
            raise ValueError(f"unknown failed case phase in {path}")
        for key in keys:
            groups.setdefault(key, []).append(failure)
    return groups


def build_reasons(before, after, rounds=3):
    metadata = []
    reasons = []
    for side, directory in (("before", before), ("after", after)):
        record = read_metadata(directory)
        build = record.get("build_provenance") or record
        metadata.append(record)
        if record.get("valid_for_comparison") is False or build.get("valid_for_comparison") is False:
            reasons.append(f"{side} binary is marked invalid for comparison")
        if not record.get("binary_sha256"):
            reasons.append(f"{side} binary SHA256 is missing")
        try:
            planned_groups(record)
        except ValueError as error:
            reasons.append(f"{side} {error}")
        else:
            if record["arguments"]["rounds"] != rounds:
                reasons.append(f"{side} planned rounds differ from the required {rounds}")
        if not record.get("finished_at_unix"):
            reasons.append(f"{side} run has no completion marker")
    hashes = [record.get("binary_sha256") for record in metadata]
    if hashes[0] and hashes[0] == hashes[1]:
        reasons.append("before and after binary SHA256 values are identical")
    return reasons


def load(directory):
    groups = {}
    try:
        groups.update((key, []) for key in planned_groups(read_metadata(directory)))
    except ValueError:
        # build_reasons rejects comparison when the planned matrix is unavailable.
        pass
    for path in sorted(directory.glob("*.json")):
        report = json.loads(path.read_text())
        if not isinstance(report, dict):
            continue
        if report.get("mode") == "gateway":
            filename = re.search(r"-r(\d+)-rtt([\d.]+)$", path.stem)
            round_number = report.get("round", int(filename[1]) if filename else None)
            rtt = report.get("storage_rtt_ms", float(filename[2]) if filename else None)
            for target in ("gateway", "direct"):
                data = report.get(target, {})
                latency = data.get("successful_latency_ms") or {}
                metrics = {"rps": data.get("successful_rps"), "p95_ms": latency.get("p95"), "p99_ms": latency.get("p99")}
                errors = data.get("errors")
                reconciled = report.get("reconciliation", {}).get("ok") is True
                valid = report.get("ok") is True and not report.get("runner_failure") and reconciled and errors == 0 and data.get("completed") == report.get("requests") and all(finite(value) for value in metrics.values())
                key = ("gateway", report["scenario"], report["concurrency"], rtt, target, None)
                row = {"round": round_number, "valid": valid, "errors": errors, "reconciled": reconciled, "metrics": metrics,
                       "configuration": {key: report.get(key) for key in CONFIG}, "artifact": str(path)}
                groups.setdefault(key, []).append(row)
        elif report.get("mode") == "summary":
            key = ("summary", None, None, None, None, report["rows"])
            for sample in report.get("rounds", []):
                metrics = {key: sample.get(key) for key in ("summary_ms", "first_page_ms", "last_page_ms")}
                valid = report.get("ok") is True and not report.get("runner_failure") and sample.get("totals", {}).get("requests") == report["rows"] and all(finite(value) for value in metrics.values())
                groups.setdefault(key, []).append({"round": sample.get("round"), "valid": valid, "errors": None,
                    "reconciled": valid, "metrics": metrics, "configuration": {}, "artifact": str(path)})
    return groups


def summarize(rows, rounds, failures=()):
    good = [row for row in rows if row["valid"]]
    round_ids = [row["round"] for row in rows]
    complete = len(rows) == rounds and set(round_ids) == set(range(1, rounds + 1)) and len(good) == rounds and not failures
    medians = {metric: median(values) if (values := [row["metrics"][metric] for row in good if metric in row["metrics"]]) else None for metric in METRICS}
    error_counts = [row["errors"] for row in rows if isinstance(row["errors"], int)]
    return {"complete": complete, "observed_rounds": len(rows), "valid_rounds": len(good), "round_ids": round_ids,
            "missing_round_ids": sorted(set(range(1, rounds + 1)) - set(round_ids)),
            "failed_cases": len(failures), "failures": list(failures),
            "errors": sum(error_counts) if error_counts else None,
            "reconciliation_failures": sum(not row["reconciled"] for row in rows), "medians": medians,
            "invalid_artifacts": [row["artifact"] for row in rows if not row["valid"]]}


def compare(before, after, rounds=3, provenance_reasons=(), before_failures=None, after_failures=None):
    results = []
    before_failures, after_failures = before_failures or {}, after_failures or {}
    for key in sorted(before.keys() | after.keys() | before_failures.keys() | after_failures.keys(), key=repr):
        left_rows, right_rows = before.get(key, []), after.get(key, [])
        left = summarize(left_rows, rounds, before_failures.get(key, []))
        right = summarize(right_rows, rounds, after_failures.get(key, []))
        configurations = {json.dumps(row["configuration"], sort_keys=True) for row in left_rows + right_rows}
        reasons = list(provenance_reasons)
        if not left["complete"]:
            reasons.append(f"before requires exactly {rounds} successful distinct rounds")
        if not right["complete"]:
            reasons.append(f"after requires exactly {rounds} successful distinct rounds")
        for side, summary in (("before", left), ("after", right)):
            if summary["failed_cases"]:
                reasons.append(f"{side} recorded {summary['failed_cases']} failed case(s); see failures")
        if len(configurations) > 1:
            reasons.append("request/runtime configurations differ")
        improvements = {}
        for metric in METRICS:
            old, new = left["medians"][metric], right["medians"][metric]
            improvements[metric] = None
            if not reasons and old is not None and new is not None and old > 0:
                improvements[metric] = (new - old) / old * 100 * (1 if metric == "rps" else -1)
        results.append(dict(zip(("kind", "scenario", "concurrency", "storage_rtt_ms", "target", "rows"), key),
            comparable=not reasons, reasons=reasons, before=left, after=right, improvement_pct=improvements))
    return results


def write(output, results, before, after, rounds):
    output.mkdir(parents=True, exist_ok=True)
    report = {"schema_version": 1, "before": str(before), "after": str(after), "required_rounds": rounds,
        "method": "planned matrix from metadata.arguments; median of successful round metrics; incomplete or failed groups never receive improvement percentages",
        "improvement_sign": "positive means faster; round p95/p99 medians are not pooled request percentiles", "groups": results}
    (output / "comparison.json").write_text(json.dumps(report, indent=2) + "\n")
    fields = ["kind", "scenario", "concurrency", "storage_rtt_ms", "target", "rows", "comparable", "reasons"]
    diagnostics = ("observed_rounds", "valid_rounds", "missing_round_ids", "failed_cases", "errors", "reconciliation_failures")
    fields += [f"{side}_{field}" for side in ("before", "after") for field in diagnostics]
    fields += [f"{side}_{metric}" for metric in METRICS for side in ("before", "after", "improvement_pct")]
    with (output / "comparison.csv").open("w", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=fields)
        writer.writeheader()
        for result in results:
            row = {field: result[field] for field in fields[:7]}
            row["reasons"] = "; ".join(result["reasons"])
            for side in ("before", "after"):
                row.update({f"{side}_{field}": result[side][field] for field in diagnostics})
                row.update({f"{side}_{metric}": result[side]["medians"][metric] for metric in METRICS})
            row.update({f"improvement_pct_{metric}": result["improvement_pct"][metric] for metric in METRICS})
            writer.writerow(row)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--before", type=Path, required=True)
    parser.add_argument("--after", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("target/perf-review/comparison"))
    parser.add_argument("--rounds", type=int, default=3)
    args = parser.parse_args()
    if args.rounds < 1 or not args.before.is_dir() or not args.after.is_dir():
        parser.error("rounds must be positive and both input directories must exist")
    try:
        results = compare(load(args.before), load(args.after), args.rounds,
                          build_reasons(args.before, args.after, args.rounds),
                          load_failures(args.before), load_failures(args.after))
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.error(str(error))
    write(args.output, results, args.before, args.after, args.rounds)
    comparable = sum(result["comparable"] for result in results)
    print(json.dumps({"groups": len(results), "comparable": comparable, "excluded": len(results) - comparable,
                      "json": str(args.output / "comparison.json"), "csv": str(args.output / "comparison.csv")}))
    return 0 if results and comparable == len(results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
