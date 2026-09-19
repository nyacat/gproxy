#!/usr/bin/env python3
"""Run storage integration tests against disposable, loopback-only services."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import time
import uuid


ROOT = Path(__file__).resolve().parents[1]
TESTS = [
    ("gproxy-store", "backend::tests::usage_records::postgres_usage_pages_and_summary_index_work"),
    ("gproxy-store", "backend::tests::quota_activity::postgres_activity_migration_retry_identity_and_atomic_settlement"),
    ("gproxy-store", "backend::tests::credential_health::postgres_credential_health_backoff_and_ordering"),
    ("gproxy-store", "backend::tests::cycle_reads::postgres_long_gap_estimates_and_index_work"),
    ("gproxy-store", "backend::tests::parity::postgres_schema_queries_and_rollback"),
    ("gproxy-store", "backend::tests::quota_migration::postgres_legacy_local_schema_completes_upstream_migrations"),
    ("gproxy-store", "backend::tests::quota_migration::branch_history::postgres::postgres_self_v10_upgrade"),
    ("gproxy-store", "backend::tests::quota_migration::branch_history::postgres::postgres_self_v11_upgrade"),
    ("gproxy-store", "backend::tests::quota_migration::branch_history::postgres::postgres_self_v12_upgrade"),
    ("gproxy-store", "backend::tests::quota_migration::branch_history::postgres::postgres_self_v12_retries_partial_snapshot_reconciliation"),
    ("gproxy-store", "backend::tests::quota_migration::branch_history::postgres::postgres_main_v10_upgrade_preserves_snapshots"),
    ("gproxy-store", "backend::tests::quota_migration::branch_history::postgres::postgres_v13_history_index_upgrade_retries_partial_ddl"),
    ("gproxy-store", "backend::postgres::tests::postgres_statement_cache_bounds_server_resources_and_reprepares_evicted_sql"),
    ("gproxy-store", "backend::postgres::tests::postgres_cancelled_transaction_releases_its_server_lock"),
    ("gproxy-store", "backend::tests::scenario::cycle::regression::postgres_concurrent_rebuild_preserves_every_linked_usage"),
    ("gproxy-store", "backend::tests::settlement_recovery::postgres_settlement_replay_migration_progress_completion_and_pagination"),
    ("gproxy-store", "backend::tests::quota_contention::postgres_same_window_settlements_survive_concurrency"),
    ("gproxy-store", "backend::tests::cycle_reads::postgres_cycle_statistics_share_reads"),
    ("gproxy-store", "backend::tests::cycle_page::postgres_history_pages_past_window_cap_preserve_filters_scope_and_equal_timestamp_rows"),
    ("gproxy-store", "backend::tests::cache::redis_cache_operations_are_atomic"),
    ("gproxy-app", "tests::credential_budget::postgres_failover_and_cache_recovery_keep_exact_spend"),
    ("gproxy-app", "tests::postgres_redis::postgres_redis_preserve_usage_identity_and_settle_concurrent_requests_once"),
]


def run(args, **kwargs):
    return subprocess.run(args, cwd=ROOT, check=True, text=True, **kwargs)


def output(args):
    return run(args, capture_output=True).stdout.strip()


def ready(args):
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        result = subprocess.run(args, cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if result.returncode == 0:
            return
        time.sleep(0.2)
    raise RuntimeError("isolated test service did not become ready")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--postgres-image", default="postgres:18.6", help="isolated PostgreSQL image (production major/minor by default)")
    parser.add_argument("--only", help="run tests whose names contain this string")
    parser.add_argument("--benchmarks", action="store_true", help="include the opt-in production-sized local benchmark")
    parser.add_argument("--jobs", type=int, default=2)
    parser.add_argument("--output", type=Path, default=ROOT / "target/perf-review/integration.json")
    args = parser.parse_args()
    available = TESTS + ([("gproxy-store", "backend::tests::cycle_reads::postgres_production_scale_statistics_benchmark")] if args.benchmarks else [])
    selected = [(package, test) for package, test in available if not args.only or args.only in test]
    if not selected:
        parser.error("--only did not match a test")
    if args.jobs < 1:
        parser.error("--jobs must be positive")
    suffix = uuid.uuid4().hex[:12]
    pg = f"gproxy-test-pg-{suffix}"
    redis = f"gproxy-test-redis-{suffix}"
    password = "isolated-test-password"
    started = []
    report = {"tests": [], "postgres_image": args.postgres_image, "redis_image": "redis:7"}
    exit_code = 0
    try:
        for name, command in [
            (pg, ["docker", "run", "--rm", "-d", "--name", pg, "-e", f"POSTGRES_PASSWORD={password}", "-p", "127.0.0.1::5432", args.postgres_image]),
            (redis, ["docker", "run", "--rm", "-d", "--name", redis, "-p", "127.0.0.1::6379", "redis:7", "redis-server", "--save", "", "--appendonly", "no"]),
        ]:
            run(command, stdout=subprocess.DEVNULL)
            started.append(name)
        ready(["docker", "exec", pg, "pg_isready", "-U", "postgres"])
        ready(["docker", "exec", redis, "redis-cli", "ping"])
        pg_port = output(["docker", "port", pg, "5432/tcp"]).rsplit(":", 1)[1]
        redis_port = output(["docker", "port", redis, "6379/tcp"]).rsplit(":", 1)[1]
        report["postgres_version"] = output(["docker", "exec", pg, "postgres", "--version"])
        report["redis_version"] = output(["docker", "exec", redis, "redis-server", "--version"])
        for index, (package, test) in enumerate(selected):
            database = f"review_{index}"
            run(["docker", "exec", pg, "createdb", "-U", "postgres", database])
            # Only this runner's private Redis container is reset.
            run(["docker", "exec", redis, "redis-cli", "FLUSHDB"], stdout=subprocess.DEVNULL)
            dsn = f"postgres://postgres:{password}@127.0.0.1:{pg_port}/{database}"
            env = os.environ.copy()
            env.update({
                "CARGO_BUILD_JOBS": str(args.jobs),
                "GPROXY_TEST_POSTGRES_DSN": dsn,
                "GPROXY_TEST_POSTGRES_CYCLE_DSN": dsn,
                "GPROXY_TEST_APP_POSTGRES_DSN": dsn,
                "GPROXY_TEST_REDIS_URL": f"redis://127.0.0.1:{redis_port}/0",
            })
            print(f"Running {package}: {test}", flush=True)
            start = time.monotonic()
            result = subprocess.run(
                ["cargo", "test", "-p", package, "--lib", test, "--", "--exact", "--ignored", "--test-threads=1", "--nocapture"],
                cwd=ROOT, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            )
            print(result.stdout, end="", flush=True)
            passed = result.returncode == 0 and "test result: ok. 1 passed;" in result.stdout
            report["tests"].append({"package": package, "name": test, "exit_code": result.returncode, "passed": passed, "elapsed_seconds": time.monotonic() - start, "output": result.stdout})
            if not passed:
                exit_code = 1
    finally:
        for name in reversed(started):
            subprocess.run(["docker", "rm", "-f", name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
