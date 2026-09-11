#!/usr/bin/env python3
"""Build and run the local gateway/usage matrix, writing only JSON/CSV artifacts.

Examples:
  python3 scripts/perf/run.py --label before-build --revision HEAD --prepare-only
  python3 scripts/perf/run.py --label after-build --prepare-only
  python3 scripts/perf/run.py --label smoke --binary target/perf-review/bin/after-build --smoke
  python3 scripts/perf/run.py --label after --binary target/perf-review/bin/after-build --storage-rtt-ms 0,2,10 --summary-rows 10000,100000,1000000

Each invocation owns its PostgreSQL and Redis containers. Every case gets a fresh
database; only this invocation's Redis is flushed. No supplied service is altered.
Run comparisons sequentially with identical flags and no concurrent builds/tests.
"""

import argparse
import asyncio
import contextlib
import csv
import fcntl
import hashlib
import io
import json
import math
import os
from pathlib import Path
import platform
import re
import secrets
import shutil
import subprocess
import tarfile
import tempfile
import time


ROOT = Path(__file__).resolve().parents[2]
EXAMPLE = Path("crates/gproxy-host-axum/examples/performance.rs")
SOURCE_SUFFIXES = {".rs", ".c", ".h", ".cc", ".cpp", ".hpp", ".py", ".js", ".mjs", ".cjs", ".ts", ".tsx", ".jsx", ".css", ".scss", ".html", ".sh", ".bash", ".ps1", ".sql", ".java", ".kt", ".xml", ".in", ".toml", ".lock"}


def documentation(path):
    return (
        path.suffix.lower() in {".md", ".mdx", ".rst"}
        or any(part.lower() in {"docs", "doc", "documentation", ".agents", ".codex", ".codegraph"} for part in path.parts)
        or path.name.lower().startswith(("readme", "changelog", "agents."))
    )


def source_input(path):
    if documentation(path):
        return False
    return (
        path.suffix.lower() in SOURCE_SUFFIXES
        or path.name in {"Dockerfile", "Makefile", "Justfile", "package.json", "pnpm-workspace.yaml", "pnpm-lock.yaml", "rust-toolchain"}
        or path.name.startswith("tsconfig") and path.suffix == ".json"
        or path.parts[0] in {"scripts", ".github", ".cargo"} and path.suffix in {".json", ".yml", ".yaml"}
    )


def working_source_files():
    names = command(["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], cwd=ROOT).stdout.split(b"\0")
    return sorted({os.fsdecode(name) for name in names if name and source_input(Path(os.fsdecode(name)))})


def source_manifest(directory, names):
    """Fingerprint selected inputs, including untracked files and tracked deletions.

    Hash sorted JSON entries with compact separators and sorted object keys.
    Symlinks fingerprint their target text; they are never followed into documents
    or outside the checkout. Ignored build artifacts and dependencies are excluded.
    """
    entries = []
    for name in sorted(set(names)):
        relative = Path(name)
        if not source_input(relative):
            continue
        path = directory / relative
        if path.is_symlink():
            payload = os.fsencode(os.readlink(path))
            entry = {"path": relative.as_posix(), "kind": "symlink", "size": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}
        elif path.is_file():
            payload = path.read_bytes()
            entry = {"path": relative.as_posix(), "kind": "file", "size": len(payload), "executable": bool(path.stat().st_mode & 0o111), "sha256": hashlib.sha256(payload).hexdigest()}
        elif not path.exists():
            entry = {"path": relative.as_posix(), "kind": "missing"}
        else:
            continue
        entries.append(entry)
    canonical = json.dumps(entries, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    return {"schema_version": 1, "sha256": hashlib.sha256(canonical).hexdigest(), "files": len(entries), "entries": entries}


def save_source_manifest(artifacts, manifest, selection):
    path = artifacts / "source-manifest.json"
    path.write_text(json.dumps(manifest, indent=2) + "\n")
    return {"sha256": manifest["sha256"], "files": manifest["files"], "path": str(path), "selection": selection}


def command(arguments, *, cwd=None, env=None, check=True, data=None):
    result = subprocess.run(arguments, cwd=cwd, env=env, input=data, capture_output=True)
    if check and result.returncode:
        # Avoid rendering command arguments: connection credentials can be present.
        raise RuntimeError(result.stderr.decode(errors="replace")[-8000:])
    return result


def output(arguments, **kwargs):
    return command(arguments, **kwargs).stdout.decode().strip()


@contextlib.contextmanager
def build_lock(target):
    target.mkdir(parents=True, exist_ok=True)
    # Serialize this runner's clean/build/copy sequence across checkout paths.
    # Cargo's own lock covers individual commands, not the sequence between them.
    with (target / ".gproxy-perf-build.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def source_files(revision):
    names = command(["git", "ls-tree", "-r", "--name-only", "-z", revision], cwd=ROOT).stdout.split(b"\0")
    for name in names:
        if not name:
            continue
        path = Path(os.fsdecode(name))
        if documentation(path):
            continue
        yield str(path)


def snapshot(revision, destination):
    files = list(source_files(revision))
    archive = command(["git", "archive", "--format=tar", revision, "--", *files], cwd=ROOT).stdout
    with tarfile.open(fileobj=io.BytesIO(archive)) as source:
        # Git paths are local; filter additionally rejects escaping links/paths.
        source.extractall(destination, filter="data")
    (destination / EXAMPLE).parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(ROOT / EXAMPLE, destination / EXAMPLE)


def hardware():
    details = {"platform": platform.platform(), "logical_cpus": os.cpu_count()}
    for key, path in {
        "cpuinfo": "/proc/cpuinfo", "memory": "/proc/meminfo",
        "cgroup_cpu_max": "/sys/fs/cgroup/cpu.max", "cgroup_memory_max": "/sys/fs/cgroup/memory.max",
    }.items():
        try:
            value = Path(path).read_text()
            if key == "cpuinfo":
                value = next((line for line in value.splitlines() if line.startswith("model name")), "unknown")
            details[key] = value.strip()
        except OSError:
            details[key] = None
    if hasattr(os, "sched_getaffinity"):
        details["cpu_affinity"] = sorted(os.sched_getaffinity(0))
    return details


def prepare(args, artifacts):
    meta = {
        "label": args.label, "revision": output(["git", "rev-parse", args.revision or "HEAD"], cwd=ROOT),
        "source": "clean revision plus identical performance example" if args.revision else "working tree",
        "hardware": hardware(), "arguments": vars(args),
        "rustc": output(["rustc", "-Vv"]),
        "started_at_unix": time.time(),
        "build_environment": {key: value for key, value in os.environ.items() if key.startswith("CARGO_PROFILE_") or key in {"RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "CARGO_BUILD_JOBS", "CARGO_BUILD_TARGET"}},
        "measurement_limits": [
            "The gateway, mock and load generator share a process and runtime; CPU/RSS include all three.",
            "Fixed-concurrency closed-loop requests have no coordinated-omission correction.",
            "Database/Redis counters cover the full case lifecycle, including setup, warmup and reconciliation.",
            "Injected latency delays each TCP read chunk in both directions; it is not a production network measurement.",
            "Public APIs do not expose pool checkout wait, allocations or per-request stage timings.",
        ],
    }
    if not args.revision:
        diff = command(["git", "diff", "HEAD", "--", *working_source_files()], cwd=ROOT).stdout
        meta["working_diff_sha256"] = hashlib.sha256(diff).hexdigest()
    if args.binary:
        binary = Path(args.binary).resolve()
        if not binary.is_file():
            raise RuntimeError(f"missing benchmark binary: {binary}")
        provenance = Path(str(binary) + ".json")
        meta["source"] = "prebuilt binary"
        meta["build_provenance"] = json.loads(provenance.read_text()) if provenance.is_file() else None
        if meta["build_provenance"] and meta["build_provenance"].get("valid_for_comparison") is False:
            raise RuntimeError("prebuilt benchmark is marked invalid: " + meta["build_provenance"].get("invalid_reason", "unspecified reason"))
        if meta["build_provenance"] and meta["build_provenance"].get("binary_sha256") != hashlib.sha256(binary.read_bytes()).hexdigest():
            raise RuntimeError("prebuilt binary differs from its recorded build SHA256")
    else:
        target = Path(args.target_dir).resolve()
        with build_lock(target), tempfile.TemporaryDirectory(prefix="gproxy-perf-source-") as directory:
            source = Path(directory) if args.revision else ROOT
            if args.revision:
                snapshot(args.revision, source)
            names = lambda: list(source_files(args.revision)) + [str(EXAMPLE)] if args.revision else working_source_files()
            manifest = source_manifest(source, names())
            selection = "revision source/manifest/build inputs plus current performance.rs" if args.revision else "git ls-files --cached --others --exclude-standard; source/manifest/build inputs and harness; documents excluded"
            meta["source_manifest"] = save_source_manifest(artifacts, manifest, selection)
            env = dict(os.environ, CARGO_TARGET_DIR=str(target))
            # Cargo may consider identical package IDs fresh across snapshot
            # paths because restored Git mtimes precede cached fingerprints.
            # Preserve third-party dependencies but invalidate every workspace
            # package in this profile before building the selected source.
            clean = command(["cargo", "clean", "--locked", "--workspace", "--profile", args.profile, "--target-dir", str(target)], cwd=source, env=env, check=False)
            (artifacts / "clean.log").write_bytes(clean.stdout + clean.stderr)
            if clean.returncode:
                raise RuntimeError(f"workspace cache cleanup failed; inspect {artifacts / 'clean.log'}")
            meta["build_cache_policy"] = "workspace packages cleaned for selected profile; third-party dependency cache retained; runner clean/build/copy lock held"
            print(f"Building {args.label} ({args.profile}); compiler output goes to the build log", flush=True)
            build = command(["cargo", "build", "--locked", "-p", "gproxy-host-axum", "--example", "performance", "--profile", args.profile, "-j", str(args.jobs)], cwd=source, env=env, check=False)
            (artifacts / "build.log").write_bytes(build.stdout + build.stderr)
            if build.returncode:
                raise RuntimeError(f"build failed; inspect {artifacts / 'build.log'}")
            if not re.search(rb"Compiling gproxy-host-axum\b", build.stdout + build.stderr):
                raise RuntimeError("build did not report compiling gproxy-host-axum after workspace cleanup")
            unchanged = source_manifest(source, names())["sha256"] == manifest["sha256"]
            meta["source_manifest"]["unchanged_during_build"] = unchanged
            if not unchanged:
                (artifacts / "metadata.json").write_text(json.dumps(meta, indent=2) + "\n")
                raise RuntimeError("selected source inputs changed during compilation; build again after edits stop")
            profile_directory = "debug" if args.profile == "dev" else args.profile
            binary = artifacts.parent / "bin" / args.label
            binary.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(target / profile_directory / "examples" / "performance", binary)
            meta["valid_for_comparison"] = True
    meta["binary"] = str(binary)
    meta["binary_sha256"] = hashlib.sha256(binary.read_bytes()).hexdigest()
    if not args.binary:
        Path(str(binary) + ".json").write_text(json.dumps(meta, indent=2) + "\n")
    (artifacts / "metadata.json").write_text(json.dumps(meta, indent=2) + "\n")
    return binary, meta


class Services:
    def __init__(self, args):
        suffix = secrets.token_hex(5)
        self.pg = "gproxy-perf-pg-" + suffix
        self.redis = "gproxy-perf-redis-" + suffix
        self.password = secrets.token_hex(20)
        self.args = args
        self.started = []

    async def start(self):
        for name, image, port, rest in [
            (self.pg, self.args.postgres_image, 5432, ["-e", "POSTGRES_PASSWORD=" + self.password]),
            (self.redis, self.args.redis_image, 6379, []),
        ]:
            arguments = ["docker", "run", "--rm", "-d", "--name", name, "-p", f"127.0.0.1::{port}"]
            if self.args.service_cpus:
                arguments += ["--cpus", str(self.args.service_cpus)]
            if self.args.service_memory:
                arguments += ["--memory", self.args.service_memory]
            arguments += rest + [image]
            if name == self.pg:
                arguments += ["postgres", "-c", "max_connections=256", "-c", "shared_preload_libraries=pg_stat_statements"]
            else:
                arguments += ["redis-server", "--save", "", "--appendonly", "no"]
            await asyncio.to_thread(command, arguments)
            self.started.append(name)
        for _ in range(200):
            pg = await asyncio.to_thread(command, ["docker", "exec", self.pg, "pg_isready", "-U", "postgres"], check=False)
            redis = await asyncio.to_thread(command, ["docker", "exec", self.redis, "redis-cli", "ping"], check=False)
            if pg.returncode == redis.returncode == 0:
                break
            await asyncio.sleep(0.1)
        else:
            raise RuntimeError("isolated storage did not become ready")
        self.pg_port = int(output(["docker", "port", self.pg, "5432/tcp"]).rsplit(":", 1)[1])
        self.redis_port = int(output(["docker", "port", self.redis, "6379/tcp"]).rsplit(":", 1)[1])

    def psql(self, database, sql):
        return output(["docker", "exec", "-i", self.pg, "psql", "-X", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", database], data=sql.encode())

    def fresh(self, database):
        command(["docker", "exec", self.pg, "createdb", "-U", "postgres", database])
        self.psql(database, "CREATE EXTENSION pg_stat_statements;")
        command(["docker", "exec", self.redis, "redis-cli", "FLUSHDB"])

    def stats(self, database):
        postgres = self.psql(database, "SELECT json_build_object('calls',coalesce(sum(calls),0),'execution_ms',coalesce(sum(total_exec_time),0),'rows',coalesce(sum(rows),0)) FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database());")
        redis = output(["docker", "exec", self.redis, "redis-cli", "INFO", "commandstats"])
        commands = {}
        for line in redis.splitlines():
            if not line.startswith("cmdstat_"):
                continue
            name, values = line.split(":", 1)
            fields = dict(value.split("=", 1) for value in values.split(","))
            commands[name.removeprefix("cmdstat_")] = {key: float(value) for key, value in fields.items()}
        return {"postgres": json.loads(postgres), "redis": commands}

    def metadata(self):
        return {
            "postgres_version": self.psql("postgres", "SELECT version();"),
            "redis_version": output(["docker", "exec", self.redis, "redis-server", "--version"]),
            "images": {name: json.loads(output(["docker", "inspect", name, "--format", "{{json .Image}} "])) for name in self.started},
            "postgres_settings": self.psql("postgres", "SELECT json_object_agg(name,setting) FROM pg_settings WHERE name IN ('max_connections','shared_buffers','fsync','synchronous_commit','full_page_writes');"),
            "storage": "isolated Docker containers; PostgreSQL durable defaults, ephemeral test volume; Redis persistence disabled",
        }

    def close(self):
        for name in reversed(self.started):
            command(["docker", "rm", "-f", "-v", name], check=False)


class DelayProxy:
    """Delay chunks at arrival + fixed one-way latency, without serial sleeps.

    A separate receiver queues timestamps so sustained throughput does not pay a
    fresh delay after every write. Queue backpressure is bounded to 8 MiB/flow.
    """
    def __init__(self, destination, milliseconds):
        self.destination = destination
        self.delay = milliseconds / 2000
        self.connections = set()

    async def __aenter__(self):
        self.server = await asyncio.start_server(self.connect, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        return self.port

    async def connect(self, reader, writer):
        task = asyncio.current_task()
        self.connections.add(task)
        peer = None
        transfers = []
        try:
            remote, peer = await asyncio.open_connection("127.0.0.1", self.destination)
            transfers = [asyncio.create_task(self.pipe(reader, peer)), asyncio.create_task(self.pipe(remote, writer))]
            await asyncio.gather(*transfers)
        except (OSError, asyncio.CancelledError):
            pass
        finally:
            for transfer in transfers:
                transfer.cancel()
            await asyncio.gather(*transfers, return_exceptions=True)
            writer.close()
            if peer:
                peer.close()
            self.connections.discard(task)

    async def pipe(self, reader, writer):
        queue = asyncio.Queue(128)

        async def receive():
            try:
                while True:
                    chunk = await reader.read(65536)
                    await queue.put((asyncio.get_running_loop().time() + self.delay, chunk))
                    if not chunk:
                        return
            except OSError as error:
                await queue.put((asyncio.get_running_loop().time(), error))

        receive_task = asyncio.create_task(receive())
        try:
            while True:
                deadline, chunk = await queue.get()
                await asyncio.sleep(max(0, deadline - asyncio.get_running_loop().time()))
                if isinstance(chunk, OSError):
                    raise chunk
                if not chunk:
                    if writer.can_write_eof():
                        writer.write_eof()
                    return
                writer.write(chunk)
                await writer.drain()
        finally:
            receive_task.cancel()
            await asyncio.gather(receive_task, return_exceptions=True)

    async def __aexit__(self, *unused):
        self.server.close()
        await self.server.wait_closed()
        connections = list(self.connections)
        for task in connections:
            task.cancel()
        await asyncio.gather(*connections, return_exceptions=True)


def environment(args, services, database, directory, pg_port, redis_port):
    env = {key: value for key, value in os.environ.items() if not key.startswith(("GPROXY_", "PERF_", "UPSTASH_"))}
    env.update({
        "GPROXY_PERSISTENCE": "postgres", "GPROXY_DSN": f"postgres://postgres:{services.password}@127.0.0.1:{pg_port}/{database}",
        "GPROXY_REDIS_URL": f"redis://127.0.0.1:{redis_port}/0", "GPROXY_PORT": "0", "GPROXY_HOST": "127.0.0.1",
        "GPROXY_DATA_DIR": str(directory), "GPROXY_PG_POOL": str(args.pg_pool), "GPROXY_MAX_IN_FLIGHT": "1024",
        "GPROXY_ADMIN_PASSWORD": "local-performance-fixture", "GPROXY_BOOTSTRAP_CHANNELS": "",
        "GPROXY_MASTER_KEY": "AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI=", "RUST_LOG": "off",
        "PERF_WORKERS": str(args.workers), "PERF_CLOCK_TICKS": str(os.sysconf("SC_CLK_TCK")),
        "PERF_STREAM_DELAY_US": str(args.stream_delay_us), "PERF_ROUNDS": str(args.rounds),
        "PERF_QUOTA_LIMIT": args.quota_limit,
    })
    return env


class BenchmarkFailure(RuntimeError):
    def __init__(self, message, return_code=None):
        super().__init__(message)
        self.return_code = return_code


async def invoke(binary, env, cwd, artifact, timeout):
    env = dict(env, PERF_OUTPUT=str(artifact))
    process = await asyncio.create_subprocess_exec(str(binary), env=env, cwd=cwd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    communication = asyncio.create_task(process.communicate())
    try:
        stdout, stderr = await asyncio.wait_for(asyncio.shield(communication), timeout)
    except BaseException as error:
        if process.returncode is None:
            process.kill()
        stdout, stderr = await communication
        artifact.with_suffix(".log").write_bytes(stdout + stderr)
        if isinstance(error, TimeoutError):
            raise BenchmarkFailure(f"benchmark exceeded {timeout} seconds; inspect {artifact.with_suffix('.log')}", process.returncode) from error
        raise
    artifact.with_suffix(".log").write_bytes(stdout + stderr)
    if process.returncode:
        raise BenchmarkFailure(f"benchmark exited with status {process.returncode}; inspect {artifact.with_suffix('.log')}", process.returncode)
    try:
        result = json.loads(artifact.read_text())
        if not isinstance(result, dict):
            raise ValueError("result is not an object")
        return result
    except (OSError, ValueError) as error:
        raise BenchmarkFailure(f"benchmark did not produce a valid result: {artifact}", process.returncode) from error


def save_failures(artifacts, label, failures):
    (artifacts / "failures.json").write_text(json.dumps({"schema_version": 1, "label": label, "failures": failures}, indent=2) + "\n")


async def execute_case(args, binary, env, cwd, artifact, failures, case):
    try:
        return await invoke(binary, env, cwd, artifact, args.case_timeout)
    except BenchmarkFailure as error:
        failure = {**case, "error": str(error), "return_code": error.return_code,
                   "log": str(artifact.with_suffix(".log")), "result": str(artifact) if artifact.is_file() else None}
        result = None
        if artifact.is_file():
            try:
                result = json.loads(artifact.read_text())
                if not isinstance(result, dict):
                    result = None
            except (OSError, ValueError) as read_error:
                failure["result_read_error"] = str(read_error)
        failures.append(failure)
        save_failures(artifact.parent, args.label, failures)
        if not args.continue_on_error:
            raise
        print(f"Recorded failed case {case['case']}; continuing", flush=True)
        if result is not None:
            # Preserve actual request errors, latencies and reconciliation. A
            # failed process cannot become a successful comparison sample.
            result["runner_failure"] = failure
        return result


def write_csv(artifacts, reports):
    fields = ["label", "scenario", "concurrency", "round", "storage_rtt_ms", "target", "completed", "errors", "successful_rps", "p50_ms", "p95_ms", "p99_ms", "first_event_p95_ms", "cpu_ms", "peak_rss_kib", "drain_ms", "reconciliation_ok"]
    with (artifacts / "gateway.csv").open("w", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=fields)
        writer.writeheader()
        for report in reports:
            if report["mode"] != "gateway":
                continue
            for name in ("direct", "gateway"):
                data = report[name]
                latency = data["successful_latency_ms"] or {}
                writer.writerow({
                    "label": report["label"], "scenario": report["scenario"], "concurrency": report["concurrency"], "round": report["round"],
                    "storage_rtt_ms": report["storage_rtt_ms"], "target": name, "completed": data["completed"], "errors": data["errors"],
                    "successful_rps": data["successful_rps"], "p50_ms": latency.get("p50"), "p95_ms": latency.get("p95"), "p99_ms": latency.get("p99"),
                    "first_event_p95_ms": (data["first_event_ms"] or {}).get("p95"), "cpu_ms": data["cpu_ms"],
                    "peak_rss_kib": data["resources_after"]["process_peak_rss_kib"], "drain_ms": report["reconciliation"]["drain_ms"],
                    "reconciliation_ok": report["reconciliation"]["ok"],
                })
    with (artifacts / "summary.csv").open("w", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=["label", "rows", "round", "summary_ms", "first_page_ms", "last_page_ms"])
        writer.writeheader()
        for report in reports:
            if report["mode"] == "summary":
                for sample in report["rounds"]:
                    writer.writerow({"label": report["label"], "rows": report["rows"], **{key: sample[key] for key in ("round", "summary_ms", "first_page_ms", "last_page_ms")}})


async def measure(args, binary, artifacts, meta):
    services = Services(args)
    reports = []
    failures = []
    save_failures(artifacts, args.label, failures)
    try:
        await services.start()
        meta["services"] = services.metadata()
        (artifacts / "metadata.json").write_text(json.dumps(meta, indent=2) + "\n")
        serial = 0
        for rtt in args.storage_rtt_ms:
            async with contextlib.AsyncExitStack() as stack:
                pg_port, redis_port = services.pg_port, services.redis_port
                if rtt:
                    pg_port = await stack.enter_async_context(DelayProxy(pg_port, rtt))
                    redis_port = await stack.enter_async_context(DelayProxy(redis_port, rtt))
                concurrency_values = args.concurrency if rtt == 0 else [128]
                for scenario in args.scenarios:
                    for concurrency in concurrency_values:
                        for round_number in range(1, args.rounds + 1):
                            serial += 1
                            database = f"perf_{serial}"
                            services.fresh(database)
                            name = f"{scenario}-c{concurrency}-r{round_number}-rtt{rtt:g}"
                            artifact = artifacts / f"{name}.json"
                            try:
                                with tempfile.TemporaryDirectory(prefix="gproxy-perf-data-") as directory:
                                    env = environment(args, services, database, directory, pg_port, redis_port)
                                    env.update(PERF_SCENARIO=scenario, PERF_CONCURRENCY=str(concurrency), PERF_REQUESTS=str(max(args.requests, concurrency * args.requests_per_worker)), PERF_WARMUP=str(max(args.warmup, concurrency) if args.warmup else 0))
                                    before = services.stats(database)
                                    print(f"Measuring {args.label}: {name}", flush=True)
                                    report = await execute_case(args, binary, env, directory, artifact, failures,
                                        {"case": name, "phase": "gateway", "scenario": scenario, "concurrency": concurrency, "round": round_number, "storage_rtt_ms": rtt})
                                    after = services.stats(database)
                                if report is not None:
                                    report.update(label=args.label, round=round_number, storage_rtt_ms=rtt, lifecycle_storage_counters={"before": before, "after": after})
                                    artifact.write_text(json.dumps(report, indent=2) + "\n")
                                    reports.append(report)
                                    write_csv(artifacts, reports)
                            finally:
                                services.psql("postgres", f'DROP DATABASE "{database}" WITH (FORCE);')
        for count in args.summary_rows:
            serial += 1
            database = f"perf_{serial}"
            services.fresh(database)
            artifact = artifacts / f"summary-{count}.json"
            try:
                with tempfile.TemporaryDirectory(prefix="gproxy-perf-data-") as directory:
                    env = environment(args, services, database, directory, services.pg_port, services.redis_port)
                    initialized = await execute_case(args, binary, dict(env, PERF_MODE="init"), directory,
                        artifacts / f"init-{count}.json", failures, {"case": f"summary-{count}", "phase": "init", "rows": count})
                    if initialized is None or initialized.get("runner_failure"):
                        continue
                    services.psql(database, f"""
                    INSERT INTO usage_rows (request_id,at,provider_id,credential_id,user_id,user_key_id,operation,upstream_model,input_tokens,output_tokens,cached_input_tokens,metrics_json,dimensions_json,cost,usage_source,ended,latency_ms)
                    SELECT 'fixture-'||n, 1700000000+n%86400, 1,1,1,1,'generate_content','upstream-model',10,5,0,
                        '{{"input_tokens":"10","output_tokens":"5"}}',json_build_object('fixture',repeat('x',512))::text,'0.00002','upstream','complete',10
                    FROM generate_series(1,{count}) AS n;
                    ANALYZE usage_rows;
                    """)
                    before = services.stats(database)
                    print(f"Measuring {args.label}: usage summary with {count} rows", flush=True)
                    report = await execute_case(args, binary, dict(env, PERF_MODE="summary"), directory,
                        artifact, failures, {"case": f"summary-{count}", "phase": "summary", "rows": count})
                    after = services.stats(database)
                    if report is not None:
                        report.update(label=args.label, lifecycle_storage_counters={"before": before, "after": after})
                        artifact.write_text(json.dumps(report, indent=2) + "\n")
                        reports.append(report)
                        write_csv(artifacts, reports)
            finally:
                services.psql("postgres", f'DROP DATABASE "{database}" WITH (FORCE);')
        meta["finished_at_unix"] = time.time()
        meta["completed_cases"] = len(reports)
        meta["failed_cases"] = len(failures)
        meta["successful_cases"] = sum(report.get("ok") is True and not report.get("runner_failure") for report in reports)
        (artifacts / "metadata.json").write_text(json.dumps(meta, indent=2) + "\n")
        return not failures
    finally:
        services.close()


def numbers(value, kind=int):
    return [kind(part) for part in value.split(",") if part]


def arguments():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--label", required=True)
    parser.add_argument("--revision", help="build a clean git revision, excluding project documentation, with the current harness")
    parser.add_argument("--binary", help="use a previously built performance example")
    parser.add_argument("--prepare-only", action="store_true", help="build/copy the binary and record metadata without starting services")
    parser.add_argument("--output", default=str(ROOT / "target/perf-review"))
    parser.add_argument("--target-dir", default=str(ROOT / "target/perf-review/build"))
    parser.add_argument("--profile", default="release")
    parser.add_argument("--jobs", type=int, default=2)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--pg-pool", type=int, default=32)
    parser.add_argument("--quota-limit", default="1000", help="fixture daily and total quota; must exceed expected usage cost")
    parser.add_argument("--concurrency", default="1,32,128,512,1024")
    parser.add_argument("--scenarios", default="buffered,stream,convert")
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--requests-per-worker", type=int, default=10, help="minimum requests/concurrency; total is max(requests, concurrency*this)")
    parser.add_argument("--warmup", type=int, default=100, help="minimum warmup requests; raised to concurrency, or 0 to disable")
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--storage-rtt-ms", default="0", help="extra bidirectional TCP delay; nonzero cases use concurrency 128")
    parser.add_argument("--stream-delay-us", type=int, default=0)
    parser.add_argument("--summary-rows", default="", help="optional comma-separated 10000,100000,1000000")
    parser.add_argument("--case-timeout", type=float, default=600)
    parser.add_argument("--continue-on-error", action="store_true", help="record failed cases and continue the matrix; still exit nonzero when any case failed")
    parser.add_argument("--postgres-image", default="postgres:17")
    parser.add_argument("--redis-image", default="redis:7")
    parser.add_argument("--service-cpus", type=float)
    parser.add_argument("--service-memory")
    parser.add_argument("--smoke", action="store_true", help="all three paths at concurrency 1, two warmups and ten measured requests")
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", args.label):
        parser.error("--label must contain 1-64 letters, numbers, underscores or hyphens")
    args.concurrency = numbers(args.concurrency)
    args.scenarios = [value for value in args.scenarios.split(",") if value]
    args.storage_rtt_ms = numbers(args.storage_rtt_ms, float)
    args.summary_rows = numbers(args.summary_rows)
    if args.smoke:
        args.concurrency, args.requests, args.requests_per_worker, args.warmup, args.rounds, args.storage_rtt_ms = [1], 10, 1, 2, 1, [0]
    if min(args.concurrency + [args.requests, args.rounds, args.workers, args.jobs]) < 1 or args.warmup < 0 or args.requests_per_worker < 0:
        parser.error("counts must be positive, with nonnegative warmup and requests-per-worker")
    if any(value not in {"buffered", "stream", "convert"} for value in args.scenarios) or any(value < 0 for value in args.storage_rtt_ms) or any(value < 1 for value in args.summary_rows):
        parser.error("invalid scenario, storage delay or dataset size")
    if args.pg_pool < 8 or args.stream_delay_us < 0 or args.case_timeout <= 0 or not all(math.isfinite(value) for value in args.storage_rtt_ms):
        parser.error("pool must be >= 8; stream delay nonnegative; timeout positive; storage delays finite")
    if args.binary and args.revision:
        parser.error("choose --binary or --revision")
    return args


def main():
    args = arguments()
    artifacts = Path(args.output).resolve() / args.label
    artifacts.mkdir(parents=True, exist_ok=False)
    binary, meta = prepare(args, artifacts)
    success = True
    if not args.prepare_only:
        success = asyncio.run(measure(args, binary, artifacts, meta))
    print(json.dumps({"artifacts": str(artifacts), "binary": str(binary)}))
    if not success:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
