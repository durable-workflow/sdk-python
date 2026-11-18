"""Run one fixed approved SDK acceptance case inside an owned internal network."""

from __future__ import annotations

import argparse
import base64
import hashlib
import ipaddress
import json
import os
import re
import signal
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
from xml.etree import ElementTree

try:
    from scripts.ci import activity_acceptance_output as output_files
    from scripts.ci import activity_acceptance_release as release
    from scripts.ci import activity_acceptance_runtime as runtime
    from scripts.ci.activity_acceptance_buildx import BuildxState
    from scripts.ci.activity_acceptance_session import Session
except ModuleNotFoundError as error:
    if error.name != "scripts":
        raise
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from scripts.ci import activity_acceptance_output as output_files
    from scripts.ci import activity_acceptance_release as release
    from scripts.ci import activity_acceptance_runtime as runtime
    from scripts.ci.activity_acceptance_buildx import BuildxState
    from scripts.ci.activity_acceptance_session import Session

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "published-activity-results"
REQUIREMENTS = "requirements.txt"
VERIFIER_FILENAME = "verify_activity_install.py"
OWNER_VALUE = "=sdk-python"
LABEL_FILTER = "label="
ID_FORMAT = "{{.ID}}"
DIAGNOSTIC_LIMIT = 64 * 1024
SCENARIO = "activity-identity"
ARTIFACT_SOURCE = "registry"
SCENARIOS = {
    "pagination": (
        "test_published_pagination.py", "test_published_iterators_cross_real_page_boundaries",
    ),
    "activity-identity": (
        "test_activity_identity.py", "test_retry_preserves_received_execution_id_and_changes_attempt_id",
    ),
    "timer-redrive": (
        "test_native_timer_redrive.py", "test_cold_workers_preserve_fractional_timer_clock_across_repeated_redrive",
    ),
    "retry-cap": (
        "test_activity_retry_cap.py", "test_installed_local_retries_preserve_cap_tail_and_committed_replay",
    ),
}
# No inherited context, registry credentials, proxy, SSH or remote Docker endpoint.
# This kernel directory is private to each CLI process and contains no config.json.
DOCKER_ENVIRONMENT = {
    "PATH": os.defpath,
    "DOCKER_HOST": "unix:///var/run/docker.sock",
    "DOCKER_CONFIG": "/proc/self/fd",
}


def call(*arguments, check=True, **kwargs):
    if arguments[0] == "docker":
        kwargs["env"] = DOCKER_ENVIRONMENT
    if arguments[:2] == ("docker", "buildx"):
        with BuildxState(DOCKER_ENVIRONMENT) as environment:
            kwargs["env"] = environment
            return subprocess.run(arguments, check=check, capture_output=True, text=True, timeout=180, **kwargs)
    return subprocess.run(arguments, check=check, capture_output=True, text=True, timeout=180, **kwargs)


def cleanup_call(*arguments, check=True):
    return subprocess.run(arguments, check=check, capture_output=True, text=True, timeout=10, env=DOCKER_ENVIRONMENT)


def record_failure(files, report, error):
    report["failure"] = type(error).__name__ + ": " + str(error)[:1000]
    report["complete"] = False
    if not isinstance(error, subprocess.CalledProcessError | subprocess.TimeoutExpired):
        return
    arguments = list(error.cmd) if isinstance(error.cmd, tuple | list) else [error.cmd]
    report["command_failure"] = {
        "program": arguments[0],
        "operation": arguments[1:4],
        "argv_sha256": hashlib.sha256(json.dumps(arguments).encode()).hexdigest(),
        "exit_code": getattr(error, "returncode", None),
        "timeout_seconds": getattr(error, "timeout", None),
        "streams": {},
    }
    if hasattr(error, "buildx_state_failure"):
        report["command_failure"]["buildx_state_failure"] = error.buildx_state_failure
    for name, value in (("stdout", error.stdout), ("stderr", error.stderr)):
        data = value.encode() if isinstance(value, str) else value or b""
        retained = data[:DIAGNOSTIC_LIMIT]
        report["command_failure"]["streams"][name] = {
            "bytes": len(data),
            "retained_bytes": len(retained),
            "truncated": len(data) > len(retained),
            "sha256": hashlib.sha256(data).hexdigest(),
        }
        try:
            files.write("command-" + name + ".txt", retained)
        except (OSError, ValueError) as retention_error:
            report.setdefault("diagnostic_retention_failures", {})[name] = type(retention_error).__name__


def deadline(_signum, _frame):
    raise TimeoutError("published cell reached its six-minute execution bound")


def verify_distributions(ready, requirements):
    expected = sorted(
        (name.lower().replace("_", "-"), version)
        for name, version in re.findall(r"^([A-Za-z0-9_.-]+)==([^\s]+)", requirements.decode(), re.MULTILINE)
    )
    actual = sorted((name.lower().replace("_", "-"), version) for name, version in json.loads(ready["distributions"]))
    if not expected or actual != expected:
        raise ValueError("consumer environment differs from the complete dependency lock")


def verify_case_result(result):
    if not result["complete"]:
        raise ValueError("published retry case reported failure")
    root = ElementTree.fromstring(result["junit"])
    cases = list(root.iter("testcase"))
    expected = SCENARIOS[SCENARIO][1]
    if len(cases) != 1 or cases[0].get("name") != expected:
        raise ValueError("published retry case inventory differs")
    if any(cases[0].find(tag) is not None for tag in ("failure", "error", "skipped")):
        raise ValueError("published retry case did not pass")


def engine():
    observed = json.loads(call("docker", "info", "--format", "{{json .}}").stdout)
    if observed["OSType"] != "linux" or observed["Architecture"] not in {"x86_64", "amd64"}:
        raise ValueError("published cell requires the local Linux amd64 engine")
    return {key: observed[key] for key in ("ID", "ServerVersion", "OSType", "Architecture")}


def packet_file(data):
    return {"base64": base64.b64encode(data).decode(), "sha256": hashlib.sha256(data).hexdigest()}


def retain_consumer_ready(files, report, ready):
    # The complete environment inventory can exceed the terminal receipt bound.
    # Keep its bytes independently and bind them rather than duplicating them.
    data = json.dumps({key: value for key, value in ready.items() if key != "sdk_inventory"}, indent=2).encode()
    name = "consumer-ready.json"
    files.write(name, data)
    report["consumer_ready"] = {"file": name, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def bind_bootstrap(captured):
    source = captured["bootstrap.py"].decode()
    for name, filename in (("CAPTURED_VERIFIER", VERIFIER_FILENAME), ("CAPTURED_ENTRY", "entry.py")):
        placeholder = name + ' = ""'
        if source.count(placeholder) != 1:
            raise ValueError("consumer source binding placeholder differs")
        source = source.replace(placeholder, name + " = " + repr(captured[filename].decode()), 1)
    return source


def capture_source(report):
    commit = call("git", "rev-parse", "HEAD", cwd=ROOT).stdout.strip()
    if re.fullmatch(r"[0-9a-f]{40}", commit) is None or os.environ.get("GITHUB_SHA", commit) != commit:
        raise ValueError("checkout differs from the selected Actions commit")
    paths = {
        REQUIREMENTS: "scripts/ci/" + (
            "retry-cap-dependencies.txt" if SCENARIO == "retry-cap" else "published-activity-requirements.txt"
        ),
        VERIFIER_FILENAME: "scripts/ci/verify_activity_install.py",
        SCENARIOS[SCENARIO][0]: "tests/integration/" + SCENARIOS[SCENARIO][0],
        "conftest.py": "tests/integration/conftest.py",
        "bootstrap.py": "scripts/ci/activity_acceptance_consumer.py",
        "entry.py": "scripts/ci/activity_acceptance_entry.py",
        "tuple.json": "tests/fixtures/" + (
            {"timer-redrive": "published-timer-redrive.json", "retry-cap": "published-retry-cap.json",
             "pagination": "published-pagination.json"}.get(
                SCENARIO, "published-activity-acceptance.json"
            )
        ),
    }
    if SCENARIO == "timer-redrive":
        paths["redrive_timer_clock_workflow.py"] = "tests/integration/redrive_timer_clock_workflow.py"
    if SCENARIO == "retry-cap":
        paths["release.json"] = f".github/release-candidates/{release.VERSION}.json"
        paths["release-loader.py"] = "scripts/ci/activity_acceptance_release.py"
    captured = {
        name: call("git", "show", commit + ":" + path, cwd=ROOT).stdout.encode() for name, path in paths.items()
    }
    report["source_commit"] = commit
    report["source_files"] = {paths[name]: hashlib.sha256(data).hexdigest() for name, data in captured.items()}
    return captured


def download_wheels(selected, captured, files):
    if ARTIFACT_SOURCE == "candidate" and SCENARIO != "retry-cap":
        raise ValueError("candidate artifacts require the fixed retry-cap case")
    approval_path = ".github/release-candidates/" + selected["sdk_version"] + ".json"
    approval = json.loads(call("git", "show", "HEAD:" + approval_path, cwd=ROOT).stdout)
    wheel_name = next(name for name in approval["files"] if name.endswith(".whl"))
    dependency_lock = captured[REQUIREMENTS]
    if SCENARIO == "retry-cap":
        if selected["sdk_version"] != release.VERSION or approval != json.loads(captured["release.json"]):
            raise ValueError("retry release selection differs from captured authority")
        captured[REQUIREMENTS], _digest = release.lock(dependency_lock, approval)
    files.write(REQUIREMENTS, captured[REQUIREMENTS])
    download_requirements = REQUIREMENTS
    if ARTIFACT_SOURCE == "candidate":
        download_requirements = "retry-dependencies.txt"
        files.write(download_requirements, dependency_lock)
    files.check()
    wheelhouse = OUTPUT / "wheels"
    wheelhouse.mkdir(mode=0o700)
    environment = {
        "PATH": os.defpath,
        "PIP_CONFIG_FILE": os.devnull,
        "PIP_DISABLE_PIP_VERSION_CHECK": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PIP_INDEX_URL": "https://pypi.org/simple",
        **{key: os.environ[key] for key in ("SSL_CERT_FILE", "PIP_CERT", "REQUESTS_CA_BUNDLE") if key in os.environ},
    }
    result = call(
        sys.executable,
        "-I",
        "-m",
        "pip",
        "download",
        "--require-hashes",
        "--only-binary=:all:",
        "--dest",
        str(wheelhouse),
        "-r",
        str(OUTPUT / download_requirements),
        env=environment,
    )
    files.write("download.log", result.stdout + result.stderr)
    files.check()
    if ARTIFACT_SOURCE == "candidate":
        data = release.candidate_wheel(ROOT, approval["files"][wheel_name])
        descriptor = os.open(wheelhouse / wheel_name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "wb") as output:
            output.write(data)
    wheels = {}
    for path in sorted(wheelhouse.iterdir()):
        if path.is_symlink() or not path.is_file() or path.suffix != ".whl":
            raise ValueError("downloaded wheel membership differs")
        wheels[path.name] = packet_file(path.read_bytes())
    if wheels[wheel_name]["sha256"] != approval["files"][wheel_name]:
        raise ValueError("registry wheel differs from the independently reviewed release")
    inputs = [REQUIREMENTS, VERIFIER_FILENAME, SCENARIOS[SCENARIO][0], "conftest.py"]
    if SCENARIO == "timer-redrive":
        inputs.append("redrive_timer_clock_workflow.py")
    return {
        "scenario": SCENARIO,
        "files": {
            name: packet_file(captured[name])
            for name in inputs
        },
        "wheels": wheels,
        "wheel_name": wheel_name,
        "wheel_sha256": approval["files"][wheel_name],
        "sdk_version": selected["sdk_version"],
    }


def verify_published_image(selected, role, files):
    prefix = "server" if role == "runtime" else "python"
    image, child_digest = selected[prefix + "_image"], selected[prefix + "_amd64_manifest"]
    call("docker", "pull", "--platform", "linux/amd64", image)
    index_text = call("docker", "buildx", "imagetools", "inspect", "--raw", image).stdout
    child_text = call(
        "docker", "buildx", "imagetools", "inspect", "--raw", image.split("@", 1)[0] + "@" + child_digest
    ).stdout
    index, child = json.loads(index_text), json.loads(child_text)
    platforms = [
        item["digest"]
        for item in index["manifests"]
        if item.get("platform") == {"architecture": "amd64", "os": "linux"}
    ]
    if platforms != [child_digest]:
        raise ValueError("published index differs from the selected platform child")
    local_index = json.loads(call("docker", "image", "inspect", image).stdout)[0]
    local_platform = json.loads(call("docker", "image", "inspect", "--platform", "linux/amd64", image).stdout)[0]
    observed = runtime.bind_local_image(
        local_index, local_platform, image.rsplit("@", 1)[-1], child_digest, child["config"]["digest"]
    )
    if role == "runtime":
        if observed["Config"]["Labels"]["org.opencontainers.image.revision"] != selected["server_source"]:
            raise ValueError("runtime source differs")
    elif "PYTHON_VERSION=" + selected["python_version"] not in observed["Config"]["Env"]:
        raise ValueError("consumer interpreter image differs")
    files.write(role + "-image.json", json.dumps(observed, indent=2))
    files.write(role + "-local-index.json", json.dumps(local_index, indent=2))
    files.write(role + "-index.json", index_text)
    files.write(role + "-manifest.json", child_text)
    return observed


def absent(result, identity):
    return result.returncode == 1 and result.stderr.strip().lower() == "error: no such object: " + identity


class OwnedDocker:
    def __init__(self, nonce, files, report):
        self.nonce, self.files, self.report = nonce, files, report
        self.containers = {"runtime": set(), "consumer": set()}
        self.networks = set()
        self.network = None
        self.identities = {}

    def inspect(self, kind, identity):
        command = ["docker", "network", "inspect", identity] if kind == "network" else ["docker", "inspect", identity]
        return json.loads(call(*command).stdout)[0]

    def labels(self, role):
        return [
            "--label",
            runtime.OWNER + OWNER_VALUE,
            "--label",
            runtime.RUN + "=" + self.nonce,
            "--label",
            runtime.ROLE + "=" + role,
        ]

    def create_network(self):
        result = call(
            "docker",
            "network",
            "create",
            "--internal",
            "--driver",
            "bridge",
            *self.labels("network"),
            "sdk-python-activity-" + self.nonce,
            check=False,
        )
        identity = result.stdout.strip()
        if runtime.full_id(identity):
            self.networks.add(identity)
        result.check_returncode()
        if not runtime.full_id(identity):
            raise ValueError("network create did not return a full ID")
        runtime.verify_network(self.inspect("network", identity), identity, self.nonce)
        self.network = identity
        self.report["network_id"] = identity

    def create(self, role, image, child, bootstrap):
        self.files.check()
        cid_name = role + ".cid"
        result = call(
            *runtime.create_arguments(image, self.network, self.nonce, OUTPUT / cid_name, role, bootstrap), check=False
        )
        identity, agrees, candidates = runtime.created_identity(result.stdout, self.files.read_cid(cid_name))
        self.containers[role].update(candidates)
        result.check_returncode()
        if not agrees:
            raise ValueError("container create channels disagree or omit the full ID")
        self.files.check()
        self.identities[role] = identity
        self.report[role + "_id"] = identity
        self.verify(role, image, child, bootstrap)
        return identity

    def verify(self, role, image, child, bootstrap):
        identity = self.identities[role]
        record = self.inspect("container", identity)
        runtime.verify_container(
            record,
            identity,
            self.nonce,
            image,
            self.network,
            child,
            role,
            bootstrap,
            cidfile=str(OUTPUT / (role + ".cid")),
        )
        return record

    def boundary(self, images, selected, bootstrap, phase):
        network = self.inspect("network", self.network)
        runtime.verify_network(network, self.network, self.nonce)
        records = {
            role: self.verify(role, images[role], selected[prefix + "_amd64_manifest"], bootstrap)
            for role, prefix in (("runtime", "server"), ("consumer", "python"))
        }
        if any(not record["State"]["Running"] or record["State"]["OOMKilled"] for record in records.values()):
            raise ValueError("both owned containers must remain running without OOM")
        url = runtime.verify_attachments(network, records["runtime"], records["consumer"])
        self.files.write(phase + "-authority.json", json.dumps({"network": network, **records}, indent=2))
        return url

    def discover(self):
        filters = [
            "--filter",
            LABEL_FILTER + runtime.OWNER + OWNER_VALUE,
            "--filter",
            LABEL_FILTER + runtime.RUN + "=" + self.nonce,
        ]
        for role in self.containers:
            result = cleanup_call(
                "docker",
                "ps",
                "-a",
                "--no-trunc",
                *filters,
                "--filter",
                LABEL_FILTER + runtime.ROLE + "=" + role,
                "--format",
                ID_FORMAT,
            )
            self.containers[role].update(value for value in result.stdout.splitlines() if runtime.full_id(value))
        result = cleanup_call(
            "docker",
            "network",
            "ls",
            "--no-trunc",
            *filters,
            "--filter",
            LABEL_FILTER + runtime.ROLE + "=network",
            "--format",
            ID_FORMAT,
        )
        self.networks.update(value for value in result.stdout.splitlines() if runtime.full_id(value))

    def remove_container(self, role, identity):
        result = cleanup_call("docker", "inspect", identity, check=False)
        if absent(result, identity):
            return {"absent": True}
        result.check_returncode()
        record = json.loads(result.stdout)[0]
        runtime.require_owner(record, identity, self.nonce, role)
        try:
            logs = cleanup_call("docker", "logs", identity, check=False)
            self.files.write(role + "-" + identity[:12] + ".log", logs.stdout + logs.stderr)
        except (OSError, ValueError):
            self.report["log_retention_failed"] = True
        cleanup_call("docker", "rm", "-f", "-v", identity)
        result = cleanup_call("docker", "inspect", identity, check=False)
        return {
            "absent": absent(result, identity),
            "oom_killed": record["State"]["OOMKilled"],
            "exit_code": record["State"]["ExitCode"],
        }

    def remove_network(self, identity):
        record = json.loads(cleanup_call("docker", "network", "inspect", identity).stdout)[0]
        runtime.verify_network(record, identity, self.nonce)
        if record["Containers"]:
            raise ValueError("network retains an attachment; refusing removal")
        cleanup_call("docker", "network", "rm", identity)
        result = cleanup_call(
            "docker", "network", "ls", "--no-trunc", "--filter", "id=" + identity, "--format", ID_FORMAT
        )
        return {"absent": not result.stdout.strip()}

    def cleanup(self):
        outcomes = {}
        try:
            self.discover()
        except Exception as error:
            outcomes["reconciliation"] = {"error": type(error).__name__}
        self.cleanup_network_boundaries(outcomes)
        # Each removal is independent: a refused output path or one foreign ID
        # cannot disable cleanup of the other captured owned objects.
        for role, identities in self.containers.items():
            for identity in sorted(identities):
                self.cleanup_one(outcomes, identity, self.remove_container, role, identity)
        for identity in sorted(self.networks):
            self.cleanup_one(outcomes, identity, self.remove_network, identity)
        self.cleanup_one(outcomes, "remaining-owned-labels", self.verify_label_absence)
        self.report["cleanup"] = outcomes
        self.report["complete"] = self.report["complete"] and all(
            value.get("absent") and not value.get("oom_killed") for value in outcomes.values()
        )

    @staticmethod
    def cleanup_one(outcomes, key, action, *arguments):
        try:
            outcomes[key] = action(*arguments)
        except Exception as error:
            outcomes[key] = {"error": type(error).__name__}

    def cleanup_network_boundaries(self, outcomes):
        for identity in sorted(self.networks):
            try:
                record = json.loads(cleanup_call("docker", "network", "inspect", identity).stdout)[0]
                runtime.verify_network(record, identity, self.nonce)
                known = set().union(*self.containers.values())
                if set(record["Containers"] or {}) - known:
                    raise ValueError("foreign network attachment before cleanup")
            except Exception as error:
                outcomes["network-boundary-" + identity] = {"error": type(error).__name__}

    def verify_label_absence(self):
        filters = [
            "--filter",
            LABEL_FILTER + runtime.OWNER + OWNER_VALUE,
            "--filter",
            LABEL_FILTER + runtime.RUN + "=" + self.nonce,
        ]
        containers = cleanup_call("docker", "ps", "-a", "--no-trunc", *filters, "--format", ID_FORMAT)
        networks = cleanup_call("docker", "network", "ls", "--no-trunc", *filters, "--format", ID_FORMAT)
        return {"absent": not containers.stdout.strip() and not networks.stdout.strip()}


def run_case(owned, selected, images, packet, bootstrap, report):
    owned.create_network()
    runtime_id = owned.create("runtime", images["runtime"], selected["server_amd64_manifest"], bootstrap)
    call("docker", "start", runtime_id)
    consumer_id = owned.create("consumer", images["consumer"], selected["python_amd64_manifest"], bootstrap)
    session = Session(consumer_id, DOCKER_ENVIRONMENT)
    try:
        session.send(packet)
        ready = session.receive("ACTIVITY_CONSUMER_READY=")
        verify_distributions(ready, base64.b64decode(packet["files"][REQUIREMENTS]["base64"]))
        url = owned.boundary(images, selected, bootstrap, "before-sdk")
        session.send({"runtime_ipv4": int(ipaddress.IPv4Address(url)), "token": runtime.ENVIRONMENT["DW_AUTH_TOKEN"]})
        result = session.receive("ACTIVITY_CONSUMER_RESULT=")
        verify_case_result(result)
        if owned.boundary(images, selected, bootstrap, "after-sdk") != url:
            raise ValueError("runtime endpoint changed during the case")
        if ready["sdk_inventory"] != result["sdk_inventory"]:
            raise ValueError("installed SDK bytes changed during the case")
        owned.files.write("sdk-inventory.json", json.dumps(result["sdk_inventory"], indent=2))
        name = "identity" if SCENARIO == "activity-identity" else SCENARIO
        owned.files.write(name + ".log", result["case_output"])
        owned.files.write(name + ".xml", result["junit"])
        retain_consumer_ready(owned.files, report, ready)
        session.send({"release": True})
        code = session.finish()
        report["consumer_exit"] = code
        report["complete"] = bool(result["complete"] and code == 0)
    finally:
        session.close()
        owned.files.write("consumer-transport.log", bytes(session.output))


def main():
    files = output_files.Output(OUTPUT)
    report = {"complete": False, "scenario": SCENARIO, "artifact_source": ARTIFACT_SOURCE,
              "started_at": datetime.now(timezone.utc).isoformat()}
    owned = OwnedDocker(uuid4().hex, files, report)
    signal.signal(signal.SIGALRM, deadline)
    signal.alarm(360)
    try:
        captured = capture_source(report)
        selected = json.loads(captured["tuple.json"])
        report["tuple"] = selected
        report["engine"] = engine()
        packet = download_wheels(selected, captured, files)
        report["requirements_sha256"] = packet["files"][REQUIREMENTS]["sha256"]
        report["wheel_sha256"] = packet["wheel_sha256"]
        report["wheel_set"] = {name: item["sha256"] for name, item in packet["wheels"].items()}
        images = {role: verify_published_image(selected, role, files) for role in ("runtime", "consumer")}
        bootstrap = bind_bootstrap(captured)
        report["consumer_bootstrap_sha256"] = hashlib.sha256(bootstrap.encode()).hexdigest()
        run_case(owned, selected, images, packet, bootstrap, report)
        if engine() != report["engine"]:
            raise ValueError("Docker engine authority changed")
    except Exception as error:
        record_failure(files, report, error)
    finally:
        signal.alarm(0)
        owned.cleanup()
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        files.finish(report)
        files.close()
    if not report["complete"]:
        raise RuntimeError("published activity acceptance or cleanup is incomplete; see result.json")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", choices=tuple(SCENARIOS), default=SCENARIO)
    parser.add_argument("--artifact-source", choices=("registry", "candidate"), default=ARTIFACT_SOURCE)
    arguments = parser.parse_args()
    SCENARIO, ARTIFACT_SOURCE = arguments.scenario, arguments.artifact_source
    if ARTIFACT_SOURCE == "candidate" and SCENARIO != "retry-cap":
        parser.error("candidate artifacts are accepted only by the fixed retry-cap case")
    main()
