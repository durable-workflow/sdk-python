from __future__ import annotations

import importlib.util
import json
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest

PATH = Path(__file__).parents[1] / "scripts/ci/published-activity-acceptance.py"
SPEC = importlib.util.spec_from_file_location("published_activity_acceptance", PATH)
assert SPEC is not None
assert SPEC.loader is not None
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)
runtime = runner.runtime


def configuration(role="runtime"):
    image = {
        "Id": "sha256:" + "f" * 64,
        "Config": {
            "Env": ["PATH=/usr/local/bin:/usr/bin:/bin"],
            "Entrypoint": ["/app/server"],
            "Cmd": None,
            "WorkingDir": "/app",
        },
    }
    entrypoint, command, directory = runtime.expected_command(image, role, "captured source")
    environment = list(image["Config"]["Env"])
    if role == "runtime":
        environment += [key + "=" + value for key, value in runtime.ENVIRONMENT.items()]
    host = {
        "NetworkMode": "c" * 64,
        "ReadonlyRootfs": True,
        "Privileged": False,
        "NanoCpus": 1_000_000_000,
        "Memory": 1_073_741_824,
        "MemorySwap": 1_073_741_824,
        "PidsLimit": 256,
        "Init": True,
        "CapDrop": ["ALL"],
        "SecurityOpt": ["no-new-privileges"],
        "Tmpfs": deepcopy(runtime.TMPFS[role]),
        "PublishAllPorts": False,
        "AutoRemove": False,
        "OomKillDisable": False,
        "MemorySwappiness": None,
        "ShmSize": 67_108_864,
        "Runtime": "runc",
        "IpcMode": "private",
        "CgroupnsMode": "private",
        "RestartPolicy": {"Name": "no", "MaximumRetryCount": 0},
        "LogConfig": {"Type": "json-file", "Config": {"max-size": "1m", "max-file": "1"}},
    }
    record = {
        "Id": "a" * 64,
        "Image": image["Id"],
        "Config": {
            "Labels": {runtime.OWNER: "sdk-python", runtime.RUN: "ours", runtime.ROLE: role},
            "Env": environment,
            "Image": image["Id"],
            "User": "1000:1000",
            "Entrypoint": entrypoint,
            "Cmd": command,
            "WorkingDir": directory,
            "Tty": False,
            "OpenStdin": role == "consumer",
        },
        "HostConfig": host,
        "Mounts": [],
        "ImageManifestDescriptor": {"digest": "sha256:" + "d" * 64},
    }
    return record, image


@pytest.mark.parametrize("role", ["runtime", "consumer"])
def test_exact_closed_container_configuration_is_admitted(role):
    record, image = configuration(role)
    runtime.verify_container(record, "a" * 64, "ours", image, "c" * 64, "sha256:" + "d" * 64, role, "captured source")
    arguments = runtime.create_arguments(image, "c" * 64, "ours", "captured.cid", role, "captured source")
    assert "--pull=never" in arguments
    assert "-p" not in arguments
    assert "--publish" not in arguments


@pytest.mark.parametrize("role", ["runtime", "consumer"])
@pytest.mark.parametrize(
    "change",
    [
        "image",
        "child",
        "user",
        "command",
        "owner",
        "nonce",
        "role",
        "proxy",
        "writable",
        "swap",
        "caps",
        "network",
        "host_mount",
        "port",
        "ipc",
        "extra_env",
    ],
)
def test_mutated_authority_or_container_profile_refuses_before_start(role, change):
    record, image = configuration(role)
    config, host = record["Config"], record["HostConfig"]
    if change == "image":
        record["Image"] = "sha256:" + "e" * 64
    elif change == "child":
        record["ImageManifestDescriptor"]["digest"] = "sha256:" + "e" * 64
    elif change == "user":
        config["User"] = "0:0"
    elif change == "command":
        config["Entrypoint"] = ["/bin/sh"]
    elif change in {"owner", "nonce", "role"}:
        config["Labels"][{"owner": runtime.OWNER, "nonce": runtime.RUN, "role": runtime.ROLE}[change]] = "foreign"
    elif change in {"proxy", "extra_env"}:
        config["Env"].append("HTTPS_PROXY=http://foreign" if change == "proxy" else "PYTHONPATH=/checkout/src")
    elif change == "writable":
        host["ReadonlyRootfs"] = False
    elif change == "swap":
        host["MemorySwap"] = -1
    elif change == "caps":
        host["CapAdd"] = ["SYS_ADMIN"]
    elif change == "network":
        host["NetworkMode"] = "host"
    elif change == "host_mount":
        record["Mounts"] = [{"Type": "bind", "Destination": "/checkout"}]
    elif change == "port":
        host["PortBindings"] = {"8080/tcp": [{"HostIp": "127.0.0.1", "HostPort": "1234"}]}
    else:
        host["IpcMode"] = "host"
    with pytest.raises(ValueError):
        runtime.verify_container(
            record, "a" * 64, "ours", image, "c" * 64, "sha256:" + "d" * 64, role, "captured source"
        )


def network_fixture():
    network = {
        "Id": "c" * 64,
        "Driver": "bridge",
        "Internal": True,
        "Attachable": False,
        "Ingress": False,
        "EnableIPv6": False,
        "Labels": {runtime.OWNER: "sdk-python", runtime.RUN: "ours", runtime.ROLE: "network"},
        "IPAM": {"Config": [{"Subnet": "172.29.0.0/16"}]},
        "Containers": {},
    }
    records = []
    for identity, address in (("a" * 64, "172.29.0.2"), ("b" * 64, "172.29.0.3")):
        records.append(
            {
                "Id": identity,
                "NetworkSettings": {
                    "Networks": {
                        "owned": {"NetworkID": network["Id"], "IPAddress": address},
                    }
                },
            }
        )
        network["Containers"][identity] = {"IPv4Address": address + "/16"}
    return network, records


def test_endpoint_is_bound_to_exact_two_ids_network_and_numeric_ip():
    network, records = network_fixture()
    runtime.verify_network(network, "c" * 64, "ours")
    assert runtime.verify_attachments(network, *records) == "172.29.0.2"


@pytest.mark.parametrize(
    "change",
    [
        "foreign",
        "missing",
        "extra_network",
        "wrong_network",
        "wrong_ip",
        "loopback",
        "hostname",
        "public",
        "wrong_subnet",
    ],
)
def test_endpoint_substitution_or_foreign_attachment_is_refused(change):
    network, records = network_fixture()
    endpoint = records[0]["NetworkSettings"]["Networks"]["owned"]
    if change == "foreign":
        network["Containers"]["e" * 64] = {"IPv4Address": "172.29.0.4/16"}
    elif change == "missing":
        del network["Containers"]["a" * 64]
    elif change == "extra_network":
        records[0]["NetworkSettings"]["Networks"]["foreign"] = deepcopy(endpoint)
    elif change == "wrong_network":
        endpoint["NetworkID"] = "e" * 64
    elif change == "wrong_ip":
        endpoint["IPAddress"] = "172.29.0.4"
    elif change == "wrong_subnet":
        network["IPAM"]["Config"][0]["Subnet"] = "172.30.0.0/16"
    else:
        endpoint["IPAddress"] = {"loopback": "127.0.0.1", "hostname": "foreign.example", "public": "8.8.8.8"}[change]
    with pytest.raises(ValueError):
        runtime.verify_attachments(network, *records)


@pytest.mark.parametrize(
    "key,value",
    [("Internal", False), ("Attachable", True), ("Ingress", True), ("EnableIPv6", True), ("Driver", "host")],
)
def test_network_refuses_a_less_restricted_profile(key, value):
    network, _records = network_fixture()
    network[key] = value
    with pytest.raises(ValueError):
        runtime.verify_network(network, "c" * 64, "ours")


def test_uncertain_create_is_reconciled_by_owner_role_nonce_and_full_id(monkeypatch, tmp_path):
    files = runner.output_files.Output(tmp_path / "result")
    owned = runner.OwnedDocker("ours", files, {"complete": False})
    seen = []

    def call(*arguments):
        seen.append(arguments)
        role = arguments[-3]
        identities = {
            "label=" + runtime.ROLE + "=runtime": "a" * 64,
            "label=" + runtime.ROLE + "=consumer": "b" * 64,
            "label=" + runtime.ROLE + "=network": "c" * 64,
        }
        return SimpleNamespace(stdout=identities[role] + "\n")

    monkeypatch.setattr(runner, "cleanup_call", call)
    try:
        owned.discover()
        assert owned.containers == {"runtime": {"a" * 64}, "consumer": {"b" * 64}}
        assert owned.networks == {"c" * 64}
        for arguments in seen:
            assert "label=" + runtime.OWNER + "=sdk-python" in arguments
            assert "label=" + runtime.RUN + "=ours" in arguments
    finally:
        files.close()


def test_cleanup_refuses_a_foreign_same_name_replacement_without_removing_it(monkeypatch, tmp_path):
    files = runner.output_files.Output(tmp_path / "result")
    owned = runner.OwnedDocker("ours", files, {"complete": False})
    record, _image = configuration()
    record["Name"] = "/reused-display-name"
    record["Config"]["Labels"][runtime.RUN] = "foreign"
    calls = []

    def call(*arguments, **_kwargs):
        calls.append(arguments)
        return SimpleNamespace(stdout=json.dumps([record]), returncode=0, stderr="", check_returncode=lambda: None)

    monkeypatch.setattr(runner, "cleanup_call", call)
    try:
        with pytest.raises(ValueError, match="cleanup ownership differs"):
            owned.remove_container("runtime", "a" * 64)
        assert calls == [("docker", "inspect", "a" * 64)]
    finally:
        files.close()


def test_partial_cleanup_failure_cannot_disable_other_owned_removals(monkeypatch, tmp_path):
    files = runner.output_files.Output(tmp_path / "result")
    report = {"complete": True}
    owned = runner.OwnedDocker("ours", files, report)
    owned.containers = {"runtime": {"a" * 64}, "consumer": {"b" * 64}}
    owned.networks = {"c" * 64}
    calls = []
    network, _records = network_fixture()
    network["Containers"] = {}
    monkeypatch.setattr(owned, "discover", lambda: None)
    monkeypatch.setattr(runner, "cleanup_call", lambda *_args: SimpleNamespace(stdout=json.dumps([network])))

    def remove(role, identity):
        calls.append(identity)
        if role == "runtime":
            raise ValueError("reserved output path refused")
        return {"absent": True}

    monkeypatch.setattr(owned, "remove_container", remove)
    monkeypatch.setattr(owned, "remove_network", lambda identity: calls.append(identity) or {"absent": True})
    try:
        owned.cleanup()
        assert calls == ["a" * 64, "b" * 64, "c" * 64]
        assert report["complete"] is False
    finally:
        files.close()


@pytest.mark.parametrize(
    "actual",
    [
        [["durable-workflow", "2.17.2"], ["pytest", "9.2.0"], ["ambient-plugin", "1.0"]],
        [["durable-workflow", "2.17.2"], ["pytest", "9.1.0"]],
        [["durable-workflow", "2.17.2"]],
    ],
)
def test_complete_lock_rejects_ambient_missing_or_changed_distributions(actual):
    lock = b"durable-workflow==2.17.2 \\\n    --hash=sha256:abc\npytest==9.2.0 \\\n    --hash=sha256:def\n"
    ready = {"distributions": json.dumps(actual)}
    with pytest.raises(ValueError, match="complete dependency lock"):
        runner.verify_distributions(ready, lock)


def test_complete_lock_accepts_only_the_exact_private_environment():
    runner.verify_distributions(
        {"distributions": json.dumps([["pytest", "9.2.0"], ["durable_workflow", "2.17.2"]])},
        b"durable-workflow==2.17.2\npytest==9.2.0\n",
    )


def test_malformed_create_retains_owned_cid_for_cleanup_and_terminal_failure(monkeypatch, tmp_path):
    files = runner.output_files.Output(tmp_path / "result")
    report = {"complete": False}
    owned = runner.OwnedDocker("ours", files, report)
    owned.network = "c" * 64
    monkeypatch.setattr(runner, "OUTPUT", files.path)
    (files.path / "runtime.cid").write_text("a" * 64 + "\n")
    result = SimpleNamespace(stdout="malformed create output", check_returncode=lambda: None)
    monkeypatch.setattr(runner, "call", lambda *_args, **_kwargs: result)
    monkeypatch.setattr(owned, "verify", lambda *_args: pytest.fail("uncertain create must not start"))
    monkeypatch.setattr(owned, "discover", lambda: None)
    monkeypatch.setattr(owned, "verify_label_absence", lambda: {"absent": True})
    removed = []
    monkeypatch.setattr(
        owned, "remove_container", lambda role, identity: removed.append((role, identity)) or {"absent": True}
    )
    try:
        _record, image = configuration()
        with pytest.raises(ValueError, match="create channels disagree"):
            owned.create("runtime", image, "sha256:" + "d" * 64, "captured source")
        owned.cleanup()
        files.finish(report)
        assert removed == [("runtime", "a" * 64)]
        receipt = json.loads((files.path / "result.json").read_text())
        assert receipt["complete"] is False
        assert receipt["cleanup"]["a" * 64]["absent"] is True
    finally:
        files.close()


def case_result():
    return {
        "complete": True,
        "junit": '<testsuites><testsuite tests="1"><testcase '
        'name="test_retry_preserves_received_execution_id_and_changes_attempt_id"/>'
        "</testsuite></testsuites>",
    }


def test_complete_case_requires_the_actual_single_retry_test_to_pass():
    runner.verify_case_result(case_result())


@pytest.mark.parametrize("change", ["skip", "failure", "error", "wrong_case", "empty", "reported_failure"])
def test_exit_zero_cannot_qualify_a_skipped_missing_or_different_case(change):
    result = case_result()
    if change in {"skip", "failure", "error"}:
        tag = "skipped" if change == "skip" else change
        result["junit"] = result["junit"].replace("/>", "><" + tag + "/></testcase>")
    elif change == "wrong_case":
        result["junit"] = result["junit"].replace(
            "test_retry_preserves_received_execution_id_and_changes_attempt_id", "unrelated_case"
        )
    elif change == "empty":
        result["junit"] = '<testsuites><testsuite tests="0"/></testsuites>'
    else:
        result["complete"] = False
    with pytest.raises(ValueError):
        runner.verify_case_result(result)


@pytest.mark.parametrize(
    "field,value",
    [
        ("GroupAdd", ["0"]),
        ("OomKillDisable", True),
        ("Sysctls", {"net.ipv4.ip_forward": "1"}),
        ("DeviceCgroupRules", ["c *:* rwm"]),
        ("CgroupParent", "foreign"),
        ("Ulimits", [{"Name": "nofile", "Soft": 999999, "Hard": 999999}]),
        ("Dns", ["8.8.8.8"]),
        ("DnsSearch", ["foreign"]),
        ("DnsOptions", ["use-vc"]),
        ("CpuQuota", -1),
        ("CpusetCpus", "0"),
        ("MemoryReservation", 999999),
        ("OomScoreAdj", -1000),
        ("BlkioDeviceReadBps", [{"Path": "/dev/sda", "Rate": 1}]),
    ],
)
def test_undeclared_group_oom_sysctl_device_cgroup_dns_and_resource_overrides_refuse(field, value):
    record, image = configuration("consumer")
    record["HostConfig"][field] = value
    with pytest.raises(ValueError):
        runtime.verify_container(
            record, "a" * 64, "ours", image, "c" * 64, "sha256:" + "d" * 64, "consumer", "captured source"
        )
