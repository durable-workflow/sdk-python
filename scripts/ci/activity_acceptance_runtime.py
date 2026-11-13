"""Exact container and network authority for the ordinary published SDK cell."""

from __future__ import annotations

import ipaddress
import re

OWNER = "dev.durable-workflow.fixture-owner"
RUN = "dev.durable-workflow.fixture-run"
ROLE = "dev.durable-workflow.fixture-role"
PRIVATE_64M = "noexec,nosuid,nodev,uid=1000,gid=1000,mode=0700,size=64m"
TMPFS = {
    "runtime": {
        "/run/runtime-tmp": "noexec,nosuid,nodev,uid=1000,gid=1000,mode=0700,size=128m",
        "/app/database": "noexec,nosuid,nodev,uid=1000,gid=1000,mode=0700,size=128m",
        "/app/storage": PRIVATE_64M,
    },
    "consumer": {
        "/run/acceptance-data": PRIVATE_64M,
        "/run/acceptance-wheels": PRIVATE_64M,
        "/run/acceptance-env": "exec,nosuid,nodev,uid=1000,gid=1000,mode=0700,size=256m",
    },
}
ENVIRONMENT = {
    "TMPDIR": "/run/runtime-tmp",
    "DB_CONNECTION": "sqlite",
    "DB_DATABASE": "/app/database/activity-identity.sqlite",
    "DW_AUTH_TOKEN": "published-activity-test-token",
    "DW_RUNTIME_CREDENTIALS_ENABLED": "false",
}


def full_id(value):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def created_identity(stdout, cidfile):
    """Retain both valid channels for cleanup, even when admission must refuse."""
    captured, printed = cidfile.strip(), stdout.strip()
    identity = None
    if full_id(captured):
        identity = captured
    elif full_id(printed):
        identity = printed
    candidates = sorted({value for value in (captured, printed) if full_id(value)})
    return identity, bool(identity and printed == identity and captured == identity), candidates


def require_owner(record, identity, nonce, role):
    labels = record["Config"]["Labels"] or {}
    if (
        not full_id(identity)
        or record["Id"] != identity
        or labels.get(OWNER) != "sdk-python"
        or labels.get(RUN) != nonce
        or labels.get(ROLE) != role
    ):
        raise ValueError("container cleanup ownership differs")


def verify_network(record, identity, nonce):
    expected = {
        "Id": identity,
        "Driver": "bridge",
        "Internal": True,
        "Attachable": False,
        "Ingress": False,
        "EnableIPv6": False,
    }
    labels = record["Labels"] or {}
    if (
        not full_id(identity)
        or any(record.get(key) != value for key, value in expected.items())
        or labels.get(OWNER) != "sdk-python"
        or labels.get(RUN) != nonce
        or labels.get(ROLE) != "network"
    ):
        raise ValueError("owned internal network differs")


def expected_command(image, role, bootstrap):
    if role == "consumer":
        return ["/usr/local/bin/python3"], ["-I", "-B", "-c", bootstrap], "/"
    return image["Config"]["Entrypoint"], image["Config"]["Cmd"], image["Config"]["WorkingDir"]


def verify_container(record, identity, nonce, image, network, child_digest, role, bootstrap=""):
    require_owner(record, identity, nonce, role)
    config, host = record["Config"], record["HostConfig"]
    environment = {key: value for key, value in (item.split("=", 1) for item in image["Config"]["Env"])}
    if role == "runtime":
        environment.update(ENVIRONMENT)
    received = {key: value for key, value in (item.split("=", 1) for item in config["Env"])}
    if len(received) != len(config["Env"]) or received != environment:
        raise ValueError("container environment differs")
    entrypoint, command, working_directory = expected_command(image, role, bootstrap)
    expected = {
        "Image": image["Id"],
        "User": "1000:1000",
        "Entrypoint": entrypoint,
        "Cmd": command,
        "WorkingDir": working_directory,
        "Tty": False,
        "OpenStdin": role == "consumer",
    }
    if record["Image"] != image["Id"] or any(config.get(key) != value for key, value in expected.items()):
        raise ValueError("container image or command differs")
    descriptor = record.get("ImageManifestDescriptor")
    if descriptor is not None and descriptor.get("digest") != child_digest:
        raise ValueError("container platform child differs")
    verify_host(host, role, network)
    if any(mount["Type"] != "tmpfs" or mount["Destination"] not in TMPFS[role] for mount in record["Mounts"]):
        raise ValueError("container has an undeclared mount")


def verify_host(host, role, network):
    expected = {
        "NetworkMode": network,
        "ReadonlyRootfs": True,
        "Privileged": False,
        "NanoCpus": 1_000_000_000,
        "Memory": 1_073_741_824,
        "MemorySwap": 1_073_741_824,
        "PidsLimit": 256,
        "Init": True,
        "CapDrop": ["ALL"],
        "SecurityOpt": ["no-new-privileges"],
        "Tmpfs": TMPFS[role],
        "PublishAllPorts": False,
        "AutoRemove": False,
        "IpcMode": "private",
        "CgroupnsMode": "private",
        "RestartPolicy": {"Name": "no", "MaximumRetryCount": 0},
        "LogConfig": {"Type": "json-file", "Config": {"max-size": "1m", "max-file": "1"}},
    }
    if any(host.get(key) != value for key, value in expected.items()):
        raise ValueError("container resource/network/security configuration differs")
    forbidden = (
        "Binds",
        "VolumesFrom",
        "Devices",
        "DeviceRequests",
        "CapAdd",
        "ExtraHosts",
        "PortBindings",
        "PidMode",
        "UTSMode",
        "UsernsMode",
        "Links",
    )
    if any(host.get(key) for key in forbidden):
        raise ValueError("container has an undeclared host resource")


def endpoint(record, network):
    endpoints = list(record["NetworkSettings"]["Networks"].values())
    if len(endpoints) != 1 or endpoints[0]["NetworkID"] != network:
        raise ValueError("container network attachment differs")
    address = ipaddress.ip_address(endpoints[0]["IPAddress"])
    if address.version != 4 or not address.is_private or address.is_loopback or address.is_unspecified:
        raise ValueError("runtime address is not a private numeric IPv4 endpoint")
    return str(address)


def verify_attachments(network, runtime_record, consumer_record):
    identities = {runtime_record["Id"], consumer_record["Id"]}
    if set(network["Containers"]) != identities:
        raise ValueError("internal network has missing or foreign attachments")
    address = endpoint(runtime_record, network["Id"])
    consumer_address = endpoint(consumer_record, network["Id"])
    if address == consumer_address:
        raise ValueError("container addresses are not distinct")
    for record in (runtime_record, consumer_record):
        actual = endpoint(record, network["Id"])
        if network["Containers"][record["Id"]]["IPv4Address"].split("/", 1)[0] != actual:
            raise ValueError("network and container endpoint observations differ")
    subnets = [ipaddress.ip_network(item["Subnet"]) for item in network["IPAM"]["Config"]]
    if not any(ipaddress.ip_address(address) in subnet for subnet in subnets):
        raise ValueError("runtime address differs from network IPAM")
    return address


def create_arguments(image, network, nonce, cidfile, role, bootstrap=""):
    arguments = [
        "docker",
        "create",
        "--pull=never",
        "--cidfile",
        str(cidfile),
        "--name",
        "sdk-python-activity-" + role + "-" + nonce[:12],
    ]
    for key, value in ((OWNER, "sdk-python"), (RUN, nonce), (ROLE, role)):
        arguments.extend(["--label", key + "=" + value])
    arguments.extend(
        [
            "--init",
            "--read-only",
            "--network",
            network,
            "--cpus",
            "1",
            "--memory",
            "1g",
            "--memory-swap",
            "1g",
            "--pids-limit",
            "256",
            "--user",
            "1000:1000",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--ipc",
            "private",
            "--cgroupns",
            "private",
            "--restart",
            "no",
            "--log-driver",
            "json-file",
            "--log-opt",
            "max-size=1m",
            "--log-opt",
            "max-file=1",
        ]
    )
    for target, options in TMPFS[role].items():
        arguments.extend(["--tmpfs", target + ":" + options])
    if role == "consumer":
        return [
            *arguments,
            "-i",
            "--workdir",
            "/",
            "--entrypoint",
            "/usr/local/bin/python3",
            image["Id"],
            "-I",
            "-B",
            "-c",
            bootstrap,
        ]
    for key, value in ENVIRONMENT.items():
        arguments.extend(["-e", key + "=" + value])
    return [*arguments, image["Id"]]
