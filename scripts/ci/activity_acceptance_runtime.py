"""Exact container and network authority for the ordinary published SDK cell."""

from __future__ import annotations

import ipaddress
import re
from pathlib import Path

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
    return image["Config"].get("Entrypoint"), image["Config"].get("Cmd"), image["Config"].get("WorkingDir", "")


def bind_local_image(index_record, platform_record, index_digest, child_digest, config_digest):
    if platform_record["Architecture"] != "amd64" or platform_record["Os"] != "linux":
        raise ValueError("local platform image differs")
    for record in (index_record, platform_record):
        if not any(value.rsplit("@", 1)[-1] == index_digest for value in record["RepoDigests"]):
            raise ValueError("local image lacks the selected immutable index")
    descriptor = platform_record.get("Descriptor")
    if index_record["Id"] == config_digest and platform_record["Id"] == config_digest:
        store = "classic"
    elif index_record["Id"] == index_digest and platform_record["Id"] == child_digest:
        store = "containerd"
    else:
        raise ValueError("local image IDs do not bind the selected manifest/config")
    if (store == "containerd" or descriptor is not None) and (
        descriptor is None
        or descriptor.get("digest") != child_digest
        or descriptor.get("platform") != {"architecture": "amd64", "os": "linux"}
    ):
        raise ValueError("local platform descriptor differs")
    return {
        **platform_record,
        "CreateReference": index_record["Id"],
        "PlatformConfigDigest": config_digest,
        "ImageStore": store,
    }


def environment_values(entries):
    values = {}
    for entry in entries:
        key, value = entry.split("=", 1)
        values[key] = value
    return values


def verify_config(config, identity, nonce, image, role, bootstrap):
    environment = environment_values(image["Config"]["Env"])
    if role == "runtime":
        environment.update(ENVIRONMENT)
    received = environment_values(config["Env"])
    if len(received) != len(config["Env"]) or received != environment:
        raise ValueError("container environment differs")
    entrypoint, command, directory = expected_command(image, role, bootstrap)
    labels = {**(image["Config"].get("Labels") or {}), OWNER: "sdk-python", RUN: nonce, ROLE: role}
    expected = {
        "Hostname": identity[:12],
        "Domainname": "",
        "Image": image.get("CreateReference", image["Id"]),
        "User": "1000:1000",
        "Entrypoint": entrypoint,
        "Cmd": command,
        "WorkingDir": directory,
        "Tty": False,
        "OpenStdin": role == "consumer",
        "StdinOnce": role == "consumer",
        "AttachStdin": role == "consumer",
        "AttachStdout": True,
        "AttachStderr": True,
        "Labels": labels,
        "Volumes": image["Config"].get("Volumes"),
        "ExposedPorts": image["Config"].get("ExposedPorts"),
        "Healthcheck": image["Config"].get("Healthcheck"),
        "StopSignal": image["Config"].get("StopSignal", ""),
        "StopTimeout": image["Config"].get("StopTimeout"),
        "Shell": image["Config"].get("Shell"),
        "OnBuild": image["Config"].get("OnBuild"),
        "ArgsEscaped": image["Config"].get("ArgsEscaped", False),
        "NetworkDisabled": False,
        "MacAddress": "",
    }
    if set(config) - (set(expected) | {"Env"}):
        raise ValueError("container has an unreviewed Config field")
    optional = {
        "ExposedPorts": None,
        "Healthcheck": None,
        "StopSignal": "",
        "StopTimeout": None,
        "Shell": None,
        "OnBuild": None,
        "ArgsEscaped": False,
        "NetworkDisabled": False,
        "MacAddress": "",
    }
    if (set(expected) - set(optional)) - set(config):
        raise ValueError("container Config lacks a required observation")
    for key, value in expected.items():
        received_value = config.get(key, optional[key]) if key in optional else config[key]
        if received_value != value:
            raise ValueError("container Config differs from the closed image/role projection")


def verify_container(record, identity, nonce, image, network, child_digest, role, bootstrap="", *, cidfile=""):
    require_owner(record, identity, nonce, role)
    verify_config(record["Config"], identity, nonce, image, role, bootstrap)
    if record["Image"] != image.get("CreateReference", image["Id"]):
        raise ValueError("container image identity differs")
    descriptor = record.get("ImageManifestDescriptor")
    if descriptor is not None and descriptor.get("digest") != child_digest:
        raise ValueError("container platform child differs")
    if image.get("ImageStore") == "containerd" and (
        descriptor is None or descriptor.get("platform") != {"os": "linux", "architecture": "amd64"}
    ):
        raise ValueError("containerd platform descriptor is missing or different")
    verify_host(record["HostConfig"], role, network, cidfile=cidfile)
    if any(mount["Type"] != "tmpfs" or mount["Destination"] not in TMPFS[role] for mount in record["Mounts"]):
        raise ValueError("container has an undeclared mount")


MASKED_PATHS = {
    "/proc/acpi",
    "/proc/asound",
    "/proc/interrupts",
    "/proc/kcore",
    "/proc/keys",
    "/proc/latency_stats",
    "/proc/sched_debug",
    "/proc/scsi",
    "/proc/timer_list",
    "/proc/timer_stats",
    "/sys/devices/virtual/powercap",
    "/sys/firmware",
}
READONLY_PATHS = {"/proc/bus", "/proc/fs", "/proc/irq", "/proc/sys", "/proc/sysrq-trigger"}


def expected_masked_paths():
    # The fixed local daemon also masks the kernel's CPU thermal-throttle paths.
    return MASKED_PATHS | {str(path) for path in Path("/sys/devices/system/cpu").glob("cpu[0-9]*/thermal_throttle")}


def verify_protected_paths(host):
    for field, expected in (("MaskedPaths", expected_masked_paths()), ("ReadonlyPaths", READONLY_PATHS)):
        actual = host.get(field)
        if not isinstance(actual, list) or len(actual) != len(set(actual)) or set(actual) != expected:
            raise ValueError("container protected system paths differ")


def verify_host(host, role, network, *, cidfile=""):
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
        "ContainerIDFile": cidfile,
        "ConsoleSize": [0, 0],
        "MemorySwappiness": None,
        "ShmSize": 67_108_864,
        "Runtime": "runc",
        "IpcMode": "private",
        "CgroupnsMode": "private",
        "RestartPolicy": {"Name": "no", "MaximumRetryCount": 0},
        "LogConfig": {"Type": "json-file", "Config": {"max-size": "1m", "max-file": "1"}},
    }
    if any(host.get(key) != value for key, value in expected.items()):
        raise ValueError("container resource/network/security configuration differs")
    if "OomKillDisable" not in host or (host["OomKillDisable"] is not False and host["OomKillDisable"] is not None):
        raise ValueError("container disables the OOM killer or lacks its lifecycle value")
    verify_protected_paths(host)
    zero_fields = (
        "IOMaximumIOps",
        "IOMaximumBandwidth",
        "CpuShares",
        "CpuPeriod",
        "CpuQuota",
        "CpuRealtimePeriod",
        "CpuRealtimeRuntime",
        "OomScoreAdj",
        "MemoryReservation",
        "BlkioWeight",
        "CpuCount",
        "CpuPercent",
        "KernelMemory",
        "KernelMemoryTCP",
    )
    if any(host.get(key, 0) != 0 for key in zero_fields):
        raise ValueError("container has an undeclared resource override")
    forbidden = (
        "Annotations",
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
        "Mounts",
        "VolumeDriver",
        "Isolation",
        "GroupAdd",
        "Sysctls",
        "DeviceCgroupRules",
        "CgroupParent",
        "Cgroup",
        "CpusetCpus",
        "CpusetMems",
        "Ulimits",
        "Dns",
        "DnsOptions",
        "DnsSearch",
        "StorageOpt",
        "BlkioWeightDevice",
        "BlkioDeviceReadBps",
        "BlkioDeviceWriteBps",
        "BlkioDeviceReadIOps",
        "BlkioDeviceWriteIOps",
    )
    known = set(expected) | set(zero_fields) | set(forbidden) | {"OomKillDisable", "MaskedPaths", "ReadonlyPaths"}
    if set(host) - known:
        raise ValueError("container has an unreviewed HostConfig field")
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
        "--platform",
        "linux/amd64",
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
            "--runtime",
            "runc",
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
            image.get("CreateReference", image["Id"]),
            "-I",
            "-B",
            "-c",
            bootstrap,
        ]
    for key, value in ENVIRONMENT.items():
        arguments.extend(["-e", key + "=" + value])
    return [*arguments, image.get("CreateReference", image["Id"])]
