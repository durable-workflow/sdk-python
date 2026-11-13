"""A reused human name grants no authority over another fixture's resources."""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

PATH = Path(__file__).parents[1] / "scripts/conformance/command_keys/run_response_loss.py"
SPEC = importlib.util.spec_from_file_location("id_bound_runner", PATH)
assert SPEC is not None and SPEC.loader is not None
controller = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(controller)
OWN_ID, FOREIGN_ID = "a" * 64, "b" * 64
NONCE = "owned-run"


def document(identity, name, owner="sdk-python", nonce=NONCE):
    return {
        "Id": identity,
        "Name": "/" + name,
        "Config": {"Labels": {controller.OWNER_LABEL: owner, controller.RUN_LABEL: nonce}},
        "State": {"OOMKilled": False},
        "Mounts": [{"Type": "volume", "Name": identity + "-data"}],
    }


class DockerModel:
    def __init__(self, own_present=False):
        self.objects = {FOREIGN_ID: document(FOREIGN_ID, "reused-name", owner="foreign", nonce="foreign-run")}
        if own_present:
            self.objects[OWN_ID] = document(OWN_ID, "original-renamed")
        self.names = {value["Name"].removeprefix("/"): key for key, value in self.objects.items()}
        self.volumes = {value["Mounts"][0]["Name"] for value in self.objects.values()}
        self.calls = []

    def __call__(self, *args, **_options):
        self.calls.append(args)
        if args[0] == "inspect":
            identity = self.names.get(args[1], args[1])
            if identity in self.objects:
                return SimpleNamespace(returncode=0, stdout=json.dumps([self.objects[identity]]), stderr="")
            return SimpleNamespace(returncode=1, stdout="[]", stderr="error: no such object: " + args[1])
        if args[0] == "rm":
            identity = self.names.get(args[-1], args[-1])
            value = self.objects.pop(identity)
            self.volumes.remove(value["Mounts"][0]["Name"])
            self.names.pop(value["Name"].removeprefix("/"))
            return SimpleNamespace(returncode=0, stdout=identity, stderr="")
        if args[:2] == ("volume", "inspect"):
            if args[2] in self.volumes:
                return SimpleNamespace(returncode=0, stdout="[]", stderr="")
            return SimpleNamespace(
                returncode=1, stdout="[]", stderr="Error response from daemon: get " + args[2] + ": no such volume"
            )
        raise AssertionError("unexpected Docker operation")


@pytest.mark.parametrize("own_present", [False, True])
def test_foreign_same_name_and_its_volume_survive_id_bound_cleanup(own_present):
    docker = DockerModel(own_present)
    foreign = copy.deepcopy(docker.objects[FOREIGN_ID])
    result = controller.cleanup_owned(docker, OWN_ID, NONCE)
    assert result["absent"] and result["volumes_absent"]
    assert docker.objects[FOREIGN_ID] == foreign
    assert FOREIGN_ID + "-data" in docker.volumes
    assert all(call[-1] != "reused-name" for call in docker.calls)
    assert [call for call in docker.calls if call[0] == "rm"] == ([("rm", "-f", "-v", OWN_ID)] if own_present else [])


def test_predecessor_name_cleanup_deletes_the_foreign_replacement():
    predecessor = DockerModel()
    # The former controller resolved and removed by reusable name. The fake
    # engine demonstrates that exact boundary without touching a real resource.
    state = json.loads(predecessor("inspect", "reused-name").stdout)[0]
    assert state["Id"] == FOREIGN_ID
    predecessor("rm", "-f", "-v", "reused-name")
    assert FOREIGN_ID not in predecessor.objects and FOREIGN_ID + "-data" not in predecessor.volumes
    corrected = DockerModel()
    controller.cleanup_owned(corrected, OWN_ID, NONCE)
    assert FOREIGN_ID in corrected.objects and FOREIGN_ID + "-data" in corrected.volumes


@pytest.mark.parametrize(
    "mutation",
    [
        lambda value: value.update(Id=FOREIGN_ID),
        lambda value: value["Config"]["Labels"].update({controller.OWNER_LABEL: "foreign"}),
        lambda value: value["Config"]["Labels"].update({controller.RUN_LABEL: "different-run"}),
    ],
)
def test_mismatched_id_or_ownership_refuses_without_mutating_anything(mutation):
    docker = DockerModel(own_present=True)
    mutation(docker.objects[OWN_ID])
    with pytest.raises(ValueError, match="ownership identity differs"):
        controller.cleanup_owned(docker, OWN_ID, NONCE)
    assert not any(call[0] == "rm" for call in docker.calls)


@pytest.mark.parametrize("value", ["reused-name", "a" * 12, "A" * 64, "a" * 64 + "\nforeign", ""])
def test_only_a_full_created_id_can_authorize_operations(value):
    with pytest.raises(ValueError, match="full immutable container ID"):
        controller.created_id(value)
    assert controller.created_id(OWN_ID + "\n") == OWN_ID
