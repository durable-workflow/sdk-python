import copy
from typing import Any

import pytest

from durable_workflow.auth_composition import (
    AUTH_COMPOSITION_CONTRACT_SCHEMA,
    AUTH_COMPOSITION_CONTRACT_VERSION,
    AuthCompositionContractError,
    parse_auth_composition_contract,
)


def auth_composition_manifest() -> dict[str, Any]:
    return {
        "schema": AUTH_COMPOSITION_CONTRACT_SCHEMA,
        "version": AUTH_COMPOSITION_CONTRACT_VERSION,
        "scope": "external_execution_carriers",
        "unknown_field_policy": "ignore_additive_reject_unknown_required",
        "precedence": {
            "connection_values": ["flag", "environment", "selected_profile", "default"],
            "profile_selection": ["flag_env", "DW_ENV", "current_profile", "default_profile"],
        },
        "canonical_environment": {
            "server_url": "DURABLE_WORKFLOW_SERVER_URL",
            "namespace": "DURABLE_WORKFLOW_NAMESPACE",
            "auth_token": "DURABLE_WORKFLOW_AUTH_TOKEN",
            "tls_verify": "DURABLE_WORKFLOW_TLS_VERIFY",
            "profile": "DW_ENV",
        },
        "auth_material": {
            "token": {
                "status": "supported",
                "transport": "bearer_authorization_header",
                "persisted_as": "secret_reference_or_profile_env_reference",
                "effective_config_value": "redacted",
            },
            "mtls": {
                "status": "reserved",
                "persisted_as": "certificate_and_key_references",
                "effective_config_value": "references_only",
            },
            "signed_headers": {
                "status": "reserved",
                "persisted_as": "key_reference_and_header_allowlist",
                "effective_config_value": "references_only",
            },
        },
        "effective_config": {
            "required_fields": ["server_url", "namespace", "profile", "auth", "tls", "identity"],
            "source_values": ["flag", "environment", "selected_profile", "profile_env", "default", "server"],
        },
        "redaction": {
            "never_echo": [
                "bearer_tokens",
                "private_keys",
                "shared_signature_keys",
                "client_certificate_private_key_material",
                "raw_authorization_headers",
            ],
            "allowed_diagnostics": [
                "redacted",
                "secret_reference_name",
                "environment_variable_name",
                "profile_name",
                "certificate_reference_name",
                "key_reference_name",
            ],
        },
    }


def test_auth_composition_contract_parses_server_manifest() -> None:
    contract = parse_auth_composition_contract(auth_composition_manifest())

    assert contract.schema == "durable-workflow.v2.auth-composition.contract"
    assert contract.version == 1
    assert contract.connection_precedence == ("flag", "environment", "selected_profile", "default")
    assert contract.profile_precedence == ("flag_env", "DW_ENV", "current_profile", "default_profile")
    assert contract.canonical_environment["auth_token"] == "DURABLE_WORKFLOW_AUTH_TOKEN"
    assert contract.supports_token_auth is True
    assert contract.reserves_mtls is True
    assert contract.reserves_signed_headers is True
    assert "identity" in contract.effective_config_required_fields
    assert "bearer_tokens" in contract.redaction_never_echo


@pytest.mark.parametrize(
    ("path", "value", "message"),
    [
        (("schema",), "wrong.schema", "schema"),
        (("version",), 2, "version"),
        (("canonical_environment", "auth_token"), "TOKEN", "auth_token"),
        (("auth_material", "token", "effective_config_value"), "plain", "token auth"),
        (("auth_material", "mtls", "status"), "supported", "mtls"),
        (("redaction", "never_echo"), ["private_keys", "raw_authorization_headers"], "bearer_tokens"),
        (("effective_config", "required_fields"), ["server_url"], "namespace"),
    ],
)
def test_auth_composition_contract_rejects_incompatible_manifest(
    path: tuple[str, ...], value: object, message: str
) -> None:
    manifest = auth_composition_manifest()
    cursor: dict[str, Any] = manifest
    for key in path[:-1]:
        cursor = cursor[key]
    cursor[path[-1]] = value

    with pytest.raises(AuthCompositionContractError, match=message):
        parse_auth_composition_contract(manifest)


def test_auth_composition_contract_ignores_additive_fields() -> None:
    manifest = copy.deepcopy(auth_composition_manifest())
    manifest["future"] = {"ignored": True}
    manifest["auth_material"]["extensions"] = {"status": "reserved"}

    contract = parse_auth_composition_contract(manifest)

    assert contract.auth_material["extensions"]["status"] == "reserved"
