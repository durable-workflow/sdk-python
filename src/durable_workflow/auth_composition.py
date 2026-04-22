from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

AUTH_COMPOSITION_CONTRACT_SCHEMA = "durable-workflow.v2.auth-composition.contract"
AUTH_COMPOSITION_CONTRACT_VERSION = 1

AUTH_COMPOSITION_REQUIRED_EFFECTIVE_CONFIG_FIELDS = (
    "server_url",
    "namespace",
    "profile",
    "auth",
    "tls",
    "identity",
)


@dataclass(frozen=True)
class AuthCompositionContractError(ValueError):
    """Raised when the server auth-composition manifest is not compatible."""

    message: str

    def __str__(self) -> str:
        return self.message


@dataclass(frozen=True)
class AuthCompositionContract:
    schema: str
    version: int
    connection_precedence: tuple[str, ...]
    profile_precedence: tuple[str, ...]
    canonical_environment: Mapping[str, str]
    auth_material: Mapping[str, Mapping[str, Any]]
    effective_config_required_fields: tuple[str, ...]
    redaction_never_echo: tuple[str, ...]

    @property
    def supports_token_auth(self) -> bool:
        token = self.auth_material.get("token")
        return token is not None and token.get("status") == "supported"

    @property
    def reserves_mtls(self) -> bool:
        mtls = self.auth_material.get("mtls")
        return mtls is not None and mtls.get("status") == "reserved"

    @property
    def reserves_signed_headers(self) -> bool:
        signed_headers = self.auth_material.get("signed_headers")
        return signed_headers is not None and signed_headers.get("status") == "reserved"


def parse_auth_composition_contract(manifest: Mapping[str, Any]) -> AuthCompositionContract:
    """Parse and validate the v1 carrier auth-composition contract manifest."""

    _require_value(manifest, "schema", AUTH_COMPOSITION_CONTRACT_SCHEMA)
    _require_value(manifest, "version", AUTH_COMPOSITION_CONTRACT_VERSION)

    precedence = _require_mapping(manifest, "precedence")
    canonical_environment = _parse_str_mapping(_require_mapping(manifest, "canonical_environment"))
    auth_material = _parse_auth_material(_require_mapping(manifest, "auth_material"))
    effective_config = _require_mapping(manifest, "effective_config")
    redaction = _require_mapping(manifest, "redaction")

    required_fields = _require_str_sequence(effective_config, "required_fields")
    missing_effective_fields = set(AUTH_COMPOSITION_REQUIRED_EFFECTIVE_CONFIG_FIELDS).difference(required_fields)
    if missing_effective_fields:
        fields = ", ".join(sorted(missing_effective_fields))
        raise AuthCompositionContractError(
            f"Auth composition contract effective_config.required_fields missing [{fields}]."
        )

    _require_env(canonical_environment, "server_url", "DURABLE_WORKFLOW_SERVER_URL")
    _require_env(canonical_environment, "namespace", "DURABLE_WORKFLOW_NAMESPACE")
    _require_env(canonical_environment, "auth_token", "DURABLE_WORKFLOW_AUTH_TOKEN")
    _require_env(canonical_environment, "tls_verify", "DURABLE_WORKFLOW_TLS_VERIFY")
    _require_env(canonical_environment, "profile", "DW_ENV")

    token = auth_material.get("token")
    if token is None or token.get("status") != "supported" or token.get("effective_config_value") != "redacted":
        raise AuthCompositionContractError("Auth composition contract must support redacted token auth.")

    for reserved in ("mtls", "signed_headers"):
        material = auth_material.get(reserved)
        if material is None or material.get("status") != "reserved":
            raise AuthCompositionContractError(
                f"Auth composition contract must reserve [{reserved}] auth material."
            )

    never_echo = _require_str_sequence(redaction, "never_echo")
    for secret in ("bearer_tokens", "private_keys", "raw_authorization_headers"):
        if secret not in never_echo:
            raise AuthCompositionContractError(
                f"Auth composition contract redaction.never_echo missing [{secret}]."
            )

    return AuthCompositionContract(
        schema=AUTH_COMPOSITION_CONTRACT_SCHEMA,
        version=AUTH_COMPOSITION_CONTRACT_VERSION,
        connection_precedence=tuple(_require_str_sequence(precedence, "connection_values")),
        profile_precedence=tuple(_require_str_sequence(precedence, "profile_selection")),
        canonical_environment=canonical_environment,
        auth_material=auth_material,
        effective_config_required_fields=tuple(required_fields),
        redaction_never_echo=tuple(never_echo),
    )


def _parse_auth_material(value: Mapping[str, Any]) -> Mapping[str, Mapping[str, Any]]:
    parsed: dict[str, Mapping[str, Any]] = {}
    for key, material in value.items():
        if not isinstance(key, str):
            raise AuthCompositionContractError("Auth composition auth_material keys must be strings.")
        if not isinstance(material, Mapping):
            raise AuthCompositionContractError(
                f"Auth composition auth_material field [{key}] must be an object."
            )
        parsed[key] = material
    return parsed


def _parse_str_mapping(value: Mapping[str, Any]) -> Mapping[str, str]:
    parsed: dict[str, str] = {}
    for key, item in value.items():
        if not isinstance(key, str):
            raise AuthCompositionContractError("Auth composition mapping keys must be strings.")
        if not isinstance(item, str):
            raise AuthCompositionContractError(f"Auth composition field [{key}] must be a string.")
        parsed[key] = item
    return parsed


def _require_mapping(value: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    if key not in value:
        raise AuthCompositionContractError(f"Auth composition contract is missing required field [{key}].")
    item = value[key]
    if not isinstance(item, Mapping):
        raise AuthCompositionContractError(f"Auth composition contract field [{key}] must be an object.")
    return item


def _require_str_sequence(value: Mapping[str, Any], key: str) -> Sequence[str]:
    if key not in value:
        raise AuthCompositionContractError(f"Auth composition contract is missing required field [{key}].")
    item = value[key]
    if not isinstance(item, Sequence) or isinstance(item, str | bytes):
        raise AuthCompositionContractError(f"Auth composition contract field [{key}] must be a list.")
    if not all(isinstance(element, str) for element in item):
        raise AuthCompositionContractError(f"Auth composition contract field [{key}] must contain only strings.")
    return item


def _require_value(value: Mapping[str, Any], key: str, expected: object) -> None:
    if key not in value:
        raise AuthCompositionContractError(f"Auth composition contract is missing required field [{key}].")
    if value[key] != expected:
        raise AuthCompositionContractError(
            f"Auth composition contract field [{key}] must be [{expected}], got [{value[key]}]."
        )


def _require_env(value: Mapping[str, str], key: str, expected: str) -> None:
    actual = value.get(key)
    if actual != expected:
        raise AuthCompositionContractError(
            f"Auth composition canonical_environment.{key} must be [{expected}], got [{actual}]."
        )
