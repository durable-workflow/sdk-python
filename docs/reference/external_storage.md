# External Storage

## Runtime-mediated transport

`Client` automatically uses the namespace runtime's external-payload transport
when cluster discovery advertises it. Payloads above the discovered inline
threshold are uploaded with the same runtime URL, namespace, and role
credential already used by the client or worker. Workflow requests and worker
completions carry a provider-neutral opaque reference; the backing bucket,
container, filesystem path, and provider credentials remain inside Server or
managed Cloud.

The same mediation applies to payload envelopes nested in workflow and
activity polls and completions, signals, queries, updates, schedules, streams,
and history exports. Incoming bytes are bounded by the runtime's advertised
maximum and checked against both `size_bytes` and `sha256` before Avro decode.
A bounded verified-byte cache can reuse content during replay. Cache eviction
only removes SDK memory: there is no client delete operation for runtime-owned
objects.

Failures are available as typed exceptions:

- `ExternalPayloadNotFound`
- `ExternalPayloadExpired`
- `ExternalPayloadUnauthorized`
- `ExternalPayloadUnavailable` (retryable)
- `ExternalPayloadOversized`
- `ExternalPayloadUnsupported`
- `ExternalPayloadIntegrityMismatch`

An unresolved or malformed runtime reference fails closed and is never
returned as decoded workflow user data.

## Direct self-hosted adapters

`LocalFilesystemExternalStorage`, `S3ExternalStorage`,
`GCSExternalStorage`, and `AzureBlobExternalStorage` are explicit self-hosted
integrations. Use them only with a runtime that has separately advertised
direct-reference acceptance, and pass the selected driver through
`Client(external_storage=..., external_storage_threshold_bytes=...)` or the
serializer helpers. The SDK never chooses one because namespace discovery
names the runtime's backing driver.

Provider clients are application-owned optional objects; they are not Durable
Workflow SDK dependencies. Direct adapters retain the legacy typed-reference
cleanup helper for application-owned objects. Runtime-owned opaque references
have no SDK delete path.

::: durable_workflow.external_storage
