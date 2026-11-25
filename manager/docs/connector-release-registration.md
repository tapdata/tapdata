# Connector release registration

The catalog is public: enterprise release artifacts are publicly readable, and catalog refreshes are cached and serialized. Download, impact inspection, and job status endpoints remain enterprise operations. Opening the catalog never loads connector classes.

## Release source and integrity

Registration trusts the Nexus repository configured by `pdk.release.repository-url`, which defaults to `https://nexus.tapdata.net/repository/maven-releases/`. The BOM selects the published connector versions. Online updates do not require a separate artifact digest allowlist or a Manager configuration change for each release.

Before registration, the downloaded JAR must match the repository's `.sha256` checksum. The checksum covers the **published artifact bytes before decryption**. Missing or mismatched checksums reject the download and delete the temporary file. This verifies download integrity; trust in executable connector code comes from the configured repository and its release process.

Registration uses the same PDK loader and metadata builder as the manual CLI. It instantiates connector code to obtain capabilities, codecs, and node types inside the manager JVM.

## Download and lifecycle

Artifacts stream directly into an owner-only temporary file, with SHA-256 calculated during the download. The 1 GiB limit is checked against Content-Length and actual bytes received. XML metadata is limited to 4 MiB and checksums to 4 KiB.

Connections have a five-second connect timeout and a socket read timeout of thirty seconds (`pdk.release.read-timeout-millis`). Catalog and checksum reads have a ten-second deadline; artifact downloads have a fifteen-minute deadline, checked between reads. A stalled socket read is always bounded by its read timeout. Redirects are rejected.

The temporary file remains available until upload finishes and the PDK archive closes. It is deleted on success or failure. This removes the full-artifact byte arrays introduced by the HTTP and multipart registration layers. The shared PDK loader still performs its own decryption and class loading, which can allocate artifact-sized buffers; this change does not claim a hard bound on total manager heap use.

## Persistence failures

Heartbeat persistence failures are logged and retried at the next heartbeat. Local active jobs are not marked interrupted from an old database snapshot. Stale-job interruption uses an atomic status/timestamp comparison so it cannot overwrite a concurrent heartbeat or completion.

Uploads that fail before a definition write clean up their newly stored resources. Once a save/upsert is attempted, an exception may mean that Mongo committed but its acknowledgement was lost. Both old and new resources are retained on that path, and the error is propagated. This may leave orphaned files requiring reconciliation, but avoids deleting files referenced by a committed definition. Old-resource deletion is scheduled only after a confirmed successful definition update.
