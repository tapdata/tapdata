# Connector release registration

The catalog is public: enterprise release artifacts are publicly readable, and catalog refreshes are cached and serialized. Download, impact inspection, and job status endpoints remain enterprise operations. Opening the catalog never loads connector classes.

## Approving connector execution

Registration uses the same PDK loader and metadata builder as the manual CLI. It instantiates connector code to obtain capabilities, codecs, and node types. This is code execution inside the manager JVM, not a sandbox.

Before registration, administrators must configure `pdk.release.trusted-sha256` with a comma- or whitespace-separated list of approved artifact SHA-256 digests. The value defaults to empty, which rejects registration. Catalog browsing and existing connections continue to work.

Approve the **published artifact bytes before decryption**, using an independent release review or trusted build output. Do not automatically import checksums from Nexus into this configuration: a compromised publisher could replace both the JAR and its checksum. Add the independently approved digest when upgrading a connector. The Nexus `.sha256` integrity check is still required in addition to deployment approval. Neither HTTP requests nor catalog responses can modify the approval list.

Approving a digest trusts that exact artifact's executable code. This does not isolate approved code from the manager or bound its runtime memory use.

## Download and lifecycle

Artifacts stream directly into an owner-only temporary file, with SHA-256 calculated during the download. The 1 GiB limit is checked against Content-Length and actual bytes received. XML metadata is limited to 4 MiB and checksums to 4 KiB.

Connections have a five-second connect timeout and a socket read timeout of thirty seconds (`pdk.release.read-timeout-millis`). Catalog and checksum reads have a ten-second deadline; artifact downloads have a fifteen-minute deadline, checked between reads. A stalled socket read is always bounded by its read timeout. Redirects are rejected.

The temporary file remains available until upload finishes and the PDK archive closes. It is deleted on success or failure. This removes the full-artifact byte arrays introduced by the HTTP and multipart registration layers. The shared PDK loader still performs its own decryption and class loading, which can allocate artifact-sized buffers; this change does not claim a hard bound on total manager heap use.

## Persistence failures

Heartbeat persistence failures are logged and retried at the next heartbeat. Local active jobs are not marked interrupted from an old database snapshot. Stale-job interruption uses an atomic status/timestamp comparison so it cannot overwrite a concurrent heartbeat or completion.

Uploads that fail before a definition write clean up their newly stored resources. Once a save/upsert is attempted, an exception may mean that Mongo committed but its acknowledgement was lost. Both old and new resources are retained on that path, and the error is propagated. This may leave orphaned files requiring reconciliation, but avoids deleting files referenced by a committed definition. Old-resource deletion is scheduled only after a confirmed successful definition update.
