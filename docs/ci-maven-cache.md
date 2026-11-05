# CI Maven cache seeding

The workflows seed missing third-party artifacts from the fileserver's
`data/enterprise-temp/tapdata/repository/` into the runner's
`/root/.m2/repository/`. This source layout was verified using a read-only
`rsync --list-only` from an office-scan runner. Credentials come from the
existing repository Secret `RSYNC_PASSWORD`; never put its value in Git.

## Intended behavior

- `--ignore-existing` is a seed operation, not an authoritative mirror.
  Existing settings and private `com/tapdata` and `io/tapdata` artifacts are
  not overwritten or deleted.
- Interrupted downloads use `--partial-dir=.rsync-partial`, keeping partial
  data separate from final artifact filenames. Do not replace it with bare
  `--partial` or `--inplace` when using `--ignore-existing`.
- Default rsync without partial retention removes incomplete transfers.
  Lack of `--partial` does not itself create a corrupted final artifact.

## Repairing an existing stale or damaged artifact

The seed does not validate or refresh existing files. A Maven `-U` build
refreshes eligible snapshots and retries missing artifacts, but does not
guarantee replacement of an already-present corrupt release JAR.

1. Identify the exact GAV, runner and failing local artifact using Maven logs.
2. Pause jobs using that runner's private cache before changing files.
3. Verify the corresponding artifact can be fetched from Nexus.
4. Move only the affected version directory to a protected backup outside
   `repository/`, then rerun Maven against the normal Nexus mirror.
5. Verify resolution/checksums before discarding the backup.

Do not clear the entire `.m2`, overwrite `settings.xml`, or rsync-delete the
cache. Keep any backup, credentials and runtime logs out of Git.

## Sonar build lifecycle

`clean install -P idaas` intentionally runs package/install after unit tests,
making reactor artifacts available to the separate Sonar Maven invocation.
This adds packaging work; no tests are skipped to compensate. The total scan
job timeout and quality-gate polling budget remain separate limits.

Gitee-backed scans currently support same-repository PR branches only.
Fork source branches are not mirrored by this workflow. Source and target
SHA checks fail closed on mirror lag rather than scanning another revision.
