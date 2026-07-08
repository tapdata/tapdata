package io.tapdata.flow.engine.V2.node.duckdb.backup;

import org.bson.Document;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public record DuckDbBackupSnapshot(Path snapshotDir,
                                   List<Document> files,
                                   Map<String, Object> appliedOffset) {
}
