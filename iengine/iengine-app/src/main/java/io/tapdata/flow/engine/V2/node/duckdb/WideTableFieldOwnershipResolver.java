package io.tapdata.flow.engine.V2.node.duckdb;

import java.util.Collections;
import java.util.Set;

@FunctionalInterface
public interface WideTableFieldOwnershipResolver {
    Set<String> resolveOwnedFields(String sourceTableName);

    static WideTableFieldOwnershipResolver noop() {
        return sourceTableName -> Collections.emptySet();
    }
}
