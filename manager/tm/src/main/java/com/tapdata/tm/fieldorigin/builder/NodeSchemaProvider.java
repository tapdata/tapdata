package com.tapdata.tm.fieldorigin.builder;

import java.util.List;

/**
 * Loads the derived models of a task node, one schema per table.
 */
@FunctionalInterface
public interface NodeSchemaProvider {
    List<NodeSchema> schemas(String nodeId);
}
