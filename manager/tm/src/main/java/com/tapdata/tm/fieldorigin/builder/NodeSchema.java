package com.tapdata.tm.fieldorigin.builder;

import com.tapdata.tm.commons.schema.Field;

import java.util.List;

/**
 * Derived model of one table on one task node.
 */
public record NodeSchema(String nodeId, String tableName, List<Field> fields) {
}
