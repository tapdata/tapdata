package com.tapdata.tm.commons.dag;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.SchemaUtils;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Placeholder for a DAG node whose type is not registered in the running version, e.g. a node
 * left behind by a development build.
 * <p>
 * It keeps the original type and every property it does not understand, so reading such a task
 * does not fail and writing it back does not lose data. It is intentionally not annotated with
 * {@link NodeType}: it must never be selected by type, and tasks containing it are rejected at start.
 */
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class UnknownNode extends Node<Schema> {

    /**
     * Key under which Gson (which ignores Jackson annotations) writes {@link #preservedProperties}.
     * {@link com.tapdata.tm.base.convert.NodeSerialize} flattens it back for persistence.
     */
    public static final String PRESERVED_PROPERTIES_FIELD = "preservedProperties";

    /*
     * Deliberately neither @JsonIgnore (Jackson would propagate it to the same-named any-getter and drop
     * the data) nor transient (Java serialization deep copies would lose it). A private field without a
     * matching accessor is not auto-detected by Jackson, so it is only exposed through the any-getter.
     */
    private final Map<String, Object> preservedProperties = new LinkedHashMap<>();

    public UnknownNode() {
        super(null, NodeCatalog.processor);
    }

    @JsonAnySetter
    public void setUnknownProperty(String name, Object value) {
        if (PRESERVED_PROPERTIES_FIELD.equals(name) && value instanceof Map) {
            // Nested form produced by a Gson round trip: merge it back instead of nesting it again.
            ((Map<?, ?>) value).forEach((k, v) -> preservedProperties.putIfAbsent(String.valueOf(k), v));
            return;
        }
        preservedProperties.put(name, value);
    }

    @JsonAnyGetter
    public Map<String, Object> getUnknownProperties() {
        return Collections.unmodifiableMap(preservedProperties);
    }

    @Override
    public Schema mergeSchema(List<Schema> inputSchemas, Schema schema, DAG.Options options) {
        return SchemaUtils.mergeSchema(inputSchemas, schema);
    }

    @Override
    protected Schema loadSchema(List<String> includes) {
        return null;
    }

    /**
     * The real behaviour of an unknown node is not known, so its schema is passed through and never persisted.
     */
    @Override
    protected Schema saveSchema(Collection<String> predecessors, String nodeId, Schema schema, DAG.Options options) {
        return schema;
    }

    @Override
    protected Schema cloneSchema(Schema schema) {
        return SchemaUtils.cloneSchema(schema);
    }
}
