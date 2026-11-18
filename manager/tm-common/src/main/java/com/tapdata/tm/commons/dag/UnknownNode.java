package com.tapdata.tm.commons.dag;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.google.gson.annotations.SerializedName;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.SchemaUtils;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

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
     * {@link com.tapdata.tm.base.convert.NodeSerialize} flattens it back for persistence. It is deliberately
     * not a plausible business property name, so a node property called {@code preservedProperties} is kept as is.
     */
    public static final String GSON_PRESERVED_PROPERTIES_KEY = "__unknownNodePreservedProperties";

    /*
     * Deliberately neither @JsonIgnore (Jackson would propagate it to the same-named any-getter and drop
     * the data) nor transient (Java serialization deep copies would lose it). A private field without a
     * matching accessor is not auto-detected by Jackson, so it is only exposed through the any-getter.
     */
    @SerializedName(GSON_PRESERVED_PROPERTIES_KEY)
    private final Map<String, Object> preservedProperties = new LinkedHashMap<>();

    /** Set once the Gson nested form has been merged, so that it keeps priority over top-level duplicates. */
    private transient boolean gsonPreservedMerged;

    public UnknownNode() {
        super(null, NodeCatalog.processor);
    }

    /**
     * Builds a placeholder straight from the raw node properties, without binding them to {@link Node} fields.
     * Used when binding fails, e.g. a newer build stored a value (such as an enum constant) this version cannot read.
     */
    public static UnknownNode fromRawProperties(Map<String, ?> properties) {
        UnknownNode node = new UnknownNode();
        properties.forEach((name, value) -> {
            if ("id".equals(name) && value instanceof String) {
                node.setId((String) value);
            } else if ("type".equals(name) && value instanceof String) {
                node.setType((String) value);
            } else if ("name".equals(name) && value instanceof String) {
                node.setName((String) value);
            } else {
                node.preservedProperties.put(name, value);
            }
        });
        return node;
    }

    /** Sorted types of the {@link UnknownNode}s among {@code nodes}; empty if there are none. */
    public static Set<String> typesOf(Collection<? extends Node> nodes) {
        Set<String> types = new TreeSet<>();
        if (nodes != null) {
            for (Node<?> node : nodes) {
                if (node instanceof UnknownNode) {
                    types.add(node.getType());
                }
            }
        }
        return types;
    }

    @JsonAnySetter
    public void setUnknownProperty(String name, Object value) {
        if (GSON_PRESERVED_PROPERTIES_KEY.equals(name) && value instanceof Map) {
            // Nested form produced by a Gson round trip. It holds the original values, so it wins over
            // top-level keys Gson wrote from fields with the current runtime value (e.g. isTransformed).
            ((Map<?, ?>) value).forEach((k, v) -> preservedProperties.put(String.valueOf(k), v));
            gsonPreservedMerged = true;
            return;
        }
        if (gsonPreservedMerged) {
            preservedProperties.putIfAbsent(name, value);
        } else {
            preservedProperties.put(name, value);
        }
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
