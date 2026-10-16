package com.tapdata.tm.fieldorigin.builder;

import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.task.dto.MergeTableProperties;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Builds field origins of every final target table of a task (TAP-13082).
 * <p>
 * A source field id is {@code connectionId_table_field} and stays unchanged through rename and merge nodes,
 * so target fields are first matched to source fields by id. When the id is regenerated on the way (js, custom,
 * aggregation ...), the field is traced by its original field name the same way data trace does
 * ({@code FieldOriginalNameMapping}). Fields created by a processor node are DERIVED/MANUAL, merge containers
 * are CONTAINER, the rest is UNKNOWN and never dropped silently.
 */
public class FieldOriginBuilder {
    public static final String MERGE_NODE_TYPE = "merge_table_processor";
    static final String FIELD_ADD_DEL_TYPE = "field_add_del_processor";
    static final Set<String> UNION_TYPES = Set.of("union_processor", "migrate_union_processor");
    public static final String ARRAY_SUFFIX = "[]";
    public static final String ARRAY_SUFFIX_DOT = ARRAY_SUFFIX + ".";
    private static final Pattern INDEX_BRACKET = Pattern.compile("\\[\\d+]");
    private static final Pattern INDEX_SEGMENT = Pattern.compile("\\.\\d+(?=\\.|$)");

    private final Map<String, OriginGraphNode> nodes;
    private final NodeSchemaProvider provider;
    private final Map<String, List<SourceField>> sourceFieldsById = new HashMap<>();
    private final Map<String, Map<String, List<SourceField>>> sourceFieldsByNodeAndName = new HashMap<>();
    private final Map<String, Set<String>> ancestorsCache = new HashMap<>();

    public FieldOriginBuilder(Collection<OriginGraphNode> graph, NodeSchemaProvider provider) {
        this.nodes = new LinkedHashMap<>();
        graph.stream().filter(Objects::nonNull).forEach(n -> nodes.put(n.getId(), n));
        this.provider = provider;
    }

    public FieldOriginBuildResult build() {
        FieldOriginBuildResult result = new FieldOriginBuildResult();
        indexSourceFields();
        for (OriginGraphNode target : targets()) {
            MergeContext merge = MergeContext.of(nearestUpstreamMerge(target.getId()));
            for (NodeSchema schema : CollectionUtils.emptyIfNull(provider.schemas(target.getId()))) {
                result.getStats().setTargetTables(result.getStats().getTargetTables() + 1);
                buildTable(target, schema, merge, result);
            }
        }
        result.setConfigHash(configHash(result.getRecords()));
        return result;
    }

    void buildTable(OriginGraphNode target, NodeSchema schema, MergeContext merge, FieldOriginBuildResult result) {
        List<Field> fields = CollectionUtils.emptyIfNull(schema.fields()).stream()
                .filter(f -> null != f && !f.isDeleted() && StringUtils.isNotBlank(f.getFieldName()))
                .toList();
        Set<String> schemaSourceTables = new HashSet<>();
        for (Field field : fields) {
            sourceFieldsById.getOrDefault(StringUtils.defaultString(field.getId()), List.of())
                    .forEach(s -> schemaSourceTables.add(s.nodeId + "\u0001" + s.table));
        }
        for (Field field : fields) {
            List<FieldOriginRecord> records = resolve(target, schema, field, merge, schemaSourceTables);
            collectStats(records, result.getStats());
            result.getRecords().addAll(records);
        }
    }

    List<FieldOriginRecord> resolve(OriginGraphNode target, NodeSchema schema, Field field, MergeContext merge, Set<String> schemaSourceTables) {
        String path = field.getFieldName();
        FieldOriginRecord base = new FieldOriginRecord();
        base.setTargetNodeId(target.getId());
        base.setTargetConnectionId(target.getConnectionId());
        base.setTargetTable(schema.tableName());
        base.setFieldPath(path);
        base.setNormalizedFieldPath(merge.normalize(path));
        MergeTableProperties property = merge.propertyOf(path);
        if (null != merge.node) {
            base.setMergeNodeId(merge.node.getId());
            if (null != property) {
                base.setMergePropertyId(property.getId());
                base.setTargetPath(StringUtils.defaultString(property.getTargetPath()));
            }
        }
        if (merge.isContainer(path, field)) {
            return List.of(typed(base, OriginType.CONTAINER, MatchMethod.NONE));
        }
        String scopeNodeId = null != property && nodes.containsKey(property.getId()) ? property.getId() : target.getId();
        Set<String> allowedSources = sourceAncestors(scopeNodeId);
        String leaf = merge.leafName(path, property);

        Map<String, FieldOriginRecord> records = new LinkedHashMap<>();
        for (SourceField source : sourceFieldsById.getOrDefault(StringUtils.defaultString(field.getId()), List.of())) {
            if (allowedSources.contains(source.nodeId)) {
                put(records, sourced(base, source, leaf, MatchMethod.ID));
            }
        }
        boolean union = passesUnion(target.getId());
        if (records.isEmpty() || union) {
            String originName = merge.leafName(StringUtils.defaultIfBlank(field.getOriginalFieldName(), path), property);
            // A union merges several tables into one, so the same-table preference only applies without union
            for (SourceField source : findByName(allowedSources, originName, union ? Set.of() : schemaSourceTables)) {
                put(records, sourced(base, source, leaf, MatchMethod.NAME));
            }
        }
        if (!records.isEmpty()) {
            return new ArrayList<>(records.values());
        }
        OriginGraphNode creator = creatorOf(field, target.getId());
        if (null != creator) {
            FieldOriginRecord derived = typed(base, FIELD_ADD_DEL_TYPE.equals(creator.getType()) ? OriginType.MANUAL : OriginType.DERIVED, MatchMethod.NONE);
            derived.setTransformNodeId(creator.getId());
            return List.of(derived);
        }
        return List.of(typed(base, OriginType.UNKNOWN, MatchMethod.NONE));
    }

    void put(Map<String, FieldOriginRecord> records, FieldOriginRecord record) {
        records.putIfAbsent(record.identity(), record);
    }

    FieldOriginRecord sourced(FieldOriginRecord base, SourceField source, String leaf, MatchMethod method) {
        FieldOriginRecord record = typed(base, StringUtils.equals(leaf, source.field) ? OriginType.DIRECT : OriginType.RENAMED, method);
        record.setSourceNodeId(source.nodeId);
        record.setSourceConnectionId(source.connectionId);
        record.setSourceTable(source.table);
        record.setSourceField(source.field);
        return record;
    }

    FieldOriginRecord typed(FieldOriginRecord base, OriginType type, MatchMethod method) {
        FieldOriginRecord record = base.copy();
        record.setOriginType(type);
        record.setMatchMethod(method);
        return record;
    }

    List<SourceField> findByName(Set<String> allowedSources, String name, Set<String> schemaSourceTables) {
        if (StringUtils.isBlank(name)) {
            return List.of();
        }
        List<SourceField> found = new ArrayList<>();
        for (String nodeId : allowedSources) {
            found.addAll(sourceFieldsByNodeAndName.getOrDefault(nodeId, Map.of()).getOrDefault(name, List.of()));
        }
        // Prefer the source tables already proven by id matches of the same target table (migrate tasks read many tables)
        List<SourceField> sameTable = found.stream()
                .filter(s -> schemaSourceTables.contains(s.nodeId + "\u0001" + s.table))
                .toList();
        return sameTable.isEmpty() ? found : sameTable;
    }

    OriginGraphNode creatorOf(Field field, String targetId) {
        String id = field.getId();
        if (StringUtils.isBlank(id)) {
            return null;
        }
        return ancestors(targetId).stream()
                .map(nodes::get)
                .filter(n -> null != n && !n.isDataNode() && id.startsWith(n.getId() + "_"))
                .findFirst()
                .orElse(null);
    }

    boolean passesUnion(String targetId) {
        return ancestors(targetId).stream().map(nodes::get).anyMatch(n -> null != n && UNION_TYPES.contains(n.getType()));
    }

    void collectStats(List<FieldOriginRecord> records, FieldOriginStats stats) {
        stats.setTotalFields(stats.getTotalFields() + 1);
        OriginType type = records.get(0).getOriginType();
        switch (type) {
            case UNKNOWN -> stats.setUnknownFields(stats.getUnknownFields() + 1);
            case CONTAINER -> stats.setContainerFields(stats.getContainerFields() + 1);
            case DERIVED -> stats.setDerivedFields(stats.getDerivedFields() + 1);
            case MANUAL -> stats.setManualFields(stats.getManualFields() + 1);
            default -> stats.setResolvedFields(stats.getResolvedFields() + 1);
        }
        if (records.size() > 1) {
            stats.setMultiSourceFields(stats.getMultiSourceFields() + 1);
        }
        if (records.stream().anyMatch(r -> r.getMatchMethod() == MatchMethod.NAME)) {
            stats.setNameMatchedFields(stats.getNameMatchedFields() + 1);
        }
    }

    void indexSourceFields() {
        for (OriginGraphNode node : nodes.values()) {
            if (!node.isDataNode() || CollectionUtils.isNotEmpty(node.getPredecessors())) {
                continue;
            }
            for (NodeSchema schema : CollectionUtils.emptyIfNull(provider.schemas(node.getId()))) {
                for (Field field : CollectionUtils.emptyIfNull(schema.fields())) {
                    if (null == field || field.isDeleted() || StringUtils.isBlank(field.getFieldName())) {
                        continue;
                    }
                    SourceField source = new SourceField(node.getId(), node.getConnectionId(), schema.tableName(), field.getFieldName());
                    if (StringUtils.isNotBlank(field.getId())) {
                        sourceFieldsById.computeIfAbsent(field.getId(), k -> new ArrayList<>()).add(source);
                    }
                    sourceFieldsByNodeAndName.computeIfAbsent(node.getId(), k -> new HashMap<>())
                            .computeIfAbsent(field.getFieldName(), k -> new ArrayList<>()).add(source);
                }
            }
        }
    }

    List<OriginGraphNode> targets() {
        Set<String> hasSuccessor = new HashSet<>();
        nodes.values().forEach(n -> hasSuccessor.addAll(n.getPredecessors()));
        return nodes.values().stream()
                .filter(n -> n.isDataNode() && CollectionUtils.isNotEmpty(n.getPredecessors()) && !hasSuccessor.contains(n.getId()))
                .toList();
    }

    OriginGraphNode nearestUpstreamMerge(String nodeId) {
        Deque<String> queue = new ArrayDeque<>(nodes.get(nodeId).getPredecessors());
        Set<String> visited = new HashSet<>();
        while (!queue.isEmpty()) {
            String id = queue.poll();
            OriginGraphNode node = nodes.get(id);
            if (null == node || !visited.add(id)) {
                continue;
            }
            if (node.isMergeNode()) {
                return node;
            }
            if (!node.isDataNode()) {
                queue.addAll(node.getPredecessors());
            }
        }
        return null;
    }

    Set<String> ancestors(String nodeId) {
        return ancestorsCache.computeIfAbsent(nodeId, id -> {
            Set<String> result = new LinkedHashSet<>();
            Deque<String> queue = new ArrayDeque<>();
            OriginGraphNode start = nodes.get(id);
            if (null != start) {
                queue.addAll(start.getPredecessors());
            }
            while (!queue.isEmpty()) {
                String current = queue.poll();
                OriginGraphNode node = nodes.get(current);
                if (null != node && result.add(current)) {
                    queue.addAll(node.getPredecessors());
                }
            }
            return result;
        });
    }

    /** Source data nodes reachable upstream from the node, including the node itself when it is a source. */
    Set<String> sourceAncestors(String nodeId) {
        Set<String> result = new LinkedHashSet<>();
        OriginGraphNode self = nodes.get(nodeId);
        if (null != self && self.isDataNode() && CollectionUtils.isEmpty(self.getPredecessors())) {
            result.add(nodeId);
        }
        for (String id : ancestors(nodeId)) {
            OriginGraphNode node = nodes.get(id);
            if (node.isDataNode() && CollectionUtils.isEmpty(node.getPredecessors())) {
                result.add(id);
            }
        }
        return result;
    }

    public static String configHash(List<FieldOriginRecord> records) {
        List<String> lines = records.stream().map(FieldOriginBuilder::hashLine).sorted().toList();
        return DigestUtils.sha256Hex(String.join("\n", lines));
    }

    static String hashLine(FieldOriginRecord r) {
        return String.join("\u0001", r.identity(), String.valueOf(r.getTargetConnectionId()),
                String.valueOf(r.getNormalizedFieldPath()), String.valueOf(r.getSourceConnectionId()),
                String.valueOf(r.getMatchMethod()), String.valueOf(r.getMergeNodeId()));
    }

    /**
     * Normalizes a changed field path from a downstream diff: {@code a[0].b} and {@code a.0.b} become {@code a[].b}.
     */
    public static String normalizeQueryPath(String path) {
        if (StringUtils.isBlank(path)) {
            return path;
        }
        String normalized = INDEX_BRACKET.matcher(path.trim()).replaceAll(ARRAY_SUFFIX);
        return INDEX_SEGMENT.matcher(normalized).replaceAll(ARRAY_SUFFIX);
    }

    record SourceField(String nodeId, String connectionId, String table, String field) {
    }

    static final class MergeContext {
        private static final MergeContext NONE = new MergeContext(null, List.of());
        final OriginGraphNode node;
        /** All merge properties with a target path, longest first */
        final List<MergeTableProperties> nested;
        final MergeTableProperties main;
        /** raw array target path -> normalized array target path, shortest first */
        final LinkedHashMap<String, String> arrays = new LinkedHashMap<>();

        private MergeContext(OriginGraphNode node, List<MergeTableProperties> flat) {
            this.node = node;
            this.nested = flat.stream()
                    .filter(p -> StringUtils.isNotBlank(p.getTargetPath()))
                    .sorted(Comparator.comparingInt((MergeTableProperties p) -> p.getTargetPath().length()).reversed())
                    .toList();
            this.main = flat.stream().filter(p -> StringUtils.isBlank(p.getTargetPath())).findFirst().orElse(null);
            flat.stream()
                    .filter(p -> StringUtils.isNotBlank(p.getTargetPath()) && p.getMergeType() == MergeTableProperties.MergeType.updateIntoArray)
                    .map(MergeTableProperties::getTargetPath)
                    .distinct()
                    .sorted(Comparator.comparingInt(String::length))
                    .forEach(raw -> arrays.put(raw, normalize(raw)));
        }

        static MergeContext of(OriginGraphNode mergeNode) {
            if (null == mergeNode || CollectionUtils.isEmpty(mergeNode.getMergeProperties())) {
                return NONE;
            }
            List<MergeTableProperties> flat = new ArrayList<>();
            flatten(mergeNode.getMergeProperties(), flat);
            return new MergeContext(mergeNode, flat);
        }

        static void flatten(List<MergeTableProperties> properties, List<MergeTableProperties> flat) {
            for (MergeTableProperties property : CollectionUtils.emptyIfNull(properties)) {
                if (null == property) {
                    continue;
                }
                flat.add(property);
                flatten(property.getChildren(), flat);
            }
        }

        MergeTableProperties propertyOf(String path) {
            if (null == node) {
                return null;
            }
            for (MergeTableProperties property : nested) {
                if (StringUtils.equals(path, property.getTargetPath()) || StringUtils.startsWith(path, property.getTargetPath() + ".")) {
                    return property;
                }
            }
            return main;
        }

        String leafName(String path, MergeTableProperties property) {
            if (null == property || StringUtils.isBlank(property.getTargetPath())) {
                return path;
            }
            String prefix = property.getTargetPath() + ".";
            return StringUtils.startsWith(path, prefix) ? path.substring(prefix.length()) : path;
        }

        boolean isContainer(String path, Field field) {
            if (null == node || !StringUtils.startsWith(field.getId(), node.getId() + "_")) {
                return false;
            }
            return nested.stream().anyMatch(p -> p.getTargetPath().equals(path));
        }

        String normalize(String path) {
            if (null == path) {
                return null;
            }
            String result = path;
            for (Map.Entry<String, String> array : arrays.entrySet()) {
                String normalizedArray = array.getValue();
                if (result.startsWith(normalizedArray + ".")) {
                    result = normalizedArray + ARRAY_SUFFIX + result.substring(normalizedArray.length());
                }
            }
            return result;
        }
    }
}
