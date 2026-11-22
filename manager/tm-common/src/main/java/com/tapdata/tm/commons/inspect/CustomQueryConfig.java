package com.tapdata.tm.commons.inspect;

import lombok.Data;
import java.io.Serializable;
import java.util.*;

/** Versioned result contract. Legacy sortFields never imply a unique result key. */
@Data
public class CustomQueryConfig implements Serializable {
    private int version = 2;
    private int maxRows = 1000;
    private int timeoutSeconds = 600;
    private int detailLimit = 100;
    private boolean ignoreCase = true;
    private List<Mapping> mappings = new ArrayList<>();

    @Data
    public static class Mapping implements Serializable {
        private String source;
        private String target;
        /** key, compare, or ignore; ignore can describe a column on either side independently. */
        private String role = "compare";
    }

    @Data
    public static class Column implements Serializable {
        private String name;
        private String type;
    }

    @Data
    public static class Snapshot implements Serializable {
        private String fingerprint;
        private String timezone;
        private List<Column> columns = new ArrayList<>();
        private List<Map<String, Object>> rows = new ArrayList<>();
        private boolean hasMore;
        private boolean declaredSchema;
        private long rowCount;
    }

    @Data
    public static class SideSummary implements Serializable {
        private String status = "pending";
        private long startedAt;
        private long endedAt;
        private long durationMillis;
        private long rows;
    }

    public void validate(Snapshot source, Snapshot target) {
        if (version != 2 || maxRows < 1 || maxRows > 10000 || timeoutSeconds < 1 || timeoutSeconds > 3600
                || (detailLimit < 1 || detailLimit > 10000)) {
            throw new IllegalArgumentException("Invalid custom query limits or version");
        }
        if (source == null || target == null || source.getColumns() == null || target.getColumns() == null) {
            throw new IllegalArgumentException("Test both queries and confirm field mappings before saving or running");
        }
        Set<String> sources = columnNames(source), targets = columnNames(target);
        Set<String> usedSource = new HashSet<>(), usedTarget = new HashSet<>();
        boolean compare = false;
        if (mappings == null) throw new IllegalArgumentException("Field mappings are required");
        for (Mapping m : mappings) {
            if (m == null || !Arrays.asList("key", "compare", "ignore").contains(m.role)) {
                throw new IllegalArgumentException("Invalid field role");
            }
            boolean hasSource = m.source != null && !m.source.isEmpty();
            boolean hasTarget = m.target != null && !m.target.isEmpty();
            if (((!hasSource || !hasTarget) && !"ignore".equals(m.role)) || (!hasSource && !hasTarget)) {
                throw new IllegalArgumentException("Map both sides or explicitly ignore a column");
            }
            if (hasSource && (!sources.contains(name(m.source)) || !usedSource.add(name(m.source)))) {
                throw new IllegalArgumentException("Missing or duplicate source mapping: " + m.source);
            }
            if (hasTarget && (!targets.contains(name(m.target)) || !usedTarget.add(name(m.target)))) {
                throw new IllegalArgumentException("Missing or duplicate target mapping: " + m.target);
            }
            compare |= "compare".equals(m.role);
        }
        validateCardinality(source.getRowCount(), target.getRowCount());
        if (!compare || !sources.equals(usedSource) || !targets.equals(usedTarget)) {
            throw new IllegalArgumentException("Map or explicitly ignore every output column and choose at least one comparison field");
        }
    }

    /** Without a result key the contract is at most one row on each side, including at runtime. */
    public void validateCardinality(long sourceRows, long targetRows) {
        if (mappings == null) throw new IllegalArgumentException("Field mappings are required");
        if ((sourceRows > 1 || targetRows > 1) && mappings.stream().noneMatch(m -> "key".equals(m.getRole())))
            throw new IllegalArgumentException("Query returned multiple rows without a result key (source=" + sourceRows
                    + ", target=" + targetRows + "); test both queries again and configure a unique result key");
    }

    public String name(String value) { return ignoreCase ? value.toLowerCase(Locale.ROOT) : value; }

    public Set<String> columnNames(Snapshot snapshot) {
        Set<String> names = new HashSet<>();
        for (Column c : snapshot.getColumns()) {
            if (c == null || c.name == null || c.name.isEmpty() || !names.add(name(c.name))) {
                throw new IllegalArgumentException("Output aliases must be nonempty and unique");
            }
        }
        if (names.isEmpty()) throw new IllegalArgumentException("Query returned no column metadata; test a query with discoverable output columns");
        return names;
    }
}
