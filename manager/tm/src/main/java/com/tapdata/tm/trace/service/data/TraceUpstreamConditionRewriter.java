package com.tapdata.tm.trace.service.data;

import com.tapdata.tm.trace.dto.boodline.FieldNameMapping;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Rewrites a wide-table filter onto an upstream table when the downstream node has no matched records.
 * Merge sub-tables use nested path + join keys; normal tables only keep fields that exist upstream.
 * After a merge table has records, siblings are filled from shared join keys / table PKs.
 */
public final class TraceUpstreamConditionRewriter {

    private TraceUpstreamConditionRewriter() {
    }

    public static List<Map<String, Object>> rewriteMergeSubTableFilters(List<Map<String, Object>> downstreamFilters,
                                                                        String nestedPath,
                                                                        List<FieldNameMapping> joinKeys,
                                                                        List<FieldNameMapping> tablePk,
                                                                        Map<String, String> upstreamFieldMapping) {
        if (CollectionUtils.isEmpty(downstreamFilters)) {
            return Collections.emptyList();
        }
        List<Map<String, Object>> result = new ArrayList<>();
        for (Map<String, Object> sourceFilter : downstreamFilters) {
            if (MapUtils.isEmpty(sourceFilter)) {
                continue;
            }
            Map<String, Object> rewritten = new LinkedHashMap<>();
            appendJoinKeyValues(rewritten, sourceFilter, joinKeys);
            appendJoinKeyValues(rewritten, sourceFilter, tablePk);
            sourceFilter.forEach((field, value) -> {
                if (StringUtils.isBlank(field) || value == null || rewritten.containsKey(field)) {
                    return;
                }
                String localField = resolveLocalField(field, nestedPath, upstreamFieldMapping);
                if (StringUtils.isNotBlank(localField) && !rewritten.containsKey(localField)) {
                    rewritten.put(localField, value);
                }
            });
            if (MapUtils.isNotEmpty(rewritten)) {
                result.add(rewritten);
            }
        }
        return result;
    }

    public static List<Map<String, Object>> rewriteNormalUpstreamFilters(List<Map<String, Object>> downstreamFilters,
                                                                         Map<String, String> currentFieldMapping,
                                                                         Map<String, String> upstreamFieldMapping) {
        if (CollectionUtils.isEmpty(downstreamFilters) || MapUtils.isEmpty(upstreamFieldMapping)) {
            return Collections.emptyList();
        }
        Map<String, String> upstreamByOrigin = invertOriginToField(upstreamFieldMapping);
        List<Map<String, Object>> result = new ArrayList<>();
        for (Map<String, Object> sourceFilter : downstreamFilters) {
            if (MapUtils.isEmpty(sourceFilter)) {
                continue;
            }
            Map<String, Object> rewritten = new LinkedHashMap<>();
            sourceFilter.forEach((currentField, value) -> {
                if (StringUtils.isBlank(currentField) || value == null) {
                    return;
                }
                String originName = MapUtils.emptyIfNull(currentFieldMapping).get(currentField);
                if (StringUtils.isBlank(originName)) {
                    originName = currentField;
                }
                String upstreamField = firstNotBlank(
                        upstreamByOrigin.get(originName),
                        upstreamFieldMapping.containsKey(originName) ? originName : null,
                        upstreamFieldMapping.containsKey(currentField) ? currentField : null
                );
                if (StringUtils.isNotBlank(upstreamField)) {
                    rewritten.put(upstreamField, value);
                }
            });
            if (MapUtils.isNotEmpty(rewritten)) {
                result.add(rewritten);
            }
        }
        return result;
    }

    public static List<Map<String, Object>> rewriteMainTableFiltersFromSubTableRecords(List<Map<String, Object>> subTableRecords,
                                                                                       List<FieldNameMapping> joinKeys,
                                                                                       Map<String, String> mainTableFieldMapping) {
        if (CollectionUtils.isEmpty(subTableRecords) || CollectionUtils.isEmpty(joinKeys)
                || MapUtils.isEmpty(mainTableFieldMapping)) {
            return Collections.emptyList();
        }
        List<Map<String, Object>> result = new ArrayList<>();
        Set<Map<String, Object>> seen = new HashSet<>();
        for (Map<String, Object> record : subTableRecords) {
            if (MapUtils.isEmpty(record)) {
                continue;
            }
            Map<String, Object> filter = new LinkedHashMap<>();
            for (FieldNameMapping joinKey : joinKeys) {
                if (joinKey == null) {
                    continue;
                }
                String subTableField = firstNotBlank(joinKey.getOriginName(), joinKey.getTargetName());
                String mainTableField = pickMainTableField(joinKey, mainTableFieldMapping);
                if (StringUtils.isAnyBlank(subTableField, mainTableField)) {
                    continue;
                }
                Object value = record.get(subTableField);
                if (value != null) {
                    filter.put(mainTableField, value);
                }
            }
            if (MapUtils.isNotEmpty(filter) && seen.add(filter)) {
                result.add(filter);
            }
        }
        return result;
    }

    /**
     * Rebuilds filters for a merge sibling from the current node's records.
     * Same-named join/PK fields are copied when they exist on the sibling.
     * Renamed join keys are mapped (e.g. order.customer_no -&gt; customer.cust_no).
     * Nested join targets stay on the merge-table origin (account.account_id can
     * fill transaction; customer_id cannot).
     */
    public static List<Map<String, Object>> rewriteSiblingFiltersFromRecords(List<Map<String, Object>> currentRecords,
                                                                             List<FieldNameMapping> currentJoinKeys,
                                                                             List<FieldNameMapping> currentTablePk,
                                                                             List<FieldNameMapping> siblingJoinKeys,
                                                                             List<FieldNameMapping> siblingTablePk,
                                                                             Map<String, String> siblingFieldMapping) {
        if (CollectionUtils.isEmpty(currentRecords)) {
            return Collections.emptyList();
        }
        Set<String> candidateFields = new LinkedHashSet<>();
        collectLocalFieldNames(candidateFields, currentJoinKeys);
        collectLocalFieldNames(candidateFields, currentTablePk);
        collectLocalFieldNames(candidateFields, siblingJoinKeys);
        collectLocalFieldNames(candidateFields, siblingTablePk);
        Set<String> siblingKeyNames = new LinkedHashSet<>();
        collectJoinOriginNames(siblingKeyNames, siblingJoinKeys);
        collectLocalFieldNames(siblingKeyNames, siblingTablePk);
        if (candidateFields.isEmpty()) {
            return Collections.emptyList();
        }
        List<Map<String, Object>> result = new ArrayList<>();
        Set<Map<String, Object>> seen = new HashSet<>();
        for (Map<String, Object> record : currentRecords) {
            if (MapUtils.isEmpty(record)) {
                continue;
            }
            Map<String, Object> filter = new LinkedHashMap<>();
            putJoinKeyMappedValues(filter, record, currentJoinKeys, siblingFieldMapping, siblingKeyNames);
            putJoinKeyMappedValues(filter, record, siblingJoinKeys, siblingFieldMapping, siblingKeyNames);
            for (String field : candidateFields) {
                Object value = record.get(field);
                if (value == null || filter.containsKey(field)
                        || !siblingAcceptsField(field, siblingFieldMapping, siblingKeyNames)) {
                    continue;
                }
                filter.put(field, value);
            }
            if (MapUtils.isNotEmpty(filter) && seen.add(filter)) {
                result.add(filter);
            }
        }
        return result;
    }

    public static boolean onlyNestedFilterKeys(List<Map<String, Object>> filters) {
        if (CollectionUtils.isEmpty(filters)) {
            return false;
        }
        boolean hasKey = false;
        for (Map<String, Object> filter : filters) {
            if (MapUtils.isEmpty(filter)) {
                continue;
            }
            for (String key : filter.keySet()) {
                if (StringUtils.isBlank(key)) {
                    continue;
                }
                hasKey = true;
                if (!key.contains(".")) {
                    return false;
                }
            }
        }
        return hasKey;
    }

    static String resolveLocalField(String field, String nestedPath, Map<String, String> upstreamFieldMapping) {
        if (StringUtils.isBlank(field)) {
            return null;
        }
        if (upstreamHasField(field, upstreamFieldMapping)) {
            return field;
        }
        String stripped = stripNestedPath(field, nestedPath);
        if (StringUtils.isBlank(stripped)) {
            return null;
        }
        if (MapUtils.isEmpty(upstreamFieldMapping) || upstreamHasField(stripped, upstreamFieldMapping)) {
            return stripped;
        }
        return null;
    }

    static String stripNestedPath(String field, String nestedPath) {
        if (StringUtils.isAnyBlank(field, nestedPath)) {
            return null;
        }
        if (StringUtils.equals(field, nestedPath)) {
            return null;
        }
        String prefix = nestedPath + ".";
        if (field.startsWith(prefix)) {
            return field.substring(prefix.length());
        }
        return null;
    }

    private static void appendJoinKeyValues(Map<String, Object> rewritten,
                                            Map<String, Object> sourceFilter,
                                            List<FieldNameMapping> mappings) {
        if (CollectionUtils.isEmpty(mappings)) {
            return;
        }
        for (FieldNameMapping mapping : mappings) {
            if (mapping == null) {
                continue;
            }
            String sourceField = firstNotBlank(mapping.getOriginName());
            String targetField = firstNotBlank(mapping.getTargetName());
            if (StringUtils.isBlank(sourceField)) {
                continue;
            }
            Object value = null;
            if (StringUtils.isNotBlank(targetField)) {
                value = sourceFilter.get(targetField);
            }
            if (value == null) {
                value = sourceFilter.get(sourceField);
            }
            if (value != null && !rewritten.containsKey(sourceField)) {
                rewritten.put(sourceField, value);
            }
        }
    }

    private static void putJoinKeyMappedValues(Map<String, Object> filter,
                                               Map<String, Object> record,
                                               List<FieldNameMapping> joinKeys,
                                               Map<String, String> siblingFieldMapping,
                                               Set<String> siblingKeyNames) {
        if (filter == null || MapUtils.isEmpty(record) || CollectionUtils.isEmpty(joinKeys)) {
            return;
        }
        for (FieldNameMapping mapping : joinKeys) {
            if (mapping == null) {
                continue;
            }
            String originName = mapping.getOriginName();
            String localTarget = localJoinField(mapping.getTargetName());
            putIfSiblingAccepts(filter, record, originName, localTarget, siblingFieldMapping, siblingKeyNames);
            putIfSiblingAccepts(filter, record, originName, originName, siblingFieldMapping, siblingKeyNames);
            putIfSiblingAccepts(filter, record, localTarget, originName, siblingFieldMapping, siblingKeyNames);
            putIfSiblingAccepts(filter, record, localTarget, localTarget, siblingFieldMapping, siblingKeyNames);
        }
    }

    private static void putIfSiblingAccepts(Map<String, Object> filter,
                                            Map<String, Object> record,
                                            String sourceField,
                                            String destField,
                                            Map<String, String> siblingFieldMapping,
                                            Set<String> siblingKeyNames) {
        if (StringUtils.isAnyBlank(sourceField, destField) || filter.containsKey(destField)) {
            return;
        }
        Object value = record.get(sourceField);
        if (value != null && siblingAcceptsField(destField, siblingFieldMapping, siblingKeyNames)) {
            filter.put(destField, value);
        }
    }

    private static String localJoinField(String field) {
        if (StringUtils.isBlank(field) || field.contains(".")) {
            return null;
        }
        return field;
    }

    /**
     * Join origin is the merge-table (sibling) field. The target is the parent/main
     * field and must not be treated as a sibling column when names differ
     * (e.g. order.customer_no -&gt; customer.cust_no).
     */
    private static void collectJoinOriginNames(Set<String> fields, List<FieldNameMapping> mappings) {
        if (fields == null || CollectionUtils.isEmpty(mappings)) {
            return;
        }
        for (FieldNameMapping mapping : mappings) {
            if (mapping == null || StringUtils.isBlank(mapping.getOriginName())) {
                continue;
            }
            fields.add(mapping.getOriginName());
        }
    }

    private static void collectLocalFieldNames(Set<String> fields, List<FieldNameMapping> mappings) {
        if (fields == null || CollectionUtils.isEmpty(mappings)) {
            return;
        }
        for (FieldNameMapping mapping : mappings) {
            if (mapping == null) {
                continue;
            }
            if (StringUtils.isNotBlank(mapping.getOriginName())) {
                fields.add(mapping.getOriginName());
            }
            String targetName = mapping.getTargetName();
            if (StringUtils.isNotBlank(targetName) && !targetName.contains(".")) {
                fields.add(targetName);
            }
        }
    }

    private static boolean siblingAcceptsField(String field, Map<String, String> siblingFieldMapping,
                                               Set<String> siblingKeyNames) {
        return upstreamHasField(field, siblingFieldMapping)
                || (siblingKeyNames != null && siblingKeyNames.contains(field));
    }

    private static String pickMainTableField(FieldNameMapping joinKey, Map<String, String> mainTableFieldMapping) {
        String originName = joinKey.getOriginName();
        String targetName = joinKey.getTargetName();
        if (upstreamHasField(originName, mainTableFieldMapping)) {
            return originName;
        }
        if (upstreamHasField(targetName, mainTableFieldMapping)) {
            return targetName;
        }
        return null;
    }

    private static boolean upstreamHasField(String field, Map<String, String> upstreamFieldMapping) {
        if (StringUtils.isBlank(field)) {
            return false;
        }
        if (MapUtils.isEmpty(upstreamFieldMapping)) {
            return false;
        }
        return upstreamFieldMapping.containsKey(field) || upstreamFieldMapping.containsValue(field);
    }

    private static Map<String, String> invertOriginToField(Map<String, String> fieldToOrigin) {
        Map<String, String> originToField = new LinkedHashMap<>();
        fieldToOrigin.forEach((fieldName, originName) -> {
            if (StringUtils.isNoneBlank(fieldName, originName)) {
                originToField.putIfAbsent(originName, fieldName);
            }
        });
        return originToField;
    }

    private static String firstNotBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (StringUtils.isNotBlank(value)) {
                return value;
            }
        }
        return null;
    }
}
