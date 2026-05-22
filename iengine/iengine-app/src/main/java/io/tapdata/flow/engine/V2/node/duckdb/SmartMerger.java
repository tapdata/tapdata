package io.tapdata.flow.engine.V2.node.duckdb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Simple smart merger implementing a last-wins deduplication strategy.
 * - If a record contains one of common PK fields (id, _id, pk, ID), use its value as grouping key.
 * - Otherwise, serialize the whole record and use that as key (full-row dedupe).
 *
 * This is a conservative first-step implementation of merge_events_smart (M3).
 */
public class SmartMerger {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final List<String> PK_CANDIDATES = Arrays.asList("_id", "id", "pk", "PK", "ID");

    public static List<Map<String, Object>> mergeLastWins(List<Map<String, Object>> events) {
        if (events == null || events.isEmpty()) return Collections.emptyList();

        // preserve insertion order of last seen keys
        Map<String, Map<String, Object>> lastByKey = new LinkedHashMap<>();

        for (Map<String, Object> ev : events) {
            String key = computeKey(ev);
            lastByKey.put(key, ev);
        }

        return new ArrayList<>(lastByKey.values());
    }

    private static String computeKey(Map<String, Object> ev) {
        for (String pk : PK_CANDIDATES) {
            if (ev.containsKey(pk)) {
                Object v = ev.get(pk);
                if (v != null) return pk + ":" + v.toString();
            }
        }

        // fallback to full JSON serialization
        try {
            return "_full:" + MAPPER.writeValueAsString(ev);
        } catch (JsonProcessingException e) {
            // last resort: use identity hashcode
            return "_hash:" + System.identityHashCode(ev);
        }
    }
}
