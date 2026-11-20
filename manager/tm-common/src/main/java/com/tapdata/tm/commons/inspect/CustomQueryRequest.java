package com.tapdata.tm.commons.inspect;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

@Data
public class CustomQueryRequest implements Serializable {
    private String connectionId;
    private String query;
    private String kind = "sql";
    private String collection;
    private String timezone;
    private int timeoutSeconds = 600;
    private boolean ignoreCase = true;

    public static java.time.ZoneId zoneId(String timezone) {
        return java.time.ZoneId.of(timezone == null || timezone.trim().isEmpty() ? "UTC" : timezone);
    }

    public void validate() {
        if (connectionId == null || connectionId.trim().isEmpty() || query == null || query.trim().isEmpty()
                || timeoutSeconds < 1 || timeoutSeconds > 3600) throw new IllegalArgumentException("Connection, query and valid timeout are required");
        zoneId(timezone);
        if (kind == null || "sql".equals(kind)) CustomQueryReadOnly.validateReadOnlyQuery(query, "custom query");
        else if ("mongoPipeline".equals(kind)) {
            if (collection == null || collection.trim().isEmpty()) throw new IllegalArgumentException("Mongo collection is required");
            pipeline();
        } else throw new IllegalArgumentException("Unsupported query protocol");
    }

    public List<Map<String, Object>> pipeline() {
        try {
            Object value = new ObjectMapper().readValue(query, Object.class);
            if (!(value instanceof List)) throw new IllegalArgumentException("Pipeline must be a JSON array");
            List<Map<String, Object>> stages = (List<Map<String, Object>>) value;
            validateStages(stages);
            return stages;
        } catch (Exception e) { throw new IllegalArgumentException("Invalid read-only Mongo pipeline: " + e.getMessage(), e); }
    }

    private static void validateStages(List<?> stages) {
        // An allowlist of data-reading stages also rejects new write-capable stages by default.
        Set<String> allowed = new HashSet<>(Arrays.asList("$match", "$project", "$group", "$sort", "$limit", "$skip", "$unwind", "$addFields", "$set", "$unset", "$replaceRoot", "$replaceWith", "$count", "$sortByCount", "$bucket", "$bucketAuto", "$sample", "$lookup", "$facet", "$unionWith", "$setWindowFields", "$densify", "$fill"));
        for (Object stage : stages) {
            if (!(stage instanceof Map) || ((Map<?, ?>) stage).size() != 1 || !allowed.contains(((Map<?, ?>) stage).keySet().iterator().next()))
                throw new IllegalArgumentException("Unsupported or non-read-only pipeline stage");
            validateNested(stage);
        }
    }
    private static void validateNested(Object value) {
        if (value instanceof Map) for (Map.Entry<?, ?> e : ((Map<?, ?>) value).entrySet()) {
            if (Arrays.asList("$out", "$merge", "$where", "$function", "$accumulator").contains(e.getKey())) throw new IllegalArgumentException("Write stages and server-side scripts are prohibited");
            if ("pipeline".equals(e.getKey()) && e.getValue() instanceof List) validateStages((List<?>) e.getValue());
            else if ("$facet".equals(e.getKey()) && e.getValue() instanceof Map)
                for (Object nested : ((Map<?, ?>) e.getValue()).values()) validateStages((List<?>) nested);
            else validateNested(e.getValue());
        } else if (value instanceof List) for (Object item : (List<?>) value) validateNested(item);
    }

    /** Length-prefix inputs so SQL, collection, identity and timezone cannot collide. */
    public String fingerprint() {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (String v : Arrays.asList(connectionId, kind == null ? "sql" : kind, query, collection, timezone)) {
                String text = v == null ? "" : v;
                digest.update((text.length() + ":" + text).getBytes(StandardCharsets.UTF_8));
            }
            StringBuilder result = new StringBuilder();
            for (byte b : digest.digest()) result.append(String.format("%02x", b & 255));
            return result.toString();
        } catch (Exception e) { throw new IllegalStateException(e); }
    }
}
