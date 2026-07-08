package io.tapdata.flow.engine.V2.node.duckdb.backup;

import com.tapdata.constant.JSONUtil;
import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataEvent;
import org.apache.commons.collections4.CollectionUtils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class DuckDbBackupOffsetTracker {
    private final Map<String, Map<String, Object>> byTable = new ConcurrentHashMap<>();
    private final AtomicLong eventSerialNo = new AtomicLong(0);
    private volatile Map<String, Object> latest = new LinkedHashMap<>();

    public void recordApplied(String tableKey, List<TapdataEvent> events) {
        if (CollectionUtils.isEmpty(events)) {
            return;
        }
        TapdataEvent last = events.get(events.size() - 1);
        if (last == null) {
            return;
        }
        long serialNo = nextSerialNo(last);
        Map<String, Object> item = toOffsetDocument(last, serialNo);
        if (tableKey != null && !tableKey.isBlank()) {
            byTable.put(tableKey, item);
        }
        Map<String, Object> snapshot = new LinkedHashMap<>(item);
        snapshot.put("byTable", new LinkedHashMap<>(byTable));
        snapshot.put("offsetHash", sha256(toJsonQuietly(snapshot)));
        latest = snapshot;
    }

    public Map<String, Object> snapshot() {
        Map<String, Object> copy = new LinkedHashMap<>(latest);
        copy.put("byTable", new LinkedHashMap<>(byTable));
        copy.putIfAbsent("eventSerialNo", eventSerialNo.get());
        copy.putIfAbsent("offsetHash", sha256(toJsonQuietly(copy)));
        return copy;
    }

    @SuppressWarnings("unchecked")
    public void restore(Map<String, Object> snapshot) {
        if (snapshot == null || snapshot.isEmpty()) {
            return;
        }
        latest = new LinkedHashMap<>(snapshot);
        Object byTableObj = snapshot.get("byTable");
        byTable.clear();
        if (byTableObj instanceof Map<?, ?> tableMap) {
            tableMap.forEach((key, value) -> {
                if (key != null && value instanceof Map<?, ?> valueMap) {
                    Map<String, Object> item = new LinkedHashMap<>();
                    valueMap.forEach((k, v) -> {
                        if (k != null) {
                            item.put(String.valueOf(k), v);
                        }
                    });
                    byTable.put(String.valueOf(key), item);
                }
            });
        }
        Object serial = snapshot.get("eventSerialNo");
        if (serial instanceof Number number) {
            eventSerialNo.set(number.longValue());
        }
    }

    private long nextSerialNo(TapdataEvent event) {
        Long sourceSerialNo = event.getSourceSerialNo();
        if (sourceSerialNo != null && sourceSerialNo > eventSerialNo.get()) {
            eventSerialNo.set(sourceSerialNo);
            return sourceSerialNo;
        }
        return eventSerialNo.incrementAndGet();
    }

    private Map<String, Object> toOffsetDocument(TapdataEvent event, long serialNo) {
        Map<String, Object> doc = new LinkedHashMap<>();
        SyncStage syncStage = event.getSyncStage();
        doc.put("syncStage", syncStage == null ? null : syncStage.name());
        doc.put("eventId", event.getEventId());
        doc.put("eventSerialNo", serialNo);
        doc.put("sourceSerialNo", event.getSourceSerialNo());
        doc.put("sourceTime", event.getSourceTime());
        doc.put("nodeIds", event.getNodeIds() == null ? null : new ArrayList<>(event.getNodeIds()));
        doc.put("offsetJson", toJsonQuietly(event.getOffset()));
        doc.put("batchOffsetJson", toJsonQuietly(event.getBatchOffset()));
        doc.put("streamOffsetJson", toJsonQuietly(event.getStreamOffset()));
        return doc;
    }

    private static String toJsonQuietly(Object value) {
        if (value == null) {
            return null;
        }
        try {
            return JSONUtil.obj2Json(value);
        } catch (Exception e) {
            return String.valueOf(value);
        }
    }

    public static String sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest((value == null ? "" : value).getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(bytes.length * 2);
            for (byte b : bytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            return Integer.toHexString(String.valueOf(value).hashCode());
        }
    }
}
