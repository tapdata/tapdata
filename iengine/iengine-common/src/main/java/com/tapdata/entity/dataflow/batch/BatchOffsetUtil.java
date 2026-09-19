package com.tapdata.entity.dataflow.batch;

import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.dataflow.TableBatchReadStatus;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapRenameTableEvent;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class BatchOffsetUtil {
    protected static final String BATCH_READ_CONNECTOR_OFFSET = "batch_read_connector_offset";
    public static final String BATCH_READ_CONNECTOR_STATUS = "batch_read_connector_status";
    private BatchOffsetUtil(){

    }

    public static boolean batchIsOverOfTable(SyncProgress syncProgress, String tableId) {
        Object offsetValue = getTableOffsetInfo(syncProgress, tableId);
        if (offsetValue instanceof BatchOffset) {
            /** 86 Iteration New Function - Full Scale Synchronization Breakpoint **/
            return TableBatchReadStatus.OVER.name().equals(((BatchOffset)offsetValue).getStatus());
        } else if (offsetValue instanceof Map
                && ((Map<?, ?>)offsetValue).containsKey(BATCH_READ_CONNECTOR_STATUS)) {
            return TableBatchReadStatus.OVER.name().equals(((Map<String, Object>)offsetValue).get(BATCH_READ_CONNECTOR_STATUS));
        }
        /** history data*/
        return false;
    }

    public static Object getBatchOffsetOfTable(SyncProgress syncProgress, String tableId) {
        Object offsetValue = getTableOffsetInfo(syncProgress, tableId);
        if (offsetValue instanceof BatchOffset) {
            /** 86 Iteration New Function - Full Scale Synchronization Breakpoint **/
            return ((BatchOffset) offsetValue).getOffset();
        } else if (offsetValue instanceof Map) {
            Map<?, ?> offsetMap = (Map<?, ?>) offsetValue;
            if (offsetMap.containsKey(BATCH_READ_CONNECTOR_OFFSET)) {
                return offsetMap.get(BATCH_READ_CONNECTOR_OFFSET);
            }
            if (offsetMap.containsKey(BATCH_READ_CONNECTOR_STATUS)) {
                /**
                 * Legacy breakpoint produced by an older engine only carried the batch read status
                 * without a resumable offset. Returning the marker map itself (the previous behavior)
                 * would hand it to the connector as the resume offset. There is nothing to resume, so
                 * fall back to a full run from scratch.
                 */
                return null;
            }
        }

        /** history data*/
        return offsetValue;
    }

    public static Object getTableOffsetInfo(SyncProgress syncProgress, String tableId) {
        Object batchOffsetObj = syncProgress.getBatchOffsetObj();
        if (batchOffsetObj instanceof Map) {
            Object tableBatchOffsetObj = ((Map<?, ?>) batchOffsetObj).get(tableId);
            if (tableBatchOffsetObj instanceof Map) {
                return new HashMap<>((Map<?, ?>) tableBatchOffsetObj);
            }
            return tableBatchOffsetObj;
        }
        return batchOffsetObj;
    }

    /**
     * The top level batch offset container (tableId -&gt; table offset) is mutated concurrently by the
     * partition read worker threads, so it must always be a thread-safe map. A restored breakpoint
     * must not downgrade it to a plain HashMap (decoding rebuilds maps and would otherwise lose the
     * ConcurrentHashMap created on the first run).
     *
     * <p>{@link ConcurrentHashMap} rejects null keys/values, but a table offset marker written by
     * {@link #updateBatchOffset(Map, Object, String)} may carry a null connector offset, and a restored
     * breakpoint can even be a bare marker map ({@code {status, offset: null}}). Copy entry by entry and
     * skip nulls so restoring such a breakpoint does not throw a NullPointerException and leave the task
     * unable to start. A null entry is semantically equivalent to a missing one:
     * {@link #getBatchOffsetOfTable} already returns null in that case.
     */
    public static Object asConcurrentBatchOffset(Object batchOffsetObj) {
        if (batchOffsetObj instanceof Map && !(batchOffsetObj instanceof ConcurrentHashMap)) {
            Map<Object, Object> concurrent = new ConcurrentHashMap<>();
            ((Map<?, ?>) batchOffsetObj).forEach((key, value) -> {
                if (null != key && null != value) {
                    concurrent.put(key, value);
                }
            });
            return concurrent;
        }
        return batchOffsetObj;
    }

    public static Map<String, Boolean> getAllTableBatchOffsetInfo(SyncProgress syncProgress) {
        Object batchOffsetObj = syncProgress.getBatchOffsetObj();
        Map<String, Boolean> tableOffset = new HashMap<>();
        if (batchOffsetObj instanceof Map) {
            Set<String> tables = ((Map<String, ?>) batchOffsetObj).keySet();
            tables.forEach(key -> tableOffset.put(key, batchIsOverOfTable(syncProgress, key)));
        }
        return tableOffset;
    }

    public static void updateBatchOffset(SyncProgress syncProgress, String tableId, Object offset, String isOverTag) {
        Object batchOffsetObj = syncProgress.getBatchOffsetObj();
        if (batchOffsetObj instanceof Map) {
            Map<String, Object> batchOffsetObjTemp = (Map<String, Object>) batchOffsetObj;
            Object batchOffsetObject = batchOffsetObjTemp.computeIfAbsent(tableId, k -> new HashMap<>());
            updateBatchOffset((Map<String, Object>)batchOffsetObject, offset, isOverTag);
        }
    }

    protected static Object updateBatchOffset(Map<String, Object> offsetMap, Object offset, String isOverTag) {
        if (null == offsetMap) {
            offsetMap = new HashMap<>();
        }
        offsetMap.put(BATCH_READ_CONNECTOR_STATUS, isOverTag);
        offsetMap.put(BATCH_READ_CONNECTOR_OFFSET, offset);
        return offsetMap;
    }

    public static Object encodeConnectorOffset(Object batchOffsetObj, Function<Object, String> encoder) {
        return transformConnectorOffset(batchOffsetObj, offset -> encodeOffsetIfNeed(offset, encoder));
    }

    public static Object decodeConnectorOffset(Object batchOffsetObj, Function<String, Object> decoder) {
        return transformConnectorOffset(batchOffsetObj, offset -> decodeOffsetIfNeed(offset, decoder));
    }

    /**
     * Rebuild the batch offset applying {@code offsetTransform} to the connector offset that is
     * carried by a table offset marker ({@link #BATCH_READ_CONNECTOR_OFFSET}). The top level map is
     * {@code tableId -&gt; tableOffset}, so we descend exactly one level and never walk into the
     * connector offset payload. Rebuilding the payload would replace nested {@code LinkedHashMap}s
     * (losing iteration order) and connector defined {@code Serializable} {@code Map} subclasses with
     * plain {@code HashMap}s, which the connector would fail to cast back when resuming.
     */
    private static Object transformConnectorOffset(Object batchOffsetObj, Function<Object, Object> offsetTransform) {
        if (!(batchOffsetObj instanceof Map)) {
            return batchOffsetObj;
        }
        Map<?, ?> source = (Map<?, ?>) batchOffsetObj;
        if (isTableOffsetMarker(source)) {
            // the whole batch offset already is a single table offset
            return transformTableOffset(source, offsetTransform);
        }
        Map<Object, Object> target = new HashMap<>(source);
        source.forEach((key, value) -> {
            if (value instanceof Map && isTableOffsetMarker((Map<?, ?>) value)) {
                target.put(key, transformTableOffset((Map<?, ?>) value, offsetTransform));
            }
        });
        return target;
    }

    private static boolean isTableOffsetMarker(Map<?, ?> offsetMap) {
        return offsetMap.containsKey(BATCH_READ_CONNECTOR_OFFSET) || offsetMap.containsKey(BATCH_READ_CONNECTOR_STATUS);
    }

    private static Object transformTableOffset(Map<?, ?> tableOffset, Function<Object, Object> offsetTransform) {
        Map<Object, Object> target = new HashMap<>(tableOffset);
        target.put(BATCH_READ_CONNECTOR_OFFSET, offsetTransform.apply(target.get(BATCH_READ_CONNECTOR_OFFSET)));
        return target;
    }

    private static Object encodeOffsetIfNeed(Object offset, Function<Object, String> encoder) {
        if (offset == null) {
            return null;
        }
        if (offset instanceof String && ((String) offset).startsWith(PdkUtil.ENCODE_PREFIX)) {
            return offset;
        }
        return encoder.apply(offset);
    }

    private static Object decodeOffsetIfNeed(Object offset, Function<String, Object> decoder) {
        if (offset instanceof String && ((String) offset).startsWith(PdkUtil.ENCODE_PREFIX)) {
            return decoder.apply((String) offset);
        }
        return offset;
    }

    protected static void tableUpdateName(SyncProgress syncProgress, String oldName, String newName) {
        Object batchTableOffsetObj = syncProgress.getBatchOffsetObj();
        if (batchTableOffsetObj instanceof Map) {
            Map<String, Object> batchOffset = (Map<String, Object>) batchTableOffsetObj;
            if (batchOffset.containsKey(oldName)) {
                Object offsetValue = batchOffset.get(oldName);
                batchOffset.remove(oldName);
                batchOffset.put(newName, offsetValue);
            }
        }
    }

    public static void updateBatchOffsetWhenTableRename(SyncProgress syncProgress, TapEvent tapEvent) {
        if (tapEvent instanceof TapRenameTableEvent) {
            TapRenameTableEvent tapRenameTableEvent = (TapRenameTableEvent) tapEvent;
            List<ValueChange<String>> nameChanges = tapRenameTableEvent.getNameChanges();
            for (ValueChange<String> nameChange : nameChanges) {
                tableUpdateName(syncProgress, nameChange.getBefore(), nameChange.getAfter());
            }
        }
    }

}
