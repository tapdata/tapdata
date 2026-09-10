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
     */
    public static Object asConcurrentBatchOffset(Object batchOffsetObj) {
        if (batchOffsetObj instanceof Map && !(batchOffsetObj instanceof ConcurrentHashMap)) {
            return new ConcurrentHashMap<>((Map<Object, Object>) batchOffsetObj);
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
        if (batchOffsetObj instanceof Map) {
            Map<?, ?> source = (Map<?, ?>) batchOffsetObj;
            Map<Object, Object> target = new HashMap<>(source);
            if (target.containsKey(BATCH_READ_CONNECTOR_OFFSET) || target.containsKey(BATCH_READ_CONNECTOR_STATUS)) {
                target.put(BATCH_READ_CONNECTOR_OFFSET, encodeOffsetIfNeed(target.get(BATCH_READ_CONNECTOR_OFFSET), encoder));
                return target;
            }
            source.forEach((key, value) -> target.put(key, encodeConnectorOffset(value, encoder)));
            return target;
        }
        return batchOffsetObj;
    }

    public static Object decodeConnectorOffset(Object batchOffsetObj, Function<String, Object> decoder) {
        if (batchOffsetObj instanceof Map) {
            Map<?, ?> source = (Map<?, ?>) batchOffsetObj;
            Map<Object, Object> target = new HashMap<>(source);
            if (target.containsKey(BATCH_READ_CONNECTOR_OFFSET) || target.containsKey(BATCH_READ_CONNECTOR_STATUS)) {
                target.put(BATCH_READ_CONNECTOR_OFFSET, decodeOffsetIfNeed(target.get(BATCH_READ_CONNECTOR_OFFSET), decoder));
                return target;
            }
            source.forEach((key, value) -> target.put(key, decodeConnectorOffset(value, decoder)));
            return target;
        }
        return batchOffsetObj;
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
