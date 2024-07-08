package com.tapdata.entity.dataflow.batch;

import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.dataflow.TableBatchReadStatus;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapRenameTableEvent;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchOffsetUtil {
    @Deprecated
    protected static final String BATCH_READ_CONNECTOR_OFFSET = "batch_read_connector_offset";
    protected static final String BATCH_READ_CONNECTOR_STATUS = "batch_read_connector_status";
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
        } else if (offsetValue instanceof Map
                && ((Map<?, ?>)offsetValue).containsKey(BATCH_READ_CONNECTOR_OFFSET)) {
            return ((Map<String, Object>)offsetValue).get(BATCH_READ_CONNECTOR_OFFSET);
        }

        /** history data*/
        return offsetValue;
    }

    public static Object getTableOffsetInfo(SyncProgress syncProgress, String tableId) {
        Object batchOffsetObj = syncProgress.getBatchOffsetObj();
        if (batchOffsetObj instanceof Map) {
            return ((Map<?, ?>) batchOffsetObj).get(tableId);
        }
        return batchOffsetObj;
    }

    public static void updateBatchOffset(SyncProgress syncProgress, String tableId, Object offset, String isOverTag) {
        Object batchOffsetObj = syncProgress.getBatchOffsetObj();
        if (batchOffsetObj instanceof Map) {
            Map<String, Object> batchOffsetObjTemp = (Map<String, Object>) batchOffsetObj;
            Object batchOffsetObject = batchOffsetObjTemp.computeIfAbsent(tableId, k -> new HashMap<>());
            if (batchOffsetObject instanceof Map
                    && ((Map<?, ?>)batchOffsetObject).containsKey(BATCH_READ_CONNECTOR_STATUS)) {
                updateBatchOffset((Map<String, Object>)batchOffsetObject, offset, isOverTag);
            } else {
                /** update history offset of table which id is ${tableId} **/
                batchOffsetObjTemp.put(tableId, updateBatchOffset(new HashMap<>(), offset, isOverTag));
            }
        }
    }

    protected static Object updateBatchOffset(Map<String, Object> offsetMap, Object offset, String isOverTag) {
        if (null == offsetMap) {
            offsetMap = new HashMap<>();
        }
        offsetMap.put(BATCH_READ_CONNECTOR_STATUS, isOverTag);
        return offsetMap;
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
