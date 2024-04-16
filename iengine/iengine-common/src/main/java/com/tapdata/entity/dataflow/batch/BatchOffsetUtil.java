package com.tapdata.entity.dataflow.batch;

import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.dataflow.TableBatchReadStatus;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapRenameTableEvent;

import java.util.List;
import java.util.Map;

public class BatchOffsetUtil {
    private BatchOffsetUtil(){

    }

    public static boolean batchIsOverOfTable(SyncProgress syncProgress, String tableId) {
        Object offsetValue = getTableOffsetInfo(syncProgress, tableId);
        if (offsetValue instanceof BatchOffset) {
            //86迭代新功能--全量表同步断点
            return TableBatchReadStatus.OVER.name().equals(((BatchOffset)offsetValue).getStatus());
        }
        //历史数据
        return false;
    }

    public static Object getBatchOffsetOfTable(SyncProgress syncProgress, String tableId) {
        Object offsetValue = getTableOffsetInfo(syncProgress, tableId);
        if (offsetValue instanceof BatchOffset) {
            //86迭代新功能--全量表同步断点
            return ((BatchOffset) offsetValue).getOffset();
        }

        //历史数据
        return offsetValue;
    }

    protected static Object getTableOffsetInfo(SyncProgress syncProgress, String tableId) {
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
            Object batchOffsetObject = batchOffsetObjTemp.computeIfAbsent(tableId, k -> new BatchOffset(offset, isOverTag));
            if (batchOffsetObject instanceof BatchOffset) {
                BatchOffset batchOffset = (BatchOffset) batchOffsetObject;
                batchOffset.setOffset(offset);
                batchOffset.setStatus(isOverTag);
            } else {
                /** update history offset of table which id is ${tableId} **/
                batchOffsetObjTemp.put(tableId, new BatchOffset(offset, isOverTag));
            }
        }
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
