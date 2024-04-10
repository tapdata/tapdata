package com.tapdata.entity.dataflow.util;

import com.tapdata.entity.dataflow.SyncProgress;

import java.util.HashMap;
import java.util.Map;

public class SyncProcessHandler {
    private final SyncProgress syncProgress;
    public static final String TASK_BATCH_TABLE_OFFSET_POINT = "task_batch_table_offset_point";
    public static final String TASK_BATCH_TABLE_OFFSET_STATUS = "task_batch_table_offset_status";
    public static final String TABLE_BATCH_STATUS_OVER = "over";
    public static final String TABLE_BATCH_STATUS_RUNNING = "running";
    private SyncProcessHandler(SyncProgress syncProgress) {
        this.syncProgress = syncProgress;
    }

    public boolean batchIsOverOfTable(String tableId) {
        Object batchOffsetObj = syncProgress.getBatchOffsetObj();
        if (null == batchOffsetObj) return false;
        Object batchOffset = ((Map<String, Object>) batchOffsetObj).get(tableId);
        if (batchOffset instanceof Map && ((Map<String, Object>)batchOffset).containsKey(TASK_BATCH_TABLE_OFFSET_STATUS)) {
            //86迭代新功能--全量表同步断点
            Map<String, Object> offsetInfo = (Map<String, Object>)batchOffset;
            return TABLE_BATCH_STATUS_OVER.equals(offsetInfo.get(TASK_BATCH_TABLE_OFFSET_STATUS));
        }

        //历史数据
        return false;
    }

    public Object getBatchOffsetOfTable(String tableId) {
        Object batchOffsetObj = syncProgress.getBatchOffsetObj();
        if (null == batchOffsetObj) {
            return null;
        }
        Object batchOffset = ((Map<String, Object>) batchOffsetObj).get(tableId);
        if (batchOffset instanceof Map && ((Map<String, Object>)batchOffset).containsKey(TASK_BATCH_TABLE_OFFSET_POINT)) {
            //86迭代新功能--全量表同步断点
            Map<String, Object> offsetInfo = (Map<String, Object>) batchOffset;
            return offsetInfo.get(TASK_BATCH_TABLE_OFFSET_POINT);
        }

        //历史数据
        return batchOffset;
    }

    public void updateBatchOffset(String tableId, Object offset, String isOverTag) {
        Map<String, Object> batchOffsetObjTemp = (Map<String, Object>) putIfAbsentBatchOffsetObj();
        Map<String, Object> tableOffsetObjTemp = (Map<String, Object>) batchOffsetObjTemp.computeIfAbsent(tableId, k -> new HashMap<String, Object>());
        tableOffsetObjTemp.put(TASK_BATCH_TABLE_OFFSET_POINT, offset);
        tableOffsetObjTemp.put(TASK_BATCH_TABLE_OFFSET_STATUS, isOverTag);
    }

    public Object putIfAbsentBatchOffsetObj() {
        Object batchOffsetObj = syncProgress.getBatchOffsetObj();
        Map<String, Object> batchOffsetObjTemp = (Map<String, Object>) batchOffsetObj;
        if (null == batchOffsetObjTemp) {
            batchOffsetObjTemp = new HashMap<>();
            syncProgress.setBatchOffsetObj(batchOffsetObjTemp);
        }

        return batchOffsetObj;
    }
}
