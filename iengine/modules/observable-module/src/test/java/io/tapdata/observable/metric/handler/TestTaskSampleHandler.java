package io.tapdata.observable.metric.handler;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

public class TestTaskSampleHandler {

    @Test
    public void testStaticFields() {
        testStaticFields("SAMPLE_TYPE_TASK", "task");
        testStaticFields("TABLE_TOTAL", "tableTotal");
        testStaticFields("CREATE_TABLE_TOTAL", "createTableTotal");
        testStaticFields("SNAPSHOT_TABLE_TOTAL", "snapshotTableTotal");
        testStaticFields("SNAPSHOT_ROW_TOTAL", "snapshotRowTotal");
        testStaticFields("SNAPSHOT_INSERT_ROW_TOTAL", "snapshotInsertRowTotal");
        testStaticFields("SNAPSHOT_START_AT", "snapshotStartAt");
        testStaticFields("SNAPSHOT_DONE_AT", "snapshotDoneAt");
        testStaticFields("SNAPSHOT_DONE_COST", "snapshotDoneCost");
        testStaticFields("CURR_SNAPSHOT_TABLE", "currentSnapshotTable");
        testStaticFields("CURR_SNAPSHOT_TABLE_ROW_TOTAL", "currentSnapshotTableRowTotal");
        testStaticFields("CURR_SNAPSHOT_TABLE_INSERT_ROW_TOTAL", "currentSnapshotTableInsertRowTotal");
        testStaticFields("OUTPUT_QPS_MAX", "outputQpsMax");
        testStaticFields("OUTPUT_QPS_AVG", "outputQpsAvg");
    }

    private void testStaticFields(String fieldName, String value) {
        Object v = null;
        try {
            Field field = TaskSampleHandler.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            v = field.get(null);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
        Assert.assertNotNull(v);
        Assert.assertEquals(value, v);
    }
}
