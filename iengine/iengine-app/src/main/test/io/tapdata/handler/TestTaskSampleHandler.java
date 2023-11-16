package io.tapdata.handler;

import io.tapdata.observable.metric.handler.TaskSampleHandler;
import io.tapdata.util.TestUtil;
import org.junit.Assert;
import org.junit.Test;

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
        testStaticFields("OUTPUT_SIZE_QPS_MAX", "outputSizeQpsMax");
        testStaticFields("OUTPUT_SIZE_QPS_AVG", "outputSizeQpsAvg");
    }

    private void testStaticFields(String fieldName, String value) {
        Object v = TestUtil.getStaticField(TaskSampleHandler.class, fieldName);
        Assert.assertNotNull(v);
        Assert.assertEquals(value, v);
    }
}
