package io.tapdata.observable.metric.handler;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class TestTaskSampleHandler {
    TaskSampleHandler taskSampleHandler;
    @Before
    public void init() {
        taskSampleHandler = new TaskSampleHandler(null);
    }

    @Test
    public void testTaskSampleHandlerOfQpsType() {
        Assert.assertEquals(Constants.QPS_TYPE_MEMORY, taskSampleHandler.qpsType);
    }

    @Test
    public void testSamples() {
        List<String> samples = taskSampleHandler.samples();
        Assert.assertNotNull(samples);
        Assert.assertFalse(samples.isEmpty());
        Assert.assertTrue(samples.contains(Constants.INPUT_DDL_TOTAL));
        Assert.assertTrue(samples.contains(Constants.INPUT_INSERT_TOTAL));
        Assert.assertTrue(samples.contains(Constants.INPUT_UPDATE_TOTAL));
        Assert.assertTrue(samples.contains(Constants.INPUT_DELETE_TOTAL));
        Assert.assertTrue(samples.contains(Constants.INPUT_OTHERS_TOTAL));
        Assert.assertTrue(samples.contains(Constants.OUTPUT_DDL_TOTAL));
        Assert.assertTrue(samples.contains(Constants.OUTPUT_INSERT_TOTAL));
        Assert.assertTrue(samples.contains(Constants.OUTPUT_UPDATE_TOTAL));
        Assert.assertTrue(samples.contains(Constants.OUTPUT_DELETE_TOTAL));
        Assert.assertTrue(samples.contains(Constants.OUTPUT_OTHERS_TOTAL));
        Assert.assertTrue(samples.contains(Constants.INPUT_QPS));
        Assert.assertTrue(samples.contains(Constants.OUTPUT_QPS));
        Assert.assertTrue(samples.contains(Constants.INPUT_SIZE_QPS));
        Assert.assertTrue(samples.contains(Constants.OUTPUT_SIZE_QPS));
        Assert.assertTrue(samples.contains(Constants.TIME_COST_AVG));
        Assert.assertTrue(samples.contains(Constants.REPLICATE_LAG));
        Assert.assertTrue(samples.contains(Constants.CURR_EVENT_TS));
        Assert.assertTrue(samples.contains(Constants.QPS_TYPE));
        Assert.assertTrue(samples.contains("createTableTotal"));
        Assert.assertTrue(samples.contains("snapshotTableTotal"));
        Assert.assertTrue(samples.contains("snapshotRowTotal"));
        Assert.assertTrue(samples.contains("snapshotInsertRowTotal"));
        Assert.assertTrue(samples.contains("snapshotStartAt"));
        Assert.assertTrue(samples.contains("snapshotDoneAt"));
        Assert.assertTrue(samples.contains("snapshotDoneCost"));
        Assert.assertTrue(samples.contains("currentSnapshotTable"));
        Assert.assertTrue(samples.contains("currentSnapshotTableRowTotal"));
        Assert.assertTrue(samples.contains("currentSnapshotTableInsertRowTotal"));
        Assert.assertTrue(samples.contains("outputQpsMax"));
        Assert.assertTrue(samples.contains("outputQpsAvg"));
        Assert.assertTrue(samples.contains("outputSizeQpsMax"));
        Assert.assertTrue(samples.contains("outputSizeQpsAvg"));
        Assert.assertTrue(samples.contains("tableTotal"));
    }

}
