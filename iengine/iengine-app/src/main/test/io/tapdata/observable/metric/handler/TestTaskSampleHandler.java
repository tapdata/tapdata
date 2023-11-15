package io.tapdata.observable.metric.handler;

import org.junit.Assert;
import org.junit.Test;
import util.TestUtil;

public class TestTaskSampleHandler {
//    TaskSampleHandler taskSampleHandler;
//    MockSampleCollector collector;
//    @Before
//    public void init() {
//        LoggerFactory.getLogger(SampleCollector.class.getSimpleName());
//        collector = new MockSampleCollector();
//        TaskDto taskDto = new TaskDto();
//        taskDto.setStartTime(new Date());
//        taskDto.setTaskRecordId(UUID.randomUUID().toString());
//        taskDto.setId(new ObjectId());
//        taskSampleHandler = new TaskSampleHandler(taskDto);
//        TestUtil.setAndGetPrivateField(taskSampleHandler, AbstractHandler.class, "collector", collector);
//    }

//    @Test
//    public void testTaskSampleHandlerOfQpsType() {
//        Assert.assertEquals(Constants.QPS_TYPE_MEMORY, taskSampleHandler.qpsType);
//    }

//    @Test
//    public void testSamples() {
//        List<String> samples = taskSampleHandler.samples();
//        Assert.assertNotNull(samples);
//        Assert.assertFalse(samples.isEmpty());
//        Assert.assertTrue(samples.contains(Constants.INPUT_DDL_TOTAL));
//        Assert.assertTrue(samples.contains(Constants.INPUT_INSERT_TOTAL));
//        Assert.assertTrue(samples.contains(Constants.INPUT_UPDATE_TOTAL));
//        Assert.assertTrue(samples.contains(Constants.INPUT_DELETE_TOTAL));
//        Assert.assertTrue(samples.contains(Constants.INPUT_OTHERS_TOTAL));
//        Assert.assertTrue(samples.contains(Constants.OUTPUT_DDL_TOTAL));
//        Assert.assertTrue(samples.contains(Constants.OUTPUT_INSERT_TOTAL));
//        Assert.assertTrue(samples.contains(Constants.OUTPUT_UPDATE_TOTAL));
//        Assert.assertTrue(samples.contains(Constants.OUTPUT_DELETE_TOTAL));
//        Assert.assertTrue(samples.contains(Constants.OUTPUT_OTHERS_TOTAL));
//        Assert.assertTrue(samples.contains(Constants.INPUT_QPS));
//        Assert.assertTrue(samples.contains(Constants.OUTPUT_QPS));
//        Assert.assertTrue(samples.contains(Constants.INPUT_SIZE_QPS));
//        Assert.assertTrue(samples.contains(Constants.OUTPUT_SIZE_QPS));
//        Assert.assertTrue(samples.contains(Constants.TIME_COST_AVG));
//        Assert.assertTrue(samples.contains(Constants.REPLICATE_LAG));
//        Assert.assertTrue(samples.contains(Constants.CURR_EVENT_TS));
//        Assert.assertTrue(samples.contains(Constants.QPS_TYPE));
//        Assert.assertTrue(samples.contains("createTableTotal"));
//        Assert.assertTrue(samples.contains("snapshotTableTotal"));
//        Assert.assertTrue(samples.contains("snapshotRowTotal"));
//        Assert.assertTrue(samples.contains("snapshotInsertRowTotal"));
//        Assert.assertTrue(samples.contains("snapshotStartAt"));
//        Assert.assertTrue(samples.contains("snapshotDoneAt"));
//        Assert.assertTrue(samples.contains("snapshotDoneCost"));
//        Assert.assertTrue(samples.contains("currentSnapshotTable"));
//        Assert.assertTrue(samples.contains("currentSnapshotTableRowTotal"));
//        Assert.assertTrue(samples.contains("currentSnapshotTableInsertRowTotal"));
//        Assert.assertTrue(samples.contains("outputQpsMax"));
//        Assert.assertTrue(samples.contains("outputQpsAvg"));
//        Assert.assertTrue(samples.contains("outputSizeQpsMax"));
//        Assert.assertTrue(samples.contains("outputSizeQpsAvg"));
//        Assert.assertTrue(samples.contains("tableTotal"));
//    }

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

//    @Test
//    public void testDoInit() {
//        taskSampleHandler.doInit(new HashMap<>());
//        Assert.assertNotNull(collector.getSpeedSampler("TABLE_TOTAL"));
//        Assert.assertNotNull(collector.getCounterSampler(Constants.INPUT_DDL_TOTAL));
//        Assert.assertNotNull(collector.getCounterSampler(Constants.INPUT_INSERT_TOTAL));
//        Assert.assertNotNull(collector.getCounterSampler(Constants.INPUT_INSERT_TOTAL));
//        Assert.assertNotNull(collector.getCounterSampler(Constants.INPUT_DELETE_TOTAL));
//        Assert.assertNotNull(collector.getCounterSampler(Constants.INPUT_OTHERS_TOTAL));
//        Assert.assertNotNull(collector.getCounterSampler(Constants.OUTPUT_DDL_TOTAL));
//        Assert.assertNotNull(collector.getCounterSampler(Constants.OUTPUT_INSERT_TOTAL));
//        Assert.assertNotNull(collector.getCounterSampler(Constants.OUTPUT_UPDATE_TOTAL));
//        Assert.assertNotNull(collector.getCounterSampler(Constants.OUTPUT_DELETE_TOTAL));
//        Assert.assertNotNull(collector.getCounterSampler(Constants.OUTPUT_OTHERS_TOTAL));
//        Assert.assertNotNull(collector.getSpeedSampler(Constants.INPUT_QPS));
//        Assert.assertNotNull(collector.getSpeedSampler(Constants.OUTPUT_QPS));
//        Assert.assertNotNull(collector.getSpeedSampler(Constants.INPUT_SIZE_QPS));
//        Assert.assertNotNull(collector.getSpeedSampler(Constants.OUTPUT_SIZE_QPS));
//        Assert.assertNotNull(collector.getAverageSampler(Constants.TIME_COST_AVG));
//        Assert.assertNotNull(collector.getCounterSampler("createTableTotal"));
//        Assert.assertNotNull(collector.getCounterSampler("snapshotTableTotal"));
//        Assert.assertNotNull(collector.getCounterSampler("snapshotRowTotal"));
//        Assert.assertNotNull(collector.getCounterSampler("snapshotInsertRowTotal"));
//        Assert.assertNotNull(collector.getSpeedSampler("currentSnapshotTable"));
//    }

}
