package io.tapdata.observable.metric.handler;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.common.sample.CollectorFactory;
import io.tapdata.common.sample.SampleCollector;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import util.TestUtil;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class TestTaskSampleHandler {
    TaskSampleHandler taskSampleHandler;
    SampleCollector collector;
    @Before
    public void init() {
        TaskDto taskDto = new TaskDto();
        taskDto.setId(new ObjectId());
        taskDto.setTestTaskId(UUID.randomUUID().toString());
        taskDto.setStartTime(new Date());
        collector = new SampleCollector();
        taskSampleHandler = new TaskSampleHandler(taskDto);
        TestUtil.setAndGetPrivateField(taskSampleHandler, AbstractHandler.class, "collector", collector);
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

    @Test
    public void testDoInit() {
        taskSampleHandler.doInit(new HashMap<>());
        Assert.assertNotNull(collector.getSpeedSampler("TABLE_TOTAL"));
        Assert.assertNotNull(collector.getCounterSampler(Constants.INPUT_DDL_TOTAL));
        Assert.assertNotNull(collector.getCounterSampler(Constants.INPUT_INSERT_TOTAL));
        Assert.assertNotNull(collector.getCounterSampler(Constants.INPUT_INSERT_TOTAL));
        Assert.assertNotNull(collector.getCounterSampler(Constants.INPUT_DELETE_TOTAL));
        Assert.assertNotNull(collector.getCounterSampler(Constants.INPUT_OTHERS_TOTAL));
        Assert.assertNotNull(collector.getCounterSampler(Constants.OUTPUT_DDL_TOTAL));
        Assert.assertNotNull(collector.getCounterSampler(Constants.OUTPUT_INSERT_TOTAL));
        Assert.assertNotNull(collector.getCounterSampler(Constants.OUTPUT_UPDATE_TOTAL));
        Assert.assertNotNull(collector.getCounterSampler(Constants.OUTPUT_DELETE_TOTAL));
        Assert.assertNotNull(collector.getCounterSampler(Constants.OUTPUT_OTHERS_TOTAL));
        Assert.assertNotNull(collector.getSpeedSampler(Constants.INPUT_QPS));
        Assert.assertNotNull(collector.getSpeedSampler(Constants.OUTPUT_QPS));
        Assert.assertNotNull(collector.getSpeedSampler(Constants.INPUT_SIZE_QPS));
        Assert.assertNotNull(collector.getSpeedSampler(Constants.OUTPUT_SIZE_QPS));
        Assert.assertNotNull(collector.getAverageSampler(Constants.TIME_COST_AVG));
        //Assert.assertNotNull(collector.getSpeedSampler(Constants.CURR_EVENT_TS));
        //Assert.assertNotNull(collector.getSpeedSampler(Constants.REPLICATE_LAG));
        Assert.assertNotNull(collector.getCounterSampler("createTableTotal"));
        Assert.assertNotNull(collector.getCounterSampler("snapshotTableTotal"));
        Assert.assertNotNull(collector.getCounterSampler("snapshotRowTotal"));
        Assert.assertNotNull(collector.getCounterSampler("snapshotInsertRowTotal"));
        //Assert.assertNotNull(collector.getSpeedSampler("snapshotStartAt"));
        //Assert.assertNotNull(collector.getSpeedSampler("snapshotDoneAt"));
        //Assert.assertNotNull(collector.getSpeedSampler("snapshotDoneCost"));
        Assert.assertNotNull(collector.getSpeedSampler("currentSnapshotTable"));
        //Assert.assertNotNull(collector.getCounterSampler("currentSnapshotTableRowTotal"));
        //Assert.assertNotNull(collector.getCounterSampler("currentSnapshotTableInsertRowTotal"));
        //Assert.assertNotNull(collector.getSpeedSampler("outputQpsMax"));
        //Assert.assertNotNull(collector.getSpeedSampler("outputQpsAvg"));
        //Assert.assertNotNull(collector.getSpeedSampler("tableTotal"));
        //Assert.assertNotNull(collector.getSpeedSampler("outputSizeQpsMax"));
        //Assert.assertNotNull(collector.getSpeedSampler("outputSizeQpsAvg"));
    }

}
