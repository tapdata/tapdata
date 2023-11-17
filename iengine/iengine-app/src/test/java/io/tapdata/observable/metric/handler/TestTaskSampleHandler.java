//package io.tapdata.io.tapdata.observable.metric.handler;
//
//import com.tapdata.tm.commons.task.dto.metric.TaskDtoEntity;
//import io.tapdata.common.sample.SampleCollector;
//import io.tapdata.observable.metric.handler.AbstractHandler;
//import io.tapdata.observable.metric.handler.TaskSampleHandler;
//import io.tapdata.util.TestUtil;
//import org.bson.types.ObjectId;
//import org.junit.Assert;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import java.util.Date;
//import java.util.HashMap;
//import java.util.UUID;
//
//public class TestTaskSampleHandler {
//    TaskSampleHandler sampleHandler;
//    @BeforeEach
//    public void init(){
//        TaskDtoEntity entity = new TaskDtoEntity();
//        entity.setId(new ObjectId());
//        entity.setTaskRecordId(UUID.randomUUID().toString());
//        entity.setStartTime(new Date());
//        sampleHandler = new TaskSampleHandler(entity);
//        SampleCollector sampleCollector = new SampleCollector((pointValues, tags) -> {
//            System.out.println("pointValues " + pointValues);
//            System.out.println("pointTags " + tags);
//            //上报到服务器
//        }).withName("agentProcess").withTag("a", "b").start();
//        TestUtil.setAndGetPrivateField(sampleHandler, AbstractHandler.class, "collector", sampleCollector);
//    }
//
//    @Test
//    public void testStaticFields() {
//        testStaticFields("SAMPLE_TYPE_TASK", "task");
//        testStaticFields("TABLE_TOTAL", "tableTotal");
//        testStaticFields("CREATE_TABLE_TOTAL", "createTableTotal");
//        testStaticFields("SNAPSHOT_TABLE_TOTAL", "snapshotTableTotal");
//        testStaticFields("SNAPSHOT_ROW_TOTAL", "snapshotRowTotal");
//        testStaticFields("SNAPSHOT_INSERT_ROW_TOTAL", "snapshotInsertRowTotal");
//        testStaticFields("SNAPSHOT_START_AT", "snapshotStartAt");
//        testStaticFields("SNAPSHOT_DONE_AT", "snapshotDoneAt");
//        testStaticFields("SNAPSHOT_DONE_COST", "snapshotDoneCost");
//        testStaticFields("CURR_SNAPSHOT_TABLE", "currentSnapshotTable");
//        testStaticFields("CURR_SNAPSHOT_TABLE_ROW_TOTAL", "currentSnapshotTableRowTotal");
//        testStaticFields("CURR_SNAPSHOT_TABLE_INSERT_ROW_TOTAL", "currentSnapshotTableInsertRowTotal");
//        testStaticFields("OUTPUT_QPS_MAX", "outputQpsMax");
//        testStaticFields("OUTPUT_QPS_AVG", "outputQpsAvg");
//    }
//
//    private void testStaticFields(String fieldName, String value) {
//        Object v = TestUtil.getStaticField(TaskSampleHandler.class, fieldName);
//        Assert.assertNotNull(v);
//        Assert.assertEquals(value, v);
//    }
//
//    @Test
//    public void testDoInit() {
//        sampleHandler.doInit(new HashMap<>());
//
//    }
//
//    @Test
//    public void testHandleBatchReadStart(){
//        sampleHandler.doInit(new HashMap<>());
//    }
//
//    @Test
//    public void testHandleStreamReadAccept() {
//        sampleHandler.doInit(new HashMap<>());
//    }
//
//    @Test
//    public void testHandleWriteRecordAccept(){
//        sampleHandler.doInit(new HashMap<>());
//    }
//}
