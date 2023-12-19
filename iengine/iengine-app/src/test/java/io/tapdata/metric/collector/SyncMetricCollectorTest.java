package io.tapdata.metric.collector;

import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.observable.logging.ObsLogger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/1/2 10:49 Create
 */
class SyncMetricCollectorTest {
    static TaskDto taskDto;
    static DataProcessorContext dataProcessorContext;
    static ISyncMetricCollector syncMetricCollector;

    @BeforeAll
    static void doInit() {
        taskDto = Mockito.mock(TaskDto.class);
        dataProcessorContext = Mockito.mock(DataProcessorContext.class);

        Mockito.when(dataProcessorContext.getTaskDto()).thenReturn(taskDto);
        Mockito.when(taskDto.getEnableSyncMetricCollector()).thenReturn(true);
    }

    @BeforeEach
    void beforeEach() {
        syncMetricCollector = ISyncMetricCollector.init(dataProcessorContext);
    }

    @Test
    void testNoneMetric() {
        // null referenceTime
        syncMetricCollector.log(TapInsertRecordEvent.create());
        // empty metric
        Assertions.assertTrue(syncMetricCollector.toString().contains("{}"));
    }

    @Test
    void testSnapshot() {
        long beginTimes = System.currentTimeMillis();
        // none snapshot metric
        Assertions.assertTrue(syncMetricCollector.toString().contains("{}"));
        syncMetricCollector.snapshotBegin();
        // none snapshot metric
        Assertions.assertTrue(syncMetricCollector.toString().contains("{}"));

        // snapshot duration is zero
        syncMetricCollector.log(TapInsertRecordEvent.create().referenceTime(beginTimes));
        ReflectionTestUtils.setField(syncMetricCollector, "snapshotBeginTimes", beginTimes);
        Assertions.assertFalse(syncMetricCollector.toString().contains("snapshotBegin"));
        ReflectionTestUtils.setField(syncMetricCollector, "snapshotCompletedTimes", beginTimes);
        Assertions.assertTrue(syncMetricCollector.toString().contains("snapshotBegin"));

        // null referenceTime
        syncMetricCollector.log(TapInsertRecordEvent.create());
        // insert records
        syncMetricCollector.log(TapInsertRecordEvent.create().referenceTime(System.currentTimeMillis()));
        // update records
        syncMetricCollector.log(TapUpdateRecordEvent.create().referenceTime(System.currentTimeMillis()));
        // delete records
        syncMetricCollector.log(TapDeleteRecordEvent.create().referenceTime(System.currentTimeMillis()));

        syncMetricCollector.snapshotCompleted();

        // exist snapshot metric
        Assertions.assertTrue(syncMetricCollector.toString().contains("snapshotBegin"));
    }


    @Test
    void testCdc() {
        long beginTimes = System.currentTimeMillis();
        syncMetricCollector.cdcBegin();

        // cdc duration is zero
        syncMetricCollector.log(TapInsertRecordEvent.create().referenceTime(beginTimes));
        ReflectionTestUtils.setField(syncMetricCollector, "cdcBeginTimes", beginTimes);
        ReflectionTestUtils.setField(syncMetricCollector, "cdcCompletedTimes", beginTimes);
        Assertions.assertTrue(syncMetricCollector.toString().contains("cdcBegin"));
//        syncMetricCollector.log(TapInsertRecordEvent.create().referenceTime(beginTimes));
//        Assertions.assertTrue(syncMetricCollector.toString().contains("cdcBegin"));

        List<TapBaseEvent> list = new ArrayList<>();
        // null referenceTime
        list.add(TapInsertRecordEvent.create());
        // insert records
        list.add(TapInsertRecordEvent.create().referenceTime(System.currentTimeMillis()));
        // update records
        list.add(TapUpdateRecordEvent.create().referenceTime(System.currentTimeMillis()));
        // delete records
        list.add(TapDeleteRecordEvent.create().referenceTime(System.currentTimeMillis()));
        syncMetricCollector.log(list);

        // exist cdc metric
        Assertions.assertTrue(syncMetricCollector.toString().contains("cdcBegin"));
    }

    @Test
    void testClose() {
        ObsLogger obsLogger = Mockito.mock(ObsLogger.class);
        syncMetricCollector.close(obsLogger);

        // Test multiple call
        syncMetricCollector.close(obsLogger);
    }

}
