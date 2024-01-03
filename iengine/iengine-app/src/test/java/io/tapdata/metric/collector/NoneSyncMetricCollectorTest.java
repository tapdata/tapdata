package io.tapdata.metric.collector;

import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.observable.logging.ObsLogger;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/1/2 14:19 Create
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class NoneSyncMetricCollectorTest {
    static ISyncMetricCollector syncMetricCollector;

    @BeforeAll
    static void doInit() {
        syncMetricCollector = ISyncMetricCollector.init(null);
    }

    @Nested
    @Order(1)
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class TestSnapshot {
        @Test
        @Order(1)
        void testSnapshotBegin() {
            syncMetricCollector.snapshotBegin();
        }

        @Test
        @Order(2)
        void testLog() {
            // null referenceTime
            syncMetricCollector.log(TapInsertRecordEvent.create());
        }

        @Test
        @Order(3)
        void testSnapshotCompleted() {
            syncMetricCollector.snapshotCompleted();
        }
    }

    @Nested
    @Order(2)
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class TestCdc {
        @Test
        @Order(1)
        void testCdcBegin() {
            syncMetricCollector.cdcBegin();
        }

        @Test
        @Order(2)
        void testLogByList() {
            List<TapBaseEvent> list = new ArrayList<>();
            syncMetricCollector.log(list);
        }
    }

    @Test
    @Order(3)
    void testDoClose() {
        ObsLogger obsLogger = Mockito.mock(ObsLogger.class);
        syncMetricCollector.close(obsLogger);
    }

}
