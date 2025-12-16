package io.tapdata.flow.engine.V2.node.hazelcast.data.batch;

import io.tapdata.entity.event.TapEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

class AdjustStageTest {

    static class TestAdjustStage implements AdjustStage {
        private boolean needAdjust;
        private Consumer<List<TapEvent>> consumer;
        private final TaskInfo taskInfo = new TaskInfo();
        private MetricInfo metricInfo;
        private String nodeId = "node-1";

        @Override
        public void needAdjustBatchSize(boolean open) {
            this.needAdjust = open;
        }

        @Override
        public void setConsumer(Consumer<List<TapEvent>> consumer) {
            this.consumer = consumer;
        }

        @Override
        public Consumer<List<TapEvent>> consumer() {
            return consumer;
        }

        @Override
        public TaskInfo getTaskInfo() {
            return taskInfo;
        }

        @Override
        public void metric(MetricInfo metricInfo) {
            this.metricInfo = metricInfo;
        }

        @Override
        public String getNodeId() {
            return nodeId;
        }

        @Override
        public void updateIncreaseReadSize(int increaseReadSize) {
            this.taskInfo.setIncreaseReadSize(increaseReadSize);
        }
    }

    @Nested
    class BasicMethodTest {
        @Test
        void testNeedAdjustAndConsumerAndMetric() {
            TestAdjustStage stage = new TestAdjustStage();
            stage.needAdjustBatchSize(true);

            List<TapEvent> received = new ArrayList<>();
            Consumer<List<TapEvent>> consumer = received::addAll;
            stage.setConsumer(consumer);

            AdjustStage.MetricInfo metricInfo = new AdjustStage.MetricInfo();
            metricInfo.setIncreaseReadSize(10);
            stage.metric(metricInfo);

            AdjustStage.TaskInfo taskInfo = stage.getTaskInfo();
            stage.updateIncreaseReadSize(5);

            Assertions.assertNotNull(stage.consumer());
            Assertions.assertEquals("node-1", stage.getNodeId());
            Assertions.assertEquals(5, taskInfo.getIncreaseReadSize());
            Assertions.assertEquals(10, metricInfo.getIncreaseReadSize());

            stage.consumer().accept(new ArrayList<>());
            Assertions.assertNotNull(received);
        }
    }

    @Nested
    class TaskInfoParseTest {
        @Test
        void testParseValidAndInvalid() {
            AdjustStage.TaskInfo taskInfo = new AdjustStage.TaskInfo();
            Number defaultVal = 1.23D;

            Number parsedValid = taskInfo.parse("3.14", defaultVal);
            Number parsedInvalid = taskInfo.parse("not-number", defaultVal);

            Assertions.assertEquals(3.14D, parsedValid.doubleValue());
            Assertions.assertEquals(defaultVal.doubleValue(), parsedInvalid.doubleValue());
        }
    }
}

