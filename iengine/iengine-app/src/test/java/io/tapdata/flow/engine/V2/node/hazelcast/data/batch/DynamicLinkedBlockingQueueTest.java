package io.tapdata.flow.engine.V2.node.hazelcast.data.batch;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DynamicLinkedBlockingQueueTest {

    @Nested
    class ConstructorAndCapacityTest {
        @Test
        void testFixCapacityMinAndMaxAndNormal() {
            DynamicLinkedBlockingQueue<String> qMin = new DynamicLinkedBlockingQueue<>(-1);
            DynamicLinkedBlockingQueue<String> qZero = new DynamicLinkedBlockingQueue<>(0);
            DynamicLinkedBlockingQueue<String> qNormal = new DynamicLinkedBlockingQueue<>(1000);
            DynamicLinkedBlockingQueue<String> qMax = new DynamicLinkedBlockingQueue<>(200000);

            Assertions.assertEquals(100, qMin.capacity());
            Assertions.assertEquals(100, qZero.capacity());
            Assertions.assertEquals(1000, qNormal.capacity());
            Assertions.assertEquals(100000, qMax.capacity());
            Assertions.assertEquals(0, qMin.size());
            Assertions.assertTrue(qMin.isEmpty());
            Assertions.assertNotNull(qMin.getQueue());
            Assertions.assertFalse(qMin.active());
        }
    }

    @Nested
    class ChangeToAndMigrateTest {
        @Test
        void testChangeToTriggerMigrationAndPollFromNewAndOldQueue() throws Exception {
            DynamicLinkedBlockingQueue<Integer> queue = new DynamicLinkedBlockingQueue<>(100);
            queue.active(() -> true);

            for (int i = 0; i < 5; i++) {
                queue.offer(i);
            }
            try {
                queue.offer(100, 3, TimeUnit.MILLISECONDS);
            } catch (Exception e) {}

            int newSize = 800;
            queue.changeTo(200, 2);
            queue.changeTo(newSize, 2);
            queue.changeTo(50, 2);
            Thread.sleep(100L);

            List<Integer> polled = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                Integer v = queue.poll();
                polled.add(v);
            }
            Assertions.assertEquals(Lists.newArrayList(0, 1, 2, 3, 4), polled);
            Assertions.assertEquals(100, queue.poll());
            Assertions.assertTrue(queue.active());
        }
    }

    @Nested
    class PollAndDrainTest {
        @Test
        void testPollReturnNullWhenInactive() throws Exception {
            DynamicLinkedBlockingQueue<Integer> queue = new DynamicLinkedBlockingQueue<>(10);
            queue.active(() -> false);

            Assertions.assertNull(queue.poll());
            Assertions.assertNull(queue.poll(10, TimeUnit.MILLISECONDS));
        }
    }

    @Nested
    class DarinTest {
        @Test
        void testDrain() throws Exception {
            DynamicLinkedBlockingQueue<Integer> queue = new DynamicLinkedBlockingQueue<>(10);
            queue.active(() -> true);

            for (int i = 0; i < 5; i++) {
                queue.offer(i);
            }
            try {
                queue.offer(100, 3, TimeUnit.MILLISECONDS);
            } catch (Exception e) {}

            List<Integer> drained = new ArrayList<>();
            queue.drain(drained, 10, 10, TimeUnit.MILLISECONDS);
            Assertions.assertEquals(Lists.newArrayList(0, 1, 2, 3, 4, 100), drained);
        }
        @Test
        void testDrainEmpty() throws Exception {
            DynamicLinkedBlockingQueue<Integer> queue = new DynamicLinkedBlockingQueue<>(10);
            queue.active(() -> true);
            List<Integer> drained = new ArrayList<>();
            int res = queue.drain(drained, 10, 10, TimeUnit.MILLISECONDS);
            Assertions.assertEquals(0, res);
        }
    }
}