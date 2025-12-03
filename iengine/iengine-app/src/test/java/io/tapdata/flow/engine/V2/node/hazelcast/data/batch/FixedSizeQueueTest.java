package io.tapdata.flow.engine.V2.node.hazelcast.data.batch;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class FixedSizeQueueTest {

    @Nested
    class ConstructorTest {
        @Test
        void testConstructorWithInvalidCapacityAndThreshold() {
            FixedSizeQueue<Integer> q1 = new FixedSizeQueue<>(-1, 5D);
            FixedSizeQueue<Integer> q2 = new FixedSizeQueue<>(0, 5D);
            FixedSizeQueue<Integer> q3 = new FixedSizeQueue<>(10, -1D);
            FixedSizeQueue<Integer> q4 = new FixedSizeQueue<>(10, 0D);
            FixedSizeQueue<Integer> q5 = new FixedSizeQueue<>(10, 100D);
            FixedSizeQueue<Integer> q6 = new FixedSizeQueue<>(10, 101D);

            // capacity <= 0 -> 10, stableThreshold <= 0 || >= 100 -> 5D
            Assertions.assertEquals(0, q1.size());
            Assertions.assertEquals(0, q2.size());
            Assertions.assertEquals(0, q3.size());
            Assertions.assertEquals(0, q4.size());
            Assertions.assertEquals(0, q5.size());
            Assertions.assertEquals(0, q6.size());
        }

        @Test
        void testConstructorWithValidParams() {
            FixedSizeQueue<Integer> queue = new FixedSizeQueue<>(5, 3D);
            Assertions.assertEquals(0, queue.size());
        }
    }

    @Nested
    class PushTest {
        @Test
        void testPushNullReturnsNull() {
            FixedSizeQueue<Integer> queue = new FixedSizeQueue<>(5, 5D);
            Integer result = queue.push(null);
            Assertions.assertNull(result);
            Assertions.assertEquals(0, queue.size());
        }

        @Test
        void testPushWhenNotFull() {
            FixedSizeQueue<Integer> queue = new FixedSizeQueue<>(5, 5D);
            Integer r1 = queue.push(10);
            Integer r2 = queue.push(20);
            Assertions.assertEquals(10, r1);
            Assertions.assertEquals(20, r2);
            Assertions.assertEquals(2, queue.size());
        }

        @Test
        void testPushWhenFullAndStableWithItemInRange() {
            FixedSizeQueue<Integer> queue = new FixedSizeQueue<>(3, 5D);
            queue.push(10);
            queue.push(10);
            queue.push(10);

            // Now full, push 10 again (in range [10, 10], stable)
            Integer result = queue.push(10);
            Assertions.assertEquals(10, result);
            Assertions.assertEquals(3, queue.size());
        }

        @Test
        void testPushWhenFullAndStableWithItemOutOfRange() {
            FixedSizeQueue<Integer> queue = new FixedSizeQueue<>(3, 5D);
            queue.push(10);
            queue.push(11);
            queue.push(12);

            // avg=11, range=2, stable if range <= |avg|+5 && range > |avg|-5
            // Push 20 (out of range [10, 12])
            Integer result = queue.push(20);
            Assertions.assertEquals(20, result);
            Assertions.assertEquals(3, queue.size());
        }

        @Test
        void testPushWhenFullAndNotStable() {
            FixedSizeQueue<Integer> queue = new FixedSizeQueue<>(3, 1D);
            queue.push(10);
            queue.push(20);
            queue.push(30);

            // avg=20, range=20, not stable (range > |avg|+1)
            Integer result = queue.push(25);
            Assertions.assertEquals(25, result);
            Assertions.assertEquals(3, queue.size());
        }

        @Test
        void testPushWithDifferentNumberTypes() {
            FixedSizeQueue<Short> qShort = new FixedSizeQueue<>(2, 5D);
            qShort.push((short) 10);
            qShort.push((short) 20);
            Short rShort = qShort.push((short) 15);
            Assertions.assertNotNull(rShort);

            FixedSizeQueue<Long> qLong = new FixedSizeQueue<>(2, 5D);
            qLong.push(100L);
            qLong.push(200L);
            Long rLong = qLong.push(150L);
            Assertions.assertNotNull(rLong);

            FixedSizeQueue<Float> qFloat = new FixedSizeQueue<>(2, 5D);
            qFloat.push(10.5f);
            qFloat.push(20.5f);
            Float rFloat = qFloat.push(15.5f);
            Assertions.assertNotNull(rFloat);

            FixedSizeQueue<Double> qDouble = new FixedSizeQueue<>(2, 5D);
            qDouble.push(10.5);
            qDouble.push(20.5);
            Double rDouble = qDouble.push(15.5);
            Assertions.assertNotNull(rDouble);
        }
    }

    @Nested
    class PollAndSizeTest {
        @Test
        void testPollAndSize() {
            FixedSizeQueue<Integer> queue = new FixedSizeQueue<>(5, 5D);
            queue.push(1);
            queue.push(2);
            Assertions.assertEquals(2, queue.size());

            Integer polled = queue.poll();
            Assertions.assertEquals(1, polled);
            Assertions.assertEquals(1, queue.size());

            queue.poll();
            Assertions.assertEquals(0, queue.size());
            Assertions.assertNull(queue.poll());
        }
    }

    @Nested
    class ToStringTest {
        @Test
        void testToString() {
            FixedSizeQueue<Integer> queue = new FixedSizeQueue<>(5, 5D);
            queue.push(1);
            queue.push(2);
            String str = queue.toString();
            Assertions.assertNotNull(str);
            Assertions.assertTrue(str.contains("1"));
            Assertions.assertTrue(str.contains("2"));
        }
    }
}

