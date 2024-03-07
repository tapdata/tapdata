package io.tapdata.flow.engine.V2.retry.task;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.flow.engine.V2.node.hazelcast.HazelcastBaseNode;
import io.tapdata.flow.engine.V2.task.retry.task.TaskRetryFactory;
import io.tapdata.flow.engine.V2.task.retry.task.TaskRetryService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.spy;

class TaskRetryServiceTest {
    private TaskRetryFactory taskRetryFactory;
    private TaskDto taskDto;
    @BeforeEach
    void init() {
        taskRetryFactory = TaskRetryFactory.getInstance();
        ObjectId objectId = new ObjectId();
        taskDto = new TaskDto();
        taskDto.setId(objectId);
    }

    @Test
    void testTaskRetryFactorySingleton() {
        AtomicReference<TaskRetryFactory> taskRetryFactory1 = new AtomicReference<>();
        AtomicReference<TaskRetryFactory> taskRetryFactory2 = new AtomicReference<>();
        IntStream.range(0, 10).forEach(i -> {
            CountDownLatch countDownLatch = new CountDownLatch(2);
            new Thread(() -> {
                try {
                    taskRetryFactory1.set(TaskRetryFactory.getInstance());
                } finally {
                    countDownLatch.countDown();
                }
            }).start();
            new Thread(() -> {
                try {
                    taskRetryFactory2.set(TaskRetryFactory.getInstance());
                } finally {
                    countDownLatch.countDown();
                }
            }).start();
            try {
                countDownLatch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
            }
            assertNotNull(taskRetryFactory1.get());
            assertNotNull(taskRetryFactory2.get());
            assertEquals(taskRetryFactory1.get(), taskRetryFactory2.get());
        });
    }

    @Test
    void testGetAndRemoveTaskRetryService() {
        TaskRetryService taskRetryService = taskRetryFactory.getTaskRetryService(taskDto, TimeUnit.SECONDS.toMillis(3L));
        assertNotNull(taskRetryFactory.getTaskRetryService(taskDto.getId().toHexString()).orElse(null));
        assertEquals(taskRetryFactory.getTaskRetryService(taskDto.getId().toHexString()).orElse(null), taskRetryService);
        taskRetryFactory.removeTaskRetryService(taskDto.getId().toHexString());
        assertNull(taskRetryFactory.getTaskRetryService(taskDto.getId().toHexString()).orElse(null));
    }

    @Test
    void testTaskRetryStartEndTime() {
        long duration = 10 * 1000L;
        TaskRetryService taskRetryService = taskRetryFactory.getTaskRetryService(taskDto, duration);
        taskRetryService.start();
        long startTs = System.currentTimeMillis();
        long endTs = startTs + duration;
        assertNotNull(taskRetryService.getStartRetryTimeMs());
        assertTrue(startTs >= taskRetryService.getStartRetryTimeMs());
        assertNotNull(taskRetryService.getEndRetryTimeMs());
        assertTrue(endTs >= taskRetryService.getEndRetryTimeMs());
    }

    @Test
    void testMethodRetryDuration() {
        long duration = TimeUnit.SECONDS.toMillis(4L);
        long methodRetryIntervalMs = TimeUnit.SECONDS.toMillis(1L);
        long methodRetryTime = 3L;
        TaskRetryService taskRetryService = taskRetryFactory.getTaskRetryService(taskDto, duration,methodRetryIntervalMs, methodRetryTime);
        taskRetryService.start();
        long methodRetryDurationMs = taskRetryService.getMethodRetryDurationMs();
        assertTrue(methodRetryDurationMs >= methodRetryTime * methodRetryIntervalMs && methodRetryDurationMs <= duration * methodRetryIntervalMs);
        try {
            TimeUnit.SECONDS.sleep(TimeUnit.MILLISECONDS.toSeconds(duration - 1000L));
        } catch (InterruptedException ignored) {
        }
        methodRetryDurationMs = taskRetryService.getMethodRetryDurationMs();
        assertTrue(methodRetryDurationMs <= 1000L);
    }

    @Test
    void testMethodZeroRetryDuration() {
        long duration = 0L;
        long methodRetryIntervalMs = TimeUnit.SECONDS.toMillis(1L);
        long methodRetryTime = 3L;
        TaskRetryService taskRetryService = taskRetryFactory.getTaskRetryService(taskDto, duration,methodRetryIntervalMs, methodRetryTime);
        taskRetryService.start();
        assertEquals(taskRetryService.getMethodRetryDurationMs(), 0L);
    }
    @Nested
    class TestGetMethodRetryDurationMinutes {
        private TaskDto taskDto;
        @BeforeEach
        void setUp(){
            ObjectId objectId = new ObjectId();
            taskDto = new TaskDto();
            taskDto.setId(objectId);
        }
        @DisplayName("Test get methodRetryDurationMinutes when methodRetryDurationMinutes Greater than 0")
        @Test
        void test1(){
            TaskRetryService taskRetryService = TaskRetryFactory.getInstance().getTaskRetryService(taskDto, 900000L, 60000L, 15L);
            long methodRetryDurationMinutes = taskRetryService.getMethodRetryDurationMinutes();
            assertEquals(15L, methodRetryDurationMinutes);
        }
        @DisplayName("Test get methodRetryDurationMinutes when methodRetryDurationMinutes less than 0")
        @Test
        void test2(){
            ObjectId objectId = new ObjectId();
            taskDto.setId(objectId);
            TaskRetryService taskRetryService = TaskRetryFactory.getInstance().getTaskRetryService(taskDto, 0L, 60000L, 15L);
            long methodRetryDurationMinutes = taskRetryService.getMethodRetryDurationMinutes();
            assertEquals(0,methodRetryDurationMinutes);
        }
    }
    @Nested
    class GetRetryTimesTest{


        @DisplayName("Test get retryTime when retryIntervalSecond is 60L")
        @Test
        void test1(){
            long retryTimes = TaskRetryService.getRetryTimes(60L);
            assertEquals(15L,retryTimes);
        }
        @DisplayName("Test get retryTime when retryIntervalSecond is 0L")
        @Test
        void test2(){
            long retryTimes = TaskRetryService.getRetryTimes(0);
            assertEquals(15L,retryTimes);
        }
    }
}