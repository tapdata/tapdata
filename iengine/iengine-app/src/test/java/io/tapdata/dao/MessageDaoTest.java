package io.tapdata.dao;

import com.tapdata.cache.CacheUtil;
import com.tapdata.cache.ICacheService;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.flow.engine.V2.util.SingleLockWithKey;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class MessageDaoTest {

    @Test
    void registrationFailsFastDuringBlockedCleanupAndCanRetryAfterwards() throws Exception {
        MessageDao messageDao = new MessageDao();
        ICacheService cacheService = mock(ICacheService.class);
        messageDao.setCacheService(cacheService);
        TaskDto stoppedTask = new TaskDto();
        stoppedTask.setId(new ObjectId());
        TaskDto newTask = new TaskDto();
        newTask.setId(stoppedTask.getId());
        CountDownLatch cleanupEntered = new CountDownLatch(1);
        CountDownLatch releaseCleanup = new CountDownLatch(1);
        ExecutorService workers = Executors.newFixedThreadPool(2);
        SingleLockWithKey taskLock = new SingleLockWithKey();
        String taskId = newTask.getId().toHexString();
        doAnswer(invocation -> {
            cleanupEntered.countDown();
            assertTrue(releaseCleanup.await(15, TimeUnit.SECONDS));
            return null;
        }).when(cacheService).destroy("old-cache");

        try {
            var cleanup = workers.submit(() -> messageDao.destroyCache(stoppedTask, "old-cache"));
            assertTrue(cleanupEntered.await(2, TimeUnit.SECONDS));
            var registration = workers.submit(() -> taskLock.call(taskId, () -> {
                messageDao.registerCache(null, null, null, newTask, null);
                return null;
            }));
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> registration.get(1, TimeUnit.SECONDS));
            assertInstanceOf(IllegalStateException.class, failure.getCause());
            assertTrue(failure.getCause().getMessage().contains("cleanup is still running"));
            verify(cacheService, never()).registerCache(any());
            assertTrue(taskLock.tryRun(taskId, () -> {}, 100, TimeUnit.MILLISECONDS),
                    "cache registration must not retain the per-task lock while cleanup is blocked");

            releaseCleanup.countDown();
            cleanup.get(2, TimeUnit.SECONDS);
            try (MockedStatic<CacheUtil> cacheUtil = mockStatic(CacheUtil.class)) {
                messageDao.registerCache(null, null, null, newTask, null);
                cacheUtil.verify(() -> CacheUtil.registerCache(null, null, null, null, cacheService));
            }
            verify(cacheService, times(1)).destroy("old-cache");
        } finally {
            releaseCleanup.countDown();
            workers.shutdownNow();
            assertTrue(workers.awaitTermination(2, TimeUnit.SECONDS));
        }
    }
}
