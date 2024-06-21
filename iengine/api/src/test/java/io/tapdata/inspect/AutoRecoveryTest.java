package io.tapdata.inspect;

import com.tapdata.entity.TapdataRecoveryEvent;
import io.tapdata.inspect.exception.DuplicateAutoRecoveryException;
import io.tapdata.inspect.exception.DuplicateClientAutoRecoveryException;
import io.tapdata.inspect.exception.NotfoundAutoRecoveryException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 自动恢复数据测试
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/6/14 10:32 Create
 */
class AutoRecoveryTest {
    private static final String taskId = "test-task-id";
    private static final String inspectTaskId = "test-inspect-task-id";
    private static final long interval = 50;

    @Test
    void testDuplicateServer() {
        Assertions.assertNotNull(Assertions.assertThrows(DuplicateAutoRecoveryException.class, () -> {
            try (AutoRecovery ignore1 = AutoRecovery.init(taskId);
                 AutoRecovery ignore2 = AutoRecovery.init(taskId)) {
                // do nothing
            }
        }), "Duplicate auto-recovery detected");
    }

    @Test
    void testClientFailedOfServerNotExists() {
        Assertions.assertNotNull(Assertions.assertThrows(NotfoundAutoRecoveryException.class, () -> {
            try (AutoRecoveryClient ignoreClient = AutoRecovery.initClient(taskId, inspectTaskId, tapdataRecoveryEvent -> {
            })) {
                // do nothing
            }
        }), "Auto-recovery detected");
    }

    @Test
    void testDuplicateClient() {
        Assertions.assertNotNull(Assertions.assertThrows(DuplicateClientAutoRecoveryException.class, () -> {
            try (
                AutoRecovery ignoreServer = AutoRecovery.init(taskId);
                AutoRecoveryClient ignoreClient1 = AutoRecovery.initClient(taskId, inspectTaskId, tapdataRecoveryEvent -> {
                });
                AutoRecoveryClient ignoreClient2 = AutoRecovery.initClient(taskId, inspectTaskId, tapdataRecoveryEvent -> {
                })
            ) {
                // do nothing
            }
        }), "Duplicate auto-recovery client detected");
    }

    @Test
    void testMainFlow() {
        String inspectTaskId1 = "test-inspect-task-id-1";
        String inspectTaskId2 = "test-inspect-task-id-2";
        AtomicBoolean exited = new AtomicBoolean(false);
        CountDownLatch afterServerInitialized = new CountDownLatch(1);
        List<Throwable> errors = new ArrayList<>();

        CompletableFuture<Void> serverFuture = CompletableFuture.runAsync(() -> {
            try (AutoRecovery ignoreAutoRecovery = AutoRecovery.init(taskId)) {
                ConcurrentLinkedQueue<TapdataRecoveryEvent> queue = new ConcurrentLinkedQueue<>();

                // 初始化 enqueue 后才能初始化 client
                AutoRecovery.setEnqueueConsumer(taskId, tapdataRecoveryEvent -> {
                    try {
                        while (!queue.offer(tapdataRecoveryEvent) && !exited.get()) {
                            TimeUnit.MILLISECONDS.sleep(interval);
                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
                afterServerInitialized.countDown();

                // 消费到 end 事件后退出
                boolean inspectTaskId1Completed = false;
                boolean inspectTaskId2Completed = false;
                while (!Thread.currentThread().isInterrupted() && !exited.get()) {
                    TapdataRecoveryEvent event = queue.poll();
                    if (event != null) {
                        System.out.printf("Accept recovery record '%s': %s\n", event.getInspectTaskId(), event.getRecoveryType());
                        AutoRecovery.completed(taskId, event);
                        if (TapdataRecoveryEvent.RECOVERY_TYPE_END.equals(event.getRecoveryType())) {
                            if (inspectTaskId1.equals(event.getInspectTaskId())) {
                                inspectTaskId1Completed = true;
                            } else if (inspectTaskId2.equals(event.getInspectTaskId())) {
                                inspectTaskId2Completed = true;
                            }

                            if (inspectTaskId1Completed && inspectTaskId2Completed) {
                                break;
                            }
                        }
                    } else {
                        TimeUnit.MILLISECONDS.sleep(interval);
                    }
                }
            } catch (Exception e) {
                errors.add(e);
            }
        });

        try {
            // 验证一个同步任务多校验任务场景
            CompletableFuture<Void> allFuture = CompletableFuture.allOf(serverFuture
                , startClient(taskId, inspectTaskId1, afterServerInitialized, errors)
                , startClient(taskId, inspectTaskId2, afterServerInitialized, errors)
            );
            allFuture.get(5, TimeUnit.SECONDS);

            if (!errors.isEmpty()) {
                Assertions.fail(errors.get(0));
            }
        } catch (Exception e) {
            exited.set(true);
            Assertions.assertNull(e);
        }
    }

    private static CompletableFuture<Void> startClient(String taskId, String inspectTaskId, CountDownLatch afterServerInitialized, List<Throwable> errors) {
        return CompletableFuture.runAsync(() -> {
            try {
                afterServerInitialized.await(2, TimeUnit.SECONDS);

                CountDownLatch completed = new CountDownLatch(1);
                try (AutoRecoveryClient recoveryClient = AutoRecovery.initClient(taskId, inspectTaskId, tapdataRecoveryEvent -> {
                    if (TapdataRecoveryEvent.RECOVERY_TYPE_END.equals(tapdataRecoveryEvent.getRecoveryType())) {
                        completed.countDown();
                    }
                })) {
                    recoveryClient.enqueue(TapdataRecoveryEvent.createBegin(inspectTaskId));

                    String tableId = inspectTaskId + "-table-id";
                    Map<String, Object> data = new HashMap<>();
                    recoveryClient.enqueue(TapdataRecoveryEvent.createInsert(inspectTaskId, tableId, data));

                    recoveryClient.enqueue(TapdataRecoveryEvent.createEnd(inspectTaskId));

                    completed.await(5, TimeUnit.SECONDS);
                }
            } catch (Exception e) {
                errors.add(e);
            }
        });
    }

}
