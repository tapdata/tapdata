package io.tapdata.observable.alert;

import io.tapdata.entity.logger.alert.TapAlertType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class TaskAlertDispatcherTest {

    @Test
    void submitShouldRejectImmediatelyWhenQueueIsFull() throws InterruptedException {
        CountDownLatch enteredPublisher = new CountDownLatch(1);
        CountDownLatch releasePublisher = new CountDownLatch(1);
        AtomicInteger published = new AtomicInteger();
        TaskAlertPublisher blockingPublisher = event -> {
            enteredPublisher.countDown();
            try {
                if (!releasePublisher.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("publisher was not released");
                }
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
            }
            published.incrementAndGet();
            return TaskAlertPublisher.PublishResult.SUCCESS;
        };
        TaskAlertDispatcher dispatcher = new TaskAlertDispatcher(blockingPublisher, 1, 1, 0, 1L, 1L);
        try {
            Assertions.assertTrue(dispatcher.submit(event("A")));
            Assertions.assertTrue(enteredPublisher.await(1, TimeUnit.SECONDS), "worker should take the first event");
            Assertions.assertTrue(dispatcher.submit(event("B")), "second event should occupy the only queue slot");

            long start = System.currentTimeMillis();
            boolean accepted = dispatcher.submit(event("C"));
            long elapsed = System.currentTimeMillis() - start;

            Assertions.assertTrue(elapsed < 100L);
            Assertions.assertFalse(accepted);
        } finally {
            releasePublisher.countDown();
            dispatcher.shutdown();
        }
    }

    @Test
    void sameKeyWithinWindowShouldCoalesce() throws InterruptedException {
        CountDownLatch publishedLatch = new CountDownLatch(1);
        AtomicInteger published = new AtomicInteger();
        TaskAlertPublisher publisher = event -> {
            published.incrementAndGet();
            publishedLatch.countDown();
            return TaskAlertPublisher.PublishResult.SUCCESS;
        };
        TaskAlertDispatcher dispatcher = new TaskAlertDispatcher(publisher, 16, 1, 0, 1L, 1L);
        try {
            dispatcher.submit(event("same"));
            dispatcher.submit(event("same"));
            Assertions.assertTrue(publishedLatch.await(2, TimeUnit.SECONDS));
        } finally {
            dispatcher.shutdown();
        }
        Assertions.assertEquals(1, published.get());
    }

    private TaskAlertEvent event(String dedupKey) {
        return TaskAlertEvent.builder()
                .eventId(java.util.UUID.randomUUID().toString())
                .type(TapAlertType.DATA_INTEGRITY)
                .code("SOURCE_CDC_EVENT_DISCARDED")
                .dedupKey(dedupKey)
                .taskId("task")
                .taskName("task")
                .nodeId("node")
                .message("msg")
                .occurredAt(System.currentTimeMillis())
                .build();
    }
}
