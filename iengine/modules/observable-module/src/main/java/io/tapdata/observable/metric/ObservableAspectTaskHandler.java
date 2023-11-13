package io.tapdata.observable.metric;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;

import java.io.Closeable;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class ObservableAspectTaskHandler<T> implements Closeable {
    private static final String TAG = ObservableAspectTaskHandler.class.getSimpleName();
    AtomicBoolean alive;
    private Future<?> future;
    private final ExecutorService executor;

    public ObservableAspectTaskHandler(AtomicBoolean alive) {
        this.alive = alive;
        executor = new ThreadPoolExecutor(
            1,
            1,
            0L,
            TimeUnit.MILLISECONDS,
            new SynchronousQueue<>(), (r, executor) -> TapLogger.error(TAG, "Thread is rejected, runnable {} pool {}", r, executor)
        );
    }

    public Void handleAspectStart(LinkedBlockingQueue<T> queue, T aspect, String tag) {
        try {
            while (alive.get()) {
                if (queue.offer(aspect, 3, TimeUnit.SECONDS)) {
                    break;
                }
            }
        } catch (InterruptedException ignore) {
            TapLogger.warn(tag, "{} enqueue thread interrupted", tag);
            Thread.currentThread().interrupt();
        } catch (Throwable throwable) {
            throw new CoreException(0, throwable.getCause(), throwable.getMessage());
        }
        return null;
    }

    public <T, R>void aspectHandle(LinkedBlockingQueue<T> queue, Function<T, R> function) {
        Runnable runnable = () -> {
            while (alive.get() || !queue.isEmpty()) {
                T aspect = null;
                try {
                    aspect = queue.poll(500, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
                Optional.ofNullable(aspect).ifPresent(function::apply);
            }
        };
        if (Objects.nonNull(this.future)) {
            try {
                this.future.cancel(true);
            } catch (Exception ignore){ } finally {
                this.future = null;
            }
        }
        future = this.executor.submit(runnable);
    }

    @Override
    public void close() {
        try {
            Optional.ofNullable(future).ifPresent(f -> f.cancel(true));
        } catch (Exception ignore) {
            //...
        }
        try {
            executor.shutdown();
        } catch (Exception ignore) {
          //...
        }
    }
}
