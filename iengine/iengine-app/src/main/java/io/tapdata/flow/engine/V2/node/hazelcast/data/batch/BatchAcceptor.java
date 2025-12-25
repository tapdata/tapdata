package io.tapdata.flow.engine.V2.node.hazelcast.data.batch;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.pdk.apis.consumer.TapStreamReadConsumer;
import io.tapdata.pdk.core.async.ThreadPoolExecutorEx;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;


/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/11/17 17:54 Create
 * @description
 */
public final class BatchAcceptor {
    private final ValueGetter<Integer> batchSizeGetter;
    private final Supplier<Long> delayMsGetter;
    private final LinkedBlockingQueue<TapEvent> pipeline;
    private final Map<TapEvent, Object> offsetMap;
    private final Predicate<Boolean> alive;
    private final ObsLogger obsLogger;
    boolean active = false;
    Future<?> monitor;
    private final AtomicReference<Object> lastOffset = new AtomicReference<>();
    private final TapStreamReadConsumer<List<TapEvent>, Object> consumer;

    public BatchAcceptor(ValueGetter<Integer> batchSizeGetter,
                           Supplier<Long> delayMsGetter,
                           Predicate<Boolean> isAlive,
                           TapStreamReadConsumer<List<TapEvent>, Object> consumer,
                           ObsLogger obsLogger) {
        this.batchSizeGetter = batchSizeGetter;
        this.delayMsGetter = delayMsGetter;
        this.pipeline = new LinkedBlockingQueue<>(5000);
        this.alive = isAlive;
        this.obsLogger = obsLogger;
        this.offsetMap = new HashMap<>(32);
        this.consumer = consumer;
    }

    public void updateBatchSize(int newSize) {
//        LinkedBlockingQueue<TapEvent> item = pipeline;
//        pipeline = new LinkedBlockingQueue<>(newSize * HazelcastSourcePdkBaseNode.SOURCE_QUEUE_FACTOR);
//        consumeOnce(item, -1);
    }

    public void startMonitor(ThreadPoolExecutorEx sourceRunner) {
        this.active = true;
        this.monitor = sourceRunner.submit(this::consumer);
    }

    public interface Timer {
        boolean expired();

        Duration remaining();

        public static Timer timer(Clock clock, Long time) {
            final TimeSince start = timeSince(clock);
            start.reset();

            return new Timer() {

                @Override
                public boolean expired() {
                    return start.elapsedTime() > time;
                }

                @Override
                public Duration remaining() {
                    return time > 0 ? Duration.of(time - start.elapsedTime(), ChronoUnit.MILLIS) : Duration.ZERO;
                }
            };
        }
    }

    public static interface TimeSince {
        /**
         * Reset the elapsed time to 0.
         */
        void reset();

        /**
         * Get the time that has elapsed since the last call to {@link #reset() reset}.
         *
         * @return the number of milliseconds
         */
        long elapsedTime();
    }

    public static TimeSince timeSince(Clock clock) {
        return new TimeSince() {
            private long lastTimeInMillis;

            @Override
            public void reset() {
                lastTimeInMillis = clock.currentTimeInMillis();
            }

            @Override
            public long elapsedTime() {
                long elapsed = clock.currentTimeInMillis() - lastTimeInMillis;
                return elapsed <= 0L ? 0L : elapsed;
            }
        };
    }

    public interface Clock {

        /**
         * The {@link Clock} instance that uses the {@link System} methods.
         */
        static final Clock SYSTEM = new Clock() {
            @Override
            public long currentTimeInMillis() {
                return System.currentTimeMillis();
            }

            @Override
            public long currentTimeInNanos() {
                return System.nanoTime();
            }

            @Override
            public Instant currentTimeAsInstant() {
                return Instant.now();
            }
        };

        /**
         * Get the {@link Clock} instance that uses the {@link System} methods.
         *
         * @return the system clock; never null
         */
        static Clock system() {
            return SYSTEM;
        }

        default Instant currentTime() {
            return Instant.ofEpochMilli(currentTimeInMillis());
        }

        /**
         * Get the current time in nanoseconds.
         *
         * @return the current time in nanoseconds.
         */
        default long currentTimeInNanos() {
            return currentTimeInMillis() * 1000000L;
        }

        /**
         * Get the current time in microseconds.
         *
         * @return the current time in microseconds.
         */
        default long currentTimeInMicros() {
            return TimeUnit.MICROSECONDS.convert(currentTimeInMillis(), TimeUnit.MILLISECONDS);
        }

        /**
         * Get the current time as an instant
         *
         * @return the current time as an instant.
         */
        default Instant currentTimeAsInstant() {
            return Instant.ofEpochMilli(currentTimeInMillis());
        }

        /**
         * Get the current time in milliseconds.
         *
         * @return the current time in milliseconds.
         */
        public long currentTimeInMillis();

    }

    public interface Metronome {
        default void start(long step) {

        }

        default void start() {

        }

        public void pause();

        public static Metronome sleeper(Long period, Clock timeSystem) {
            long periodInMillis = period;
            return new Metronome() {
                private long next = timeSystem.currentTimeInMillis() + periodInMillis;

                @Override
                public void start() {
                    start(periodInMillis);
                }

                @Override
                public void start(long step) {
                    next = timeSystem.currentTimeInMillis() + step;
                }


                @Override
                public void pause() {
                    for (; ; ) {
                        final long now = timeSystem.currentTimeInMillis();
                        if (next <= now) {
                            break;
                        }
                        try {
                            Thread.sleep(next - now);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    next = next + periodInMillis;
                }

                @Override
                public String toString() {
                    return "Metronome (sleep for " + periodInMillis + " ms)";
                }
            };
        }
    }


    void consumer() {
        while (alive.test(null)) {
            consumeOnce(this.pipeline, batchSizeGetter.get());
        }
        this.active = false;
    }

    void consumeOnce(LinkedBlockingQueue<TapEvent> item, int batchSize) {
        final Long delay = delayMsGetter.get();
        long timeout = fixDelay(delay);
        final Timer timer = Timer.timer(Clock.SYSTEM, timeout);
        final Metronome metronome = Metronome.sleeper(timeout, Clock.SYSTEM);
        final List<TapEvent> events = new ArrayList<>();
        while (!timer.expired()) {
            if (item.drainTo(events, batchSize) != 0) {
                break;
            }
            metronome.pause();
        }
        if (!events.isEmpty()) {
            events.parallelStream()
                    .forEach(e -> {
                        Object offset = offsetMap.get(e);
                        offsetMap.remove(e);
                        if (null != offset) {
                            this.lastOffset.set(offset);
                        }
                    });
            this.consumer.accept(events, this.lastOffset.get());
        }
    }

    long fixDelay(Long delay) {
        return null == delay || delay <= 0L ? 1000L : delay;
    }

    void offerSingle(TapEvent eventInfo, Object offset) {
        try {
            this.pipeline.put(eventInfo);
            if (null != offset) {
                this.offsetMap.put(eventInfo, offset);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // Handle the case where the offer was not successful
            // For example, log an error or take other appropriate action
            obsLogger.debug("Failed to offer eventInfo to the pipeline within 3 seconds.");
        }
    }

    public long getDelayMs() {
        return delayMsGetter.get();
    }

    public void accept(TapEvent event, Object offset) {
        if (!this.active || null == event) {
            return;
        }
        offerSingle(event, offset);
    }

    public void accept(List<TapEvent> event, Object offset) {
        if (!this.active || null == event || event.isEmpty()) {
            return;
        }
        for (int i = 0; i < event.size(); i++) {
            if (null == event.get(i)) {
                continue;
            }
            Object o = i == event.size() - 1 ? offset : null;
            offerSingle(event.get(i), o);
        }
    }

    public void close() {
        this.active = false;
        this.monitor.cancel(true);
    }

    public interface ValueGetter<T extends Number> {
        T get();
    }

}