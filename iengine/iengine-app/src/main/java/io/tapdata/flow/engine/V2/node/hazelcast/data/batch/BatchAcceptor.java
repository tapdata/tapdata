package io.tapdata.flow.engine.V2.node.hazelcast.data.batch;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.pdk.apis.consumer.TapStreamReadConsumer;
import io.tapdata.pdk.core.async.ThreadPoolExecutorEx;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;


/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/11/17 17:54 Create
 * @description
 */
public final class BatchAcceptor {

    static class EventInfo {
        private final Object e;
        @Getter
        private final Object offset;
        @Getter
        private final int size;

        public EventInfo(List<TapEvent> events, Object offset) {
            this.e = events;
            this.offset = offset;
            this.size = events.size();
        }

        public EventInfo(TapEvent events, Object offset) {
            this.e = events;
            this.offset = offset;
            this.size = 1;
        }

        public Object getEvents() {
            return e;
        }

    }

    private final ValueGetter<Integer> batchSizeGetter;
    private final Supplier<Long> delayMsGetter;
    private final TapStreamReadConsumer<List<TapEvent>, Object> consumer;
    private final LinkedBlockingQueue<EventInfo> pipeline;
    private final Predicate<Boolean> alive;
    private final ObsLogger obsLogger;
    boolean active = false;
    Future<?> monitor;
    private final AtomicReference<Object> lastOffset = new AtomicReference<>();


    public BatchAcceptor(ValueGetter<Integer> batchSizeGetter, Supplier<Long> delayMsGetter, Predicate<Boolean> isAlive, TapStreamReadConsumer<List<TapEvent>, Object> consumer, ObsLogger obsLogger) {
        this.batchSizeGetter = batchSizeGetter;
        this.delayMsGetter = delayMsGetter;
        this.consumer = consumer;
        this.pipeline = new LinkedBlockingQueue<>(2000);
        this.alive = isAlive;
        this.obsLogger = obsLogger;
    }

    public void startMonitor(ThreadPoolExecutorEx sourceRunner) {
        this.active = true;
        this.monitor = sourceRunner.submit(this::consumer);
    }

    void consumer() {
        long lastTime = System.currentTimeMillis();
        List<TapEvent> events = new ArrayList<>();
        int count = 0;
        while (alive.test(null)) {
            EventInfo eventInfo = null;
            while (alive.test(null) && !pipeline.isEmpty()) {
                Long delay = delayMsGetter.get();
                long timeout = fixDelay(delay);
                eventInfo = pollSingle(timeout);
                if (eventInfo != null) {
                    classification(eventInfo, events);
                    Optional.ofNullable(eventInfo.getOffset()).ifPresent(this.lastOffset::set);
                    count += eventInfo.getSize();
                }
                if (count >= batchSizeGetter.get()) {
                    this.consumer.accept(events, this.lastOffset.get());
                    lastTime = System.currentTimeMillis();
                    events = new ArrayList<>();
                    count = 0;
                }
            }
            if (!events.isEmpty() && System.currentTimeMillis() - lastTime >= delayMsGetter.get()) {
                this.consumer.accept(events,  this.lastOffset.get());
                lastTime = System.currentTimeMillis();
                count = 0;
            }
        }
        this.active = false;
    }

    long fixDelay(Long delay) {
        return null == delay || delay <= 0L ? 1000L : delay;
    }

    void classification(EventInfo eventInfo, List<TapEvent> events) {
        if (eventInfo.getEvents() instanceof TapEvent e) {
            events.add(e);
        } else if (eventInfo.getEvents() instanceof Collection<?> list) {
            events.addAll((Collection<TapEvent>) list);
        }
    }

    void offerSingle(EventInfo eventInfo) {
        try {
            boolean offered = this.pipeline.offer(eventInfo, 3, TimeUnit.SECONDS);
            if (!offered) {
                // Handle the case where the offer was not successful
                // For example, log an error or take other appropriate action
                obsLogger.debug("Failed to offer eventInfo to the pipeline within 3 seconds.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    EventInfo pollSingle(long timeout) {
        EventInfo poll = null;
        try {
            poll = pipeline.poll(timeout, TimeUnit.SECONDS);
            return poll;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    public long getDelayMs() {
        return delayMsGetter.get();
    }

    public void accept(TapEvent event, Object offset) {
        if (!this.active || null == event) {
            return;
        }
        offerSingle(new EventInfo(event, offset));
    }

    public void accept(List<TapEvent> event, Object offset) {
        if (!this.active || null == event || event.isEmpty()) {
            return;
        }
        offerSingle(new EventInfo(event, offset));
    }

    public void close() {
        this.active = false;
        this.monitor.cancel(true);
    }


    public interface ValueGetter<T extends Number> {
        T get();
    }
}
