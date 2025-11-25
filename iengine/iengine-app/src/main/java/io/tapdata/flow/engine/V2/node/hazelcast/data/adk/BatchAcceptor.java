package io.tapdata.flow.engine.V2.node.hazelcast.data.adk;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.consumer.TapStreamReadConsumer;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;


/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/11/17 17:54 Create
 * @description
 */
public final class BatchAcceptor {
    private final ValueGetter<Integer> batchSizeGetter;
    private final long delayMs;
    private final TapStreamReadConsumer<List<TapEvent>, Object> consumer;
    private List<TapEvent> values;
    private long lastAcceptTime;
    public BatchAcceptor(ValueGetter<Integer> batchSizeGetter, long delayMs, TapStreamReadConsumer<List<TapEvent>, Object> consumer) {
        this.batchSizeGetter = batchSizeGetter;
        this.delayMs = delayMs;
        this.consumer = consumer;
        this.values = new ArrayList<>();
        this.lastAcceptTime = 0L;
    }

    public long getDelayMs() {
        return delayMs;
    }

    public void accept(TapEvent event, Object offset) {
        if (null == event) {
            return;
        }
        Collection<TapEvent> events = new ArrayList<>();
        events.add(event);
        accept(events, offset);
    }

    public void accept(Collection<TapEvent> events, Object offset) {
        if (CollectionUtils.isEmpty(events)) {
            return;
        }
        List<TapEvent> es = events.stream()
                .filter(Objects::nonNull)
                .toList();
        if (CollectionUtils.isEmpty(es)) {
            return;
        }
        this.values.addAll(es);
        if (this.values.size() >= batchSizeGetter.batchSize() ||
                (this.lastAcceptTime > 0L && System.currentTimeMillis() - this.lastAcceptTime > this.delayMs)) {
            consumer.accept(this.values, offset);
            this.values = new ArrayList<>();
            this.lastAcceptTime = System.currentTimeMillis();
        }
    }


    public interface ValueGetter<T extends Number> {
        T batchSize();
    }
}
