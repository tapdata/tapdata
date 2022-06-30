package io.tapdata.pdk.apis.consumer;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.pdk.apis.utils.StateListener;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class StreamReadConsumer implements BiConsumer<List<TapEvent>, Object> {
    public static final int STATE_STREAM_READ_PENDING = 1;
    public static final int STATE_STREAM_READ_STARTED = 10;
    public static final int STATE_STREAM_READ_ENDED = 100;
    private int state = STATE_STREAM_READ_PENDING;

    private BiConsumer<List<TapEvent>, Object> consumer;
    private StateListener<Integer> stateListener;
    private boolean asyncMethodAndNoRetry = false;

    public void asyncMethodAndNoRetry() {
        asyncMethodAndNoRetry = true;
    }

    public synchronized void streamReadStarted() {
        if(state == STATE_STREAM_READ_STARTED)
            return;

        int old = state;
        state = STATE_STREAM_READ_STARTED;
        if(stateListener != null) {
            stateListener.stateChanged(old, state);
        }
    }

    public synchronized void streamReadEnded() {
        if(state == STATE_STREAM_READ_ENDED)
            return;

        int old = state;
        state = STATE_STREAM_READ_ENDED;
        if(stateListener != null) {
            stateListener.stateChanged(old, state);
        }
    }

    public int getState() {
        return state;
    }

    public boolean isAsyncMethodAndNoRetry() {
        return asyncMethodAndNoRetry;
    }

    public static StreamReadConsumer create(BiConsumer<List<TapEvent>, Object> consumer) {
        return new StreamReadConsumer().consumer(consumer);
    }

    private StreamReadConsumer consumer(BiConsumer<List<TapEvent>, Object> consumer) {
        this.consumer = consumer;
        return this;
    }

    public StreamReadConsumer stateListener(StateListener<Integer> stateListener) {
        this.stateListener = stateListener;
        return this;
    }

    @Override
    public void accept(List<TapEvent> events, Object offset) {
        consumer.accept(events, offset);
    }

}
