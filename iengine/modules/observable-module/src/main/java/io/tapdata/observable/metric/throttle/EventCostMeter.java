package io.tapdata.observable.metric.throttle;

/**
 * Zero-allocation production byte length calculator matching {@link EventV1ReferenceSerializer} without sorting map keys or materializing buffers.
 */
public final class EventCostMeter {

    private final int maxDepth;
    private final int maxWorkUnits;

    public EventCostMeter() {
        this(EventV1CodecSupport.DEFAULT_MAX_DEPTH, EventV1CodecSupport.DEFAULT_MAX_WORK_UNITS);
    }

    public EventCostMeter(int maxDepth, int maxWorkUnits) {
        if (maxDepth <= 0 || maxWorkUnits <= 0) {
            throw new IllegalArgumentException("maxDepth and maxWorkUnits must be positive");
        }
        this.maxDepth = maxDepth;
        this.maxWorkUnits = maxWorkUnits;
    }

    public long measure(EventV1View view) {
        return measure(view, this.maxWorkUnits);
    }

    public long measure(EventV1View view, int overrideMaxWorkUnits) {
        if (overrideMaxWorkUnits <= 0) {
            throw new IllegalArgumentException("maxWorkUnits must be positive");
        }
        EventV1CodecSupport.CountingSink sink = new EventV1CodecSupport.CountingSink();
        EventV1CodecSupport.emit(view, sink, false, maxDepth, overrideMaxWorkUnits);
        return sink.getCount();
    }
}
