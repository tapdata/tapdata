package io.tapdata.observable.metric.throttle;

/**
 * Deterministic canonical UTF-8 reference serializer for event-v1 parity testing and offline reference auditing.
 */
public final class EventV1ReferenceSerializer {

    private final int maxDepth;
    private final int maxWorkUnits;

    public EventV1ReferenceSerializer() {
        this(EventV1CodecSupport.DEFAULT_MAX_DEPTH, EventV1CodecSupport.DEFAULT_MAX_WORK_UNITS);
    }

    public EventV1ReferenceSerializer(int maxDepth, int maxWorkUnits) {
        if (maxDepth <= 0 || maxWorkUnits <= 0) {
            throw new IllegalArgumentException("maxDepth and maxWorkUnits must be positive");
        }
        this.maxDepth = maxDepth;
        this.maxWorkUnits = maxWorkUnits;
    }

    public byte[] serialize(EventV1View view) {
        EventV1CodecSupport.ByteArraySink sink = new EventV1CodecSupport.ByteArraySink();
        EventV1CodecSupport.emit(view, sink, true, maxDepth, maxWorkUnits);
        return sink.toByteArray();
    }
}
