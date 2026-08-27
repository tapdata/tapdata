package io.tapdata.dql.preview;

import java.util.Map;

/**
 * Safe, bounded Payload representation for DQL UI and diagnostics.
 */
public final class DqlPayloadPreview {
    private final Map<String, Object> payloadPreview;
    private final boolean truncated;

    public DqlPayloadPreview(Map<String, Object> payloadPreview, boolean truncated) {
        this.payloadPreview = payloadPreview;
        this.truncated = truncated;
    }

    public Map<String, Object> getPayloadPreview() {
        return payloadPreview;
    }

    public boolean isTruncated() {
        return truncated;
    }
}
