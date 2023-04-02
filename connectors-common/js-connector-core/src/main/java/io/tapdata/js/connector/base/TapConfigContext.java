package io.tapdata.js.connector.base;

public class TapConfigContext {
    private Long streamReadIntervalSecond = 60 * 1000L;

    public Long getStreamReadIntervalSeconds() {
        return this.streamReadIntervalSecond;
    }

    public void setStreamReadIntervalSeconds(Object streamReadIntervalSecond) {
        if (streamReadIntervalSecond instanceof Number) {
            this.streamReadIntervalSecond = ((Number) streamReadIntervalSecond).longValue() * 1000;
        }
    }
}
