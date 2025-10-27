package com.tapdata.tm.config.micrometer;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import io.firedome.MultiTaggedCounter;
import io.micrometer.core.instrument.Metrics;
import lombok.extern.slf4j.Slf4j;

/**
 * @Author knight
 * @Date 2025/8/12 11:17
 */

@Slf4j
public class MicrometerLogAppender extends AppenderBase<ILoggingEvent> {

    private final MultiTaggedCounter multiTaggedCounter;

    public MicrometerLogAppender() {
        this.multiTaggedCounter = new MultiTaggedCounter("tm_errors_log", Metrics.globalRegistry, "level");
    }
    @Override
    protected void append(ILoggingEvent event) {
        if (event.getLevel().toString().equals("ERROR")) {
            multiTaggedCounter.increment(event.getLevel().levelStr);
            log.info("MicrometerLogAppender increment tm_errors_log ERROR");
        } else if (event.getLevel().toString().equals("WARN")) {
            multiTaggedCounter.increment(event.getLevel().levelStr);
            log.info("MicrometerLogAppender increment tm_errors_log WARN");
        }
    }
}
