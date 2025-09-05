package io.tapdata.common;

import io.micrometer.core.instrument.Metrics;
import io.tapdata.firedome.MultiTaggedCounter;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;

/**
 * @Author knight
 * @Date 2025/8/11 17:49
 */
@Slf4j
public class MicrometerLogAppender extends AbstractAppender {

    private final MultiTaggedCounter multiTaggedCounter;

    public MicrometerLogAppender() {
        super("MicrometerLogAppender", null, null, true, null);
        this.multiTaggedCounter = new MultiTaggedCounter("ef_errors_log", Metrics.globalRegistry, "level");
    }
    @Override
    public void append(LogEvent event) {
        if (event.getLevel().isMoreSpecificThan(Level.ERROR)) {
            multiTaggedCounter.increment(event.getLevel().name());
            log.info("MicrometerLogAppender increment ef_errors_log ERROR");
        } else if (event.getLevel().isMoreSpecificThan(Level.WARN)) {
            multiTaggedCounter.increment(event.getLevel().name());
            log.info("MicrometerLogAppender increment ef_errors_log WARN");
        }
    }
}
