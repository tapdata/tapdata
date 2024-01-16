package io.tapdata.metric.collector;

import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.observable.logging.ObsLogger;

import java.util.List;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/12/13 14:23 Create
 */
public class NoneSyncMetricCollector implements ISyncMetricCollector {
    @Override
    public void snapshotBegin() {
        // none process logic
    }

    @Override
    public void snapshotCompleted() {
        // none process logic
    }

    @Override
    public void cdcBegin() {
        // none process logic
    }

    @Override
    public void log(TapBaseEvent tapEvent) {
        // none process logic
    }

    @Override
    public void log(List<? extends TapBaseEvent> tapEvents) {
        // none process logic
    }

    @Override
    public void close(ObsLogger obsLogger) {
        // none process logic
    }
}
