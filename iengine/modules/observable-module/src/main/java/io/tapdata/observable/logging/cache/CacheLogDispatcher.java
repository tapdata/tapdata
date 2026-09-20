package io.tapdata.observable.logging.cache;

import com.tapdata.tm.commons.schema.MonitoringLogsDto;

@FunctionalInterface
public interface CacheLogDispatcher {
    void dispatch(MonitoringLogsDto log, CacheLogSink sink);
}
