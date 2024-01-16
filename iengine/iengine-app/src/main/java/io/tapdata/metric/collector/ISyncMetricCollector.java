package io.tapdata.metric.collector;

import com.tapdata.entity.task.context.DataProcessorContext;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.observable.logging.ObsLogger;

import java.util.List;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/12/13 14:15 Create
 */
public interface ISyncMetricCollector {
    void snapshotBegin();

    void snapshotCompleted();

    void cdcBegin();

    void log(TapBaseEvent tapEvent);

    void log(List<? extends TapBaseEvent> tapEvents);

    void close(ObsLogger obsLogger);

    static ISyncMetricCollector init(DataProcessorContext dataProcessorContext) {
        if (null != dataProcessorContext
            && null != dataProcessorContext.getTaskDto()
            && Boolean.TRUE.equals(dataProcessorContext.getTaskDto().getEnableSyncMetricCollector())
        ) {
            return new SyncMetricCollector(100, 0);
        }
        return new NoneSyncMetricCollector();
    }

}
