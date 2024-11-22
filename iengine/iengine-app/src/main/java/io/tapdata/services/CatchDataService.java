package io.tapdata.services;

import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.observable.logging.debug.DataCache;
import io.tapdata.observable.logging.debug.DataCacheFactory;
import io.tapdata.service.skeleton.annotation.RemoteService;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/11/20 18:45
 */
@RemoteService
public class CatchDataService {

    public boolean openCatchData(String taskId, Long recordCeiling, Long intervalCeiling) {
        return ObsLoggerFactory.getInstance().openCatchData(taskId, recordCeiling, intervalCeiling);
    }

    public List<MonitoringLogsDto> getCatchData(String taskId) {
        return Optional.ofNullable(DataCacheFactory.getInstance().getDataCache(taskId))
                .map(DataCache::getAndRemoveAll).orElse(Collections.emptyList());
    }

    public boolean closeCatchData(String taskId) {
        return ObsLoggerFactory.getInstance().closeCatchData(taskId);
    }

    public Map<String,Object> getCatchDataStatus(String taskId) {
        return ObsLoggerFactory.getInstance().getCatchDataStatus(taskId);
    }
}
