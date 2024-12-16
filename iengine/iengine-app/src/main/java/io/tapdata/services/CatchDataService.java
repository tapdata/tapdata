package io.tapdata.services;

import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.observable.logging.debug.DataCache;
import io.tapdata.observable.logging.debug.DataCacheFactory;
import io.tapdata.service.skeleton.annotation.RemoteService;

import java.util.*;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/11/20 18:45
 */
@RemoteService
public class CatchDataService {

    public boolean openCatchData(String taskId, Long recordCeiling, Long intervalCeiling, String query) {
        return ObsLoggerFactory.getInstance().openCatchData(taskId, recordCeiling, intervalCeiling);
    }

    public Map<String, Object> getCatchData(String taskId, Integer count, String query) {
        return Optional.ofNullable(DataCacheFactory.getInstance().getDataCache(taskId))
                .map(dataCache -> dataCache.searchAndRemove(count, query)).orElse(DataCache.emptyResult);
    }

    public boolean closeCatchData(String taskId) {
        return ObsLoggerFactory.getInstance().closeCatchData(taskId);
    }

    public Map<String,Object> getCatchDataStatus(String taskId) {
        return ObsLoggerFactory.getInstance().getCatchDataStatus(taskId);
    }
}
