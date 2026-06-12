package com.tapdata.tm.utils;

import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.commons.task.dto.TaskDto;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.springframework.data.mongodb.core.query.Criteria;

import java.util.HashMap;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/5/28 15:30 Create
 * @description
 */
public final class TaskFilter {
    public static final String STATUS = "status";
    public static final String SYNC_TYPE = "syncType";
    private TaskFilter() {

    }

    public static void filter(Where where) {
        where.computeIfAbsent(STATUS, k -> {
            Document statusCondition = new Document();
            statusCondition.put("$nin", Lists.of(TaskDto.STATUS_DELETE_FAILED, TaskDto.STATUS_DELETING));
            return statusCondition;
        });
        //过滤掉挖掘任务
        String syncType = (String) where.get(SYNC_TYPE);
        if (StringUtils.isBlank(syncType)) {
            Document logCollectorFilter = new Document();
            logCollectorFilter.put("$nin", Lists.of(TaskDto.SYNC_TYPE_LOG_COLLECTOR, TaskDto.SYNC_TYPE_CONN_HEARTBEAT));
            where.put(SYNC_TYPE, logCollectorFilter);
        }

        //过滤调共享缓存任务
        HashMap<String, Object> notShareCache = new HashMap<>();
        notShareCache.put("$ne", true);
        where.put("shareCache", notShareCache);
    }

    public static Criteria filter(Criteria criteria) {
        return criteria.and(STATUS).nin(Lists.of(TaskDto.STATUS_DELETE_FAILED, TaskDto.STATUS_DELETING))
                .and(SYNC_TYPE).nin(Lists.of(TaskDto.SYNC_TYPE_LOG_COLLECTOR, TaskDto.SYNC_TYPE_CONN_HEARTBEAT))
                .and("shareCache").ne(true);
    }
}
