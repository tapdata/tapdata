package com.tapdata.tm.shareCdcTableMetrics.service;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.service.IBaseService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.shareCdcTableMetrics.ShareCdcTableMetricsDto;
import com.tapdata.tm.shareCdcTableMetrics.entity.ShareCdcTableMetricsEntity;
import com.tapdata.tm.shareCdcTableMetrics.entity.ShareCdcTableMetricsVo;
import com.tapdata.tm.shareCdcTableMetrics.repository.ShareCdcTableMetricsRepository;
import org.bson.types.ObjectId;

import java.util.List;

public interface ShareCdcTableMetricsService extends IBaseService<ShareCdcTableMetricsDto, ShareCdcTableMetricsEntity, ObjectId, ShareCdcTableMetricsRepository> {
    Page<ShareCdcTableMetricsDto> getPageInfo(String taskId, String nodeId, String tableTaskId, String keyword, int page, int size);

    List<ShareCdcTableMetricsVo> getCollectInfoByTaskId(String taskId);

    void saveOrUpdateDaily(ShareCdcTableMetricsDto shareCdcTableMetricsDto, UserDetail userDetail);

    void deleteByTaskId(String taskId);

    void setSourceService(DataSourceService sourceService);

    void setMongoTemplate(org.springframework.data.mongodb.core.MongoTemplate mongoTemplate);

    void setTaskService(com.tapdata.tm.task.service.TaskService taskService);

    public enum Operation {
        INSERT,
        UPDATE,
    }
}
