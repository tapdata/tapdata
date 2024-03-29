package com.tapdata.tm.shareCdcTableMetrics.service;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.shareCdcTableMetrics.ShareCdcTableMetricsDto;
import com.tapdata.tm.shareCdcTableMetrics.entity.ShareCdcTableMetricsEntity;
import com.tapdata.tm.shareCdcTableMetrics.entity.ShareCdcTableMetricsVo;
import com.tapdata.tm.shareCdcTableMetrics.repository.ShareCdcTableMetricsRepository;
import lombok.NonNull;
import org.bson.types.ObjectId;

import java.util.List;

public abstract class ShareCdcTableMetricsService extends BaseService<ShareCdcTableMetricsDto, ShareCdcTableMetricsEntity, ObjectId, ShareCdcTableMetricsRepository> {
    public ShareCdcTableMetricsService(@NonNull ShareCdcTableMetricsRepository repository) {
        super(repository, ShareCdcTableMetricsDto.class, ShareCdcTableMetricsEntity.class);
    }
    public abstract Page<ShareCdcTableMetricsDto> getPageInfo(String taskId, String nodeId, String tableTaskId, String keyword, int page, int size);

    public abstract List<ShareCdcTableMetricsVo> getCollectInfoByTaskId(String taskId);

    public abstract void saveOrUpdateDaily(ShareCdcTableMetricsDto shareCdcTableMetricsDto, UserDetail userDetail);

    public abstract void deleteByTaskId(String taskId);

    public abstract void setSourceService(DataSourceService sourceService);

    public abstract void setMongoTemplate(org.springframework.data.mongodb.core.MongoTemplate mongoTemplate);

    public abstract void setTaskService(com.tapdata.tm.task.service.TaskService taskService);

    public enum Operation {
        INSERT,
        UPDATE,
    }
}
