package com.tapdata.tm.shareCdcTableMetrics.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.shareCdcTableMetrics.ShareCdcTableMetricsDto;
import com.tapdata.tm.shareCdcTableMetrics.entity.ShareCdcTableMetricsEntity;
import com.tapdata.tm.shareCdcTableMetrics.repository.ShareCdcTableMetricsRepository;
import com.tapdata.tm.config.security.UserDetail;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Service;

/**
 * @Author:
 * @Date: 2023/03/09
 * @Description:
 */
@Service
@Slf4j
public class ShareCdcTableMetricsService extends BaseService<ShareCdcTableMetricsDto, ShareCdcTableMetricsEntity, ObjectId, ShareCdcTableMetricsRepository> {
    public ShareCdcTableMetricsService(@NonNull ShareCdcTableMetricsRepository repository) {
        super(repository, ShareCdcTableMetricsDto.class, ShareCdcTableMetricsEntity.class);
    }

    protected void beforeSave(ShareCdcTableMetricsDto shareCdcTableMetrics, UserDetail user) {

    }
}