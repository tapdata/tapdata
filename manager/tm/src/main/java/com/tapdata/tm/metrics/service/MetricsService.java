package com.tapdata.tm.metrics.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metrics.dto.MetricsDto;
import com.tapdata.tm.metrics.entity.MetricsEntity;
import com.tapdata.tm.metrics.repository.MetricsRepository;
import lombok.NonNull;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Service;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/12/11 下午1:58
 */
@Service
public class MetricsService extends BaseService<MetricsDto, MetricsEntity, ObjectId, MetricsRepository> {
    public MetricsService(@NonNull MetricsRepository repository) {
        super(repository, MetricsDto.class, MetricsEntity.class);
    }

    @Override
    protected void beforeSave(MetricsDto dto, UserDetail userDetail) {

    }
}
