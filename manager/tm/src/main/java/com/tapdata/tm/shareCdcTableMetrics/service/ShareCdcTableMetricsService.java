package com.tapdata.tm.shareCdcTableMetrics.service;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.TmPageable;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.shareCdcTableMetrics.ShareCdcTableMetricsDto;
import com.tapdata.tm.shareCdcTableMetrics.entity.ShareCdcTableMetricsEntity;
import com.tapdata.tm.shareCdcTableMetrics.repository.ShareCdcTableMetricsRepository;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.utils.Lists;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.aspectj.weaver.ast.Var;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @Author:
 * @Date: 2023/03/09
 * @Description:
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class ShareCdcTableMetricsService extends BaseService<ShareCdcTableMetricsDto, ShareCdcTableMetricsEntity, ObjectId, ShareCdcTableMetricsRepository> {

    private DataSourceService sourceService;

    public ShareCdcTableMetricsService(@NonNull ShareCdcTableMetricsRepository repository) {
        super(repository, ShareCdcTableMetricsDto.class, ShareCdcTableMetricsEntity.class);
    }

    protected void beforeSave(ShareCdcTableMetricsDto shareCdcTableMetrics, UserDetail user) {

    }

    public Page<ShareCdcTableMetricsDto> getPageInfo(String taskId, int page, int size) {
        TmPageable pageable = new TmPageable();
        pageable.setPage(page);
        pageable.setSize(size);

        Criteria criteria = Criteria.where("taskId").is(taskId);
        Query query = Query.query(criteria);

        long count = count(query);
        if (count == 0) {
            return new Page<>(count, Lists.newArrayList());
        }
        query.with(Sort.by(Sort.Direction.DESC, "currentEventTime"));
        query.with(pageable);

        // TODO: 2023/3/9 need add group by
        List<ShareCdcTableMetricsDto> list = findAll(query);
        List<String> connectionIds = list.stream().map(ShareCdcTableMetricsDto::getConnectionId).collect(Collectors.toList());

        List<DataSourceConnectionDto> connectionDtos = sourceService.findAllByIds(connectionIds);
        Map<ObjectId, String> nameMap = connectionDtos.stream().collect(Collectors.toMap(DataSourceConnectionDto::getId, DataSourceConnectionDto::getName, (existing, replacement) -> existing));

        list.forEach(info -> info.setConnectionId(nameMap.get(new ObjectId(info.getConnectionId()))));

        return new Page<>(count, list);
    }
}