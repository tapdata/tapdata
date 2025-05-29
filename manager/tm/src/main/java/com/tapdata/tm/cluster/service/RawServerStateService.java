package com.tapdata.tm.cluster.service;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.cluster.dto.RawServerStateDto;
import com.tapdata.tm.cluster.entity.RawServerStateEntity;
import com.tapdata.tm.cluster.repository.RawServerStateRepository;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.utils.QueryUtil;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.stereotype.Service;

import java.util.List;

import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;

@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class RawServerStateService extends BaseService<RawServerStateDto, RawServerStateEntity, ObjectId, RawServerStateRepository> {

    public RawServerStateService(@NonNull RawServerStateRepository repository) {
        super(repository, RawServerStateDto.class, RawServerStateEntity.class);
    }

    @Override
    protected void beforeSave(RawServerStateDto dto, UserDetail userDetail) {

    }

    public Page<RawServerStateDto> getAllLatest(Filter filter) {
        Aggregation aggregation = Aggregation.newAggregation(
                match(QueryUtil.parseWhereToCriteria(filter.getWhere())),
                group("serviceId").max("timestamp").as("timestamp").first("$$ROOT").as("latestDoc"),
                project()
                        .and("latestDoc.dataSource").as("dataSource")
                        .and("latestDoc.reportedData").as("reportedData")
                        .and("latestDoc.serviceId").as("serviceId")
                        .and("timestamp").as("timestamp").andExclude("_id")
        );
        List<RawServerStateDto> results = repository.aggregate(aggregation, RawServerStateDto.class).getMappedResults();
        results.forEach(v -> v.setIsAlive(System.currentTimeMillis() - v.getTimestamp().getTime() <= 3 * 60 * 1000L));
        return new Page<>(results.size(), results);
    }
}
