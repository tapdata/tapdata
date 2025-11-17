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
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;

@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class RawServerStateService extends BaseService<RawServerStateDto, RawServerStateEntity, ObjectId, RawServerStateRepository> {
    public static final String SERVICE_ID = "serviceId";
    public static final String TIMESTAMP = "timestamp";
    public static final String DELETE = "delete";
    public RawServerStateService(@NonNull RawServerStateRepository repository) {
        super(repository, RawServerStateDto.class, RawServerStateEntity.class);
    }

    @Override
    protected void beforeSave(RawServerStateDto dto, UserDetail userDetail) {
        // do nothing
    }

    public Page<RawServerStateDto> getAllLatest(Filter filter) {
        filter.getWhere().put(DELETE, new Document().append("$ne", true));
        Aggregation aggregation = Aggregation.newAggregation(
                match(QueryUtil.parseWhereToCriteria(filter.getWhere())),
                sort(Sort.by(Sort.Direction.DESC, TIMESTAMP)),
                group(SERVICE_ID).max(TIMESTAMP).as(TIMESTAMP).first("$$ROOT").as("latestDoc"),
                project()
                        .and("latestDoc.dataSource").as("dataSource")
                        .and("latestDoc.reportedData").as("reportedData")
                        .and("latestDoc.serviceIP").as("serviceIP")
                        .and("latestDoc.servicePort").as("servicePort")
                        .and("latestDoc.serviceId").as(SERVICE_ID)
                        .and(TIMESTAMP).as(TIMESTAMP).andExclude("_id")
        );
        List<RawServerStateDto> results = new ArrayList<>(repository.aggregate(aggregation, RawServerStateDto.class).getMappedResults());
        results.forEach(v -> v.setIsAlive(System.currentTimeMillis() - v.getTimestamp().getTime() <= 3 * 60 * 1000L));
        results.sort(Comparator.comparing(RawServerStateDto::getTimestamp,
                        Comparator.nullsLast(Comparator.naturalOrder()))
                .thenComparing(RawServerStateDto::getServiceId,
                        Comparator.nullsLast(Comparator.naturalOrder())));
        return new Page<>(results.size(), results);
    }

    public void deleteAll(String serviceId) {
        repository.updateMany(Query.query(Criteria.where(SERVICE_ID).is(serviceId).and(DELETE).ne(true)), Update.update(DELETE, true));
    }
}
