package com.tapdata.tm.inspect.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.inspect.dto.InspectDetailsDto;
import com.tapdata.tm.inspect.entity.InspectDetailsEntity;
import com.tapdata.tm.inspect.repository.InspectDetailsRepository;
import com.tapdata.tm.inspect.vo.FailTableAndRowsVo;
import lombok.NonNull;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;

import java.util.List;

public abstract class InspectDetailsService extends BaseService<InspectDetailsDto, InspectDetailsEntity, ObjectId, InspectDetailsRepository> {
    public InspectDetailsService(@NonNull InspectDetailsRepository repository) {
        super(repository, InspectDetailsDto.class, InspectDetailsEntity.class);
    }

    public FailTableAndRowsVo totalFailTableAndRows(String inspectResultId) {
        AggregationResults<FailTableAndRowsVo> results = repository.aggregate(Aggregation.newAggregation(
            Aggregation.match(Criteria.where("inspectResultId").is(inspectResultId).and("source").exists(true))
            , Aggregation.group("taskId").count().as("rows")
            , Aggregation.group().count().as("tableTotals").sum("rows").as("rowsTotals")
        ), FailTableAndRowsVo.class);
        List<FailTableAndRowsVo> mappedResults = results.getMappedResults();
        if (!mappedResults.isEmpty()) {
            return mappedResults.get(0);
        }
        return null;
    }

}
