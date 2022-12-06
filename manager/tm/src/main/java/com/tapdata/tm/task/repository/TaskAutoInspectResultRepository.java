package com.tapdata.tm.task.repository;

import com.tapdata.tm.autoinspect.constants.ResultStatus;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.task.entity.TaskAutoInspectGroupTableResultEntity;
import com.tapdata.tm.task.entity.TaskAutoInspectResultEntity;
import org.bson.types.ObjectId;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/15 08:35 Create
 */
@Repository
public class TaskAutoInspectResultRepository extends BaseRepository<TaskAutoInspectResultEntity, ObjectId> {
    public TaskAutoInspectResultRepository(MongoTemplate mongoOperations) {
        super(TaskAutoInspectResultEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }

    public Page<TaskAutoInspectGroupTableResultEntity> groupByTable(String taskId, String tableName, long skip, int limit) {
        Assert.notNull(taskId, "taskId not null");

        Criteria where = Criteria.where("taskId").is(taskId);
        if (null != tableName && !tableName.isEmpty()) {
            where.and("originalTableName").regex(Pattern.compile(".*" + tableName + ".*"));
        }
        MatchOperation match = Aggregation.match(where);

        GroupOperation group = Optional.of(new String[]{"taskId", "originalTableName", "targetTableName", "sourceConnId", "targetConnId"}).map(fields -> {
            GroupOperation g = Aggregation.group(fields).count().as("counts");
            for (String f : fields) {
                g = g.first(f).as(f);
            }
            return g;
        }).orElse(null);

        Page<TaskAutoInspectGroupTableResultEntity> dataPage = new Page<>();

        //check data (group by taskId,originalTableName,targetTableName)
        List<TaskAutoInspectGroupTableResultEntity> dataCounts = mongoOperations.aggregate(Aggregation.newAggregation(match
                , group
                , Aggregation.group("taskId").first("taskId").as("taskId").count().as("counts")
        ), entityInformation.getCollectionName(), TaskAutoInspectGroupTableResultEntity.class).getMappedResults();

        //not found any data
        if (dataCounts.isEmpty() || dataCounts.get(0).getCounts() == 0) {
            return dataPage;
        }
        dataPage.setTotal(dataCounts.get(0).getCounts());

        //query data
        List<TaskAutoInspectGroupTableResultEntity> dataList = mongoOperations.aggregate(Aggregation.newAggregation(
                match,
                LookupOperation.newLookup().from("Connections").localField("sourceConnId").foreignField("_id").as("sourceConn"),
                Aggregation.unwind("sourceConn"),
                LookupOperation.newLookup().from("Connections").localField("targetConnId").foreignField("_id").as("targetConn"),
                Aggregation.unwind("targetConn"),
                group.sum(ConditionalOperators.when(Criteria.where("status").is(ResultStatus.ToBeCompared)).then(1).otherwise(0)).as("toBeCompared")
                        .first("sourceConn.name").as("sourceConnName")
                        .first("targetConn.name").as("targetConnName"),
                Aggregation.sort(Sort.Direction.ASC, "originalTableName"),
                Aggregation.skip(skip),
                Aggregation.limit(limit)
        ), entityInformation.getCollectionName(), TaskAutoInspectGroupTableResultEntity.class).getMappedResults();
        dataPage.setItems(dataList);

        return dataPage;
    }

    public Map<String, Object> totalDiffTables(String taskId) {
        Criteria where = Criteria.where("taskId").is(taskId);
        MatchOperation match = Aggregation.match(where);

        return mongoOperations.aggregate(Aggregation.newAggregation(
                match
                , Optional.of(new String[]{"taskId", "originalTableName"}).map(fields -> {
                    GroupOperation g = Aggregation.group(fields).count().as("counts");
                    for (String f : fields) {
                        g = g.first(f).as(f);
                    }
                    return g;
                }).get()
                , Aggregation.group().count().as("tables").sum("counts").as("totals")
        ), entityInformation.getCollectionName(), Map.class).getUniqueMappedResult();
    }
}
