package com.tapdata.tm.autoinspect.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.autoinspect.constants.AutoInspectConstants;
import com.tapdata.tm.autoinspect.constants.CheckAgainStatus;
import com.tapdata.tm.autoinspect.constants.ResultStatus;
import com.tapdata.tm.autoinspect.dto.TaskAutoInspectResultDto;
import com.tapdata.tm.autoinspect.entity.CheckAgainProgress;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.monitor.param.IdFilterPageParam;
import com.tapdata.tm.task.entity.TaskAutoInspectGroupTableResultEntity;
import com.tapdata.tm.task.entity.TaskAutoInspectResultEntity;
import com.tapdata.tm.task.repository.TaskAutoInspectResultRepository;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.ws.enums.MessageType;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.data.mongodb.core.aggregation.ProjectionOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/15 08:25 Create
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class TaskAutoInspectResultsService extends BaseService<TaskAutoInspectResultDto, TaskAutoInspectResultEntity, ObjectId, TaskAutoInspectResultRepository> {
    private TaskService taskService;
    private MessageQueueService messageQueueService;

    public TaskAutoInspectResultsService(@NonNull TaskAutoInspectResultRepository repository) {
        super(repository, TaskAutoInspectResultDto.class, TaskAutoInspectResultEntity.class);
    }

    @Override
    protected void beforeSave(TaskAutoInspectResultDto dto, UserDetail userDetail) {
    }

    public Page<TaskAutoInspectGroupTableResultEntity> groupByTable(IdFilterPageParam param) {
        return repository.groupByTable(param.getId(), param.getFilter(), param.getSkip(), param.getLimit());
    }

    public long cleanResultsByTask(TaskDto taskDto) {
        Query query = Query.query(Criteria.where("taskId").is(taskDto.getId().toHexString()));
        return repository.deleteAll(query);
    }

    public Map<String, Object> totalDiffTables(String taskId) {
        return repository.totalDiffTables(taskId);
    }

    public UpdateResult checkAgainStart(String taskId, String processId, List<String> tables, String checkAgainSN, UserDetail userDetail) {
        CheckAgainProgress checkAgainProgress = new CheckAgainProgress(checkAgainSN);
        Update update = Update.update(AutoInspectConstants.CHECK_AGAIN_PROGRESS_PATH, checkAgainProgress);
        taskService.updateById(taskId, update, userDetail);

        Query query = Query.query(Criteria.where("taskId").is(taskId).and("originalTableName").in(tables).and("checkAgainSN").is(AutoInspectConstants.CHECK_AGAIN_DEFAULT_SN));
        update = Update.update("status", ResultStatus.ToBeCompared)
                .set("checkAgainSN", checkAgainSN)
                .set("last_updated", new Date());
        UpdateResult updateResult = repository.update(query, update, userDetail);

        JSONObject data = new JSONObject();
        data.put("taskId", taskId);
        data.put("type", MessageType.AUTO_INSPECT_AGAIN.getType());
        data.put("data", JSON.toJSONString(checkAgainProgress));

        // 通知 Engine
        messageQueueService.sendPipeMessage(data, null, processId);
        return updateResult;
    }

    public void checkAgainTimeout(String taskId, String checkAgainSN, UserDetail userDetail) {
        Query query = Query.query(Criteria.where("taskId").is(taskId).and("checkAgainSN").is(checkAgainSN));
        Update update = Update.update("status", ResultStatus.Completed).set("checkAgainSN", AutoInspectConstants.CHECK_AGAIN_DEFAULT_SN);
        repository.update(query, update, userDetail);

        update = Update.update(AutoInspectConstants.CHECK_AGAIN_PROGRESS_PATH + ".status", CheckAgainStatus.Timeout);
        taskService.updateById(taskId, update, userDetail);
    }

    public long countByTaskId(String taskId) {
        Query query = new Query(Criteria.where("taskId").is(taskId));

        return repository.count(query);
    }


    public Set<String> groupByTask(UserDetail user) {
        GroupOperation groupOperation = Aggregation.group("taskId");
        ProjectionOperation project = Aggregation.project("taskId");
        Aggregation aggregation = Aggregation.newAggregation(groupOperation, project);
        AggregationResults<Document> autoInspectResults = repository.getMongoOperations().aggregate(aggregation, "TaskAutoInspectResults", Document.class);
        List<Document> mappedResults = autoInspectResults.getMappedResults();

        Set<String> taskIds = new HashSet<>();

        if (CollectionUtils.isNotEmpty(mappedResults)) {
            taskIds = mappedResults.stream().map(d -> ((String) d.get("taskId"))).collect(Collectors.toSet());
        }

        return taskIds;
    }
}
