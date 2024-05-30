package com.tapdata.tm.task.service;

import cn.hutool.core.collection.CollUtil;
import com.tapdata.tm.commons.task.dto.ErrorEvent;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.repository.TaskRepository;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MongoUtils;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class TaskErrorEventService {
    protected static final String ERROR_EVENTS_SKIP_KEY = "errorEvents.$[element].skip";
    protected static final String ELEMENT_ID = "element._id";
    private TaskRepository taskRepository;
    private TaskService taskService;

    public List<ErrorEvent> getErrorEventByTaskId(String taskId, UserDetail user) {
        TaskDto taskDto = taskService.findOne(Query.query(Criteria.where("_id").is(MongoUtils.toObjectId(taskId))), user);
        if (null == taskDto) {
            return Lists.newArrayList();
        }
        return Optional.ofNullable(taskDto.getErrorEvents()).orElse(Lists.newArrayList());
    }

    public void signSkipErrorEvents(String taskId, List<String> ids) {
        Criteria criteria = Criteria.where("_id").is(MongoUtils.toObjectId(taskId));
        Query query = Query.query(criteria);
        if (CollUtil.isEmpty(ids)) {
            taskService.update(query, new Update().set(ERROR_EVENTS_SKIP_KEY, false).filterArray(Criteria.where(ELEMENT_ID).nin(Lists.newArrayList())));
            return;
        }
        BulkOperations bulkOperations = taskRepository.bulkOperations(BulkOperations.BulkMode.UNORDERED);
        List<ObjectId> objectIds = ids.stream().map(MongoUtils::toObjectId).collect(Collectors.toList());
        bulkOperations.updateMulti(query, new Update().set(ERROR_EVENTS_SKIP_KEY, true)
                    .filterArray(Criteria.where(ELEMENT_ID).in(objectIds)))
                .updateMulti(query, new Update().set(ERROR_EVENTS_SKIP_KEY, false)
                    .filterArray(Criteria.where(ELEMENT_ID).nin(objectIds)));
        bulkOperations.execute();
    }
}
