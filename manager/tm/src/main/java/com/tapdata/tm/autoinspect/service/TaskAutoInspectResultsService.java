package com.tapdata.tm.autoinspect.service;

import com.tapdata.tm.autoinspect.dto.TaskAutoInspectResultDto;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.monitor.param.IdFilterPageParam;
import com.tapdata.tm.task.entity.TaskAutoInspectGroupTableResultEntity;
import com.tapdata.tm.task.entity.TaskAutoInspectResultEntity;
import com.tapdata.tm.task.repository.TaskAutoInspectResultRepository;
import com.tapdata.tm.user.service.UserService;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/15 08:25 Create
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class TaskAutoInspectResultsService extends BaseService<TaskAutoInspectResultDto, TaskAutoInspectResultEntity, ObjectId, TaskAutoInspectResultRepository> {
    private UserService userService;

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
}
