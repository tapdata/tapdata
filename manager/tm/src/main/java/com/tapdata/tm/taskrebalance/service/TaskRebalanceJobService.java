package com.tapdata.tm.taskrebalance.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.taskrebalance.constant.TaskRebalanceJobStatus;
import com.tapdata.tm.taskrebalance.dto.TaskRebalanceJobDto;
import com.tapdata.tm.taskrebalance.entity.TaskRebalanceJobEntity;
import com.tapdata.tm.taskrebalance.repository.TaskRebalanceJobRepository;
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TaskRebalanceJobService extends BaseService<TaskRebalanceJobDto, TaskRebalanceJobEntity, ObjectId, TaskRebalanceJobRepository> {
    private static final ThreadLocal<Boolean> BYPASS_REBALANCE_CHECK = ThreadLocal.withInitial(() -> false);
    private static final ThreadLocal<Boolean> REBALANCE_OPERATION = ThreadLocal.withInitial(() -> false);

    public TaskRebalanceJobService(@NonNull TaskRebalanceJobRepository repository) {
        super(repository, TaskRebalanceJobDto.class, TaskRebalanceJobEntity.class);
    }

    @Override
    protected void beforeSave(TaskRebalanceJobDto dto, UserDetail userDetail) {
    }

    public boolean hasActiveJob(String taskId, UserDetail userDetail) {
        if (taskId == null) {
            return false;
        }
        Query query = Query.query(Criteria.where(TaskRebalanceJobDto.FIELD_TASK_ID).is(taskId)
                .and(TaskRebalanceJobDto.FIELD_STATUS).in(TaskRebalanceJobStatus.ACTIVE_STATUS));
        return count(query, userDetail) > 0;
    }

    public boolean hasAnyActiveJob(List<String> taskIds, UserDetail userDetail) {
        if (CollectionUtils.isEmpty(taskIds)) {
            return false;
        }
        Query query = Query.query(Criteria.where(TaskRebalanceJobDto.FIELD_TASK_ID).in(taskIds)
                .and(TaskRebalanceJobDto.FIELD_STATUS).in(TaskRebalanceJobStatus.ACTIVE_STATUS));
        return count(query, userDetail) > 0;
    }

    public boolean isCheckBypassed() {
        return Boolean.TRUE.equals(BYPASS_REBALANCE_CHECK.get());
    }

    public boolean isRebalanceOperation() {
        return Boolean.TRUE.equals(REBALANCE_OPERATION.get());
    }

    public void runWithoutRebalanceCheck(Runnable runnable) {
        Boolean old = BYPASS_REBALANCE_CHECK.get();
        BYPASS_REBALANCE_CHECK.set(true);
        try {
            runnable.run();
        } finally {
            BYPASS_REBALANCE_CHECK.set(old);
        }
    }

    public void runAsRebalanceOperation(Runnable runnable) {
        Boolean oldBypass = BYPASS_REBALANCE_CHECK.get();
        Boolean oldOperation = REBALANCE_OPERATION.get();
        BYPASS_REBALANCE_CHECK.set(true);
        REBALANCE_OPERATION.set(true);
        try {
            runnable.run();
        } finally {
            BYPASS_REBALANCE_CHECK.set(oldBypass);
            REBALANCE_OPERATION.set(oldOperation);
        }
    }
}
