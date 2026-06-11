package com.tapdata.tm.taskrebalance.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.taskrebalance.constant.TaskRebalanceJobStatus;
import com.tapdata.tm.taskrebalance.dto.TaskRebalanceJobDto;
import com.tapdata.tm.taskrebalance.entity.TaskRebalanceJobEntity;
import com.tapdata.tm.taskrebalance.repository.TaskRebalanceJobRepository;
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.Date;
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
        // No-op: rebalance jobs do not need extra save-time validation.
    }

    public boolean hasActiveJob(String taskId, UserDetail userDetail) {
        if (taskId == null) {
            return false;
        }
        Query query = Query.query(Criteria.where(TaskRebalanceJobDto.FIELD_TASK_ID).is(taskId)
                .and(TaskRebalanceJobDto.FIELD_STATUS).in(TaskRebalanceJobStatus.ACTIVE_STATUS));
        return count(query, userDetail) > 0;
    }

    public boolean hasBlockingActiveJob(String taskId, long timeoutMs, UserDetail userDetail) {
        if (taskId == null) {
            return false;
        }
        Query query = Query.query(Criteria.where(TaskRebalanceJobDto.FIELD_TASK_ID).is(taskId)
                .and(TaskRebalanceJobDto.FIELD_STATUS).in(TaskRebalanceJobStatus.STOPPING, TaskRebalanceJobStatus.STARTING));
        List<TaskRebalanceJobDto> jobs = findAllDto(query, userDetail);
        if (CollectionUtils.isEmpty(jobs)) {
            return false;
        }
        long now = System.currentTimeMillis();
        return jobs.stream().anyMatch(job -> !isJobTimedOut(job, now, timeoutMs));
    }

    private boolean isJobTimedOut(TaskRebalanceJobDto job, long now, long timeoutMs) {
        Date beginAt = job.getBeginAt();
        Date reference = beginAt == null ? job.getCreateAt() : beginAt;
        if (reference == null) {
            return false;
        }
        return now - reference.getTime() >= timeoutMs;
    }

    public boolean hasAnyActiveJob(List<String> taskIds, UserDetail userDetail) {
        if (CollectionUtils.isEmpty(taskIds)) {
            return false;
        }
        Query query = Query.query(Criteria.where(TaskRebalanceJobDto.FIELD_TASK_ID).in(taskIds)
                .and(TaskRebalanceJobDto.FIELD_STATUS).in(TaskRebalanceJobStatus.ACTIVE_STATUS));
        return count(query, userDetail) > 0;
    }

    public StatusStatistics countStatusByRebalanceId(String rebalanceId) {
        StatusStatistics statistics = new StatusStatistics();
        if (StringUtils.isBlank(rebalanceId)) {
            return statistics;
        }
        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.match(Criteria.where(TaskRebalanceJobDto.FIELD_REBALANCE_ID).is(rebalanceId)),
                Aggregation.group(TaskRebalanceJobDto.FIELD_STATUS).count().as("count")
        );
        AggregationResults<Document> results = repository.aggregate(aggregation, Document.class);
        for (Document result : results.getMappedResults()) {
            Object status = result.get("_id");
            Number count = result.get("count", Number.class);
            statistics.record(status == null ? null : status.toString(), count == null ? 0 : count.intValue());
        }
        return statistics;
    }

    public boolean isCheckBypassed() {
        return Boolean.TRUE.equals(BYPASS_REBALANCE_CHECK.get());
    }

    public boolean isRebalanceOperation() {
        return Boolean.TRUE.equals(REBALANCE_OPERATION.get());
    }

    public void runAsRebalanceOperation(Runnable runnable) {
        Boolean oldBypass = BYPASS_REBALANCE_CHECK.get();
        Boolean oldOperation = REBALANCE_OPERATION.get();
        BYPASS_REBALANCE_CHECK.set(true);
        REBALANCE_OPERATION.set(true);
        try {
            runnable.run();
        } finally {
            if (Boolean.TRUE.equals(oldBypass)) {
                BYPASS_REBALANCE_CHECK.set(true);
            } else {
                BYPASS_REBALANCE_CHECK.remove();
            }
            if (Boolean.TRUE.equals(oldOperation)) {
                REBALANCE_OPERATION.set(true);
            } else {
                REBALANCE_OPERATION.remove();
            }
        }
    }

    public static class StatusStatistics {
        private int pending;
        private int stopping;
        private int starting;
        private int ok;
        private int cancelled;
        private int failed;
        private int total;

        public void record(String status, int count) {
            if (count <= 0) {
                return;
            }
            total += count;
            if (TaskRebalanceJobStatus.PENDING.equals(status)) {
                pending += count;
            } else if (TaskRebalanceJobStatus.STOPPING.equals(status)) {
                stopping += count;
            } else if (TaskRebalanceJobStatus.STARTING.equals(status)) {
                starting += count;
            } else if (TaskRebalanceJobStatus.OK.equals(status)) {
                ok += count;
            } else if (TaskRebalanceJobStatus.CANCELLED.equals(status)) {
                cancelled += count;
            } else if (TaskRebalanceJobStatus.isTerminal(status)) {
                failed += count;
            }
        }

        public int getActiveCount() {
            return pending + stopping + starting;
        }

        public int getPending() {
            return pending;
        }

        public int getStopping() {
            return stopping;
        }

        public int getStarting() {
            return starting;
        }

        public int getOk() {
            return ok;
        }

        public int getCancelled() {
            return cancelled;
        }

        public int getFailed() {
            return failed;
        }

        public int getTotal() {
            return total;
        }
    }
}
