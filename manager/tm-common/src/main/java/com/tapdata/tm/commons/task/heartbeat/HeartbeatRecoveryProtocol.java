package com.tapdata.tm.commons.task.heartbeat;

import com.tapdata.tm.commons.task.dto.TaskDto;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.Map;

/** Engine and TM use the same compare-and-set document, scoped to the current task run. */
public final class HeartbeatRecoveryProtocol {
    private HeartbeatRecoveryProtocol() { }

    /** Scan payload deliberately excludes the potentially megabyte-sized DAG/schema. */
    public static Query observation(Query query) {
        query.fields().include("_id").include("name").include("userId").include("status")
                .include("agentId").include("taskRecordId").include("lastStartDate")
                .include("syncType").include("preview").include("shareCdcEnable")
                .include("attrs." + TaskDto.ATTRS_USED_SHARE_CACHE)
                .include("attrs." + HeartbeatWatchdog.CONFIG)
                .include("attrs." + HeartbeatWatchdog.HEALTH)
                .include("attrs." + HeartbeatWatchdog.RECOVERY).include("attrs.syncProgress");
        return query;
    }

    public static Query owner(TaskDto task) {
        return Query.query(Criteria.where("_id").is(task.getId())
                .and("status").is(TaskDto.STATUS_RUNNING)
                .and("agentId").is(task.getAgentId())
                .and("taskRecordId").is(task.getTaskRecordId())
                .and("lastStartDate").is(task.getLastStartDate()));
    }

    public static Query compare(TaskDto task, Map<String, Object> previous) {
        // Field comparisons avoid MongoDB's order-sensitive embedded-document equality.
        return owner(task).addCriteria(Criteria.where(HeartbeatWatchdog.RECOVERY_PATH + ".id").is(previous.get("id"))
                .and(HeartbeatWatchdog.RECOVERY_PATH + ".state").is(previous.get("state")));
    }

    public static Update write(Map<String, Object> next) {
        return Update.update(HeartbeatWatchdog.RECOVERY_PATH, next);
    }
}
