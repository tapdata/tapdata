package com.tapdata.tm.commons.task.heartbeat;

import com.tapdata.tm.commons.task.dto.TaskDto;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import java.util.Map;

/** Engine and TM use the same compare-and-set document, scoped to the current task run. */
public final class HeartbeatRecoveryProtocol {
    private HeartbeatRecoveryProtocol() { }

    public static Query owner(TaskDto task) {
        Map<String, Object> config = HeartbeatWatchdog.attr(task, HeartbeatWatchdog.CONFIG);
        return Query.query(Criteria.where("_id").is(task.getId())
                .and("status").is(TaskDto.STATUS_RUNNING)
                .and("agentId").is(task.getAgentId())
                .and("taskRecordId").is(task.getTaskRecordId())
                .and("lastStartDate").is(task.getLastStartDate())
                .and("attrs." + HeartbeatWatchdog.CONFIG + ".enabled").is(true)
                .and("attrs." + HeartbeatWatchdog.CONFIG + ".units").is(config.get("units"))
                .and("attrs." + HeartbeatWatchdog.CONFIG + ".timeoutMs").is(config.get("timeoutMs"))
                .and("attrs." + HeartbeatWatchdog.CONFIG + ".graceMs").is(config.get("graceMs")));
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
