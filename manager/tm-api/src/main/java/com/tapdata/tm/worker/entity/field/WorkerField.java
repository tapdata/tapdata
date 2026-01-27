package com.tapdata.tm.worker.entity.field;

import com.tapdata.tm.base.field.CollectionField;

/**
 * @see com.tapdata.tm.worker.entity.Worker
 */
public enum WorkerField implements CollectionField {
    PROCESS_ID("process_id"),
    WORKER_TYPE("worker_type"),
    PLATFORM_INFO("platformInfo"),
    AGENT_TAGS("agentTags"),
    TCM("tcm"),
    PING_TIME("ping_time"),
    WORKER_IP("worker_ip"),
    CPU_LOAD("cpuLoad"),
    HOSTNAME("hostname"),
    JOB_IDS("job_ids"),
    RUNNING_THREAD("running_thread"),
    TOTAL_THREAD("total_thread"),
    USED_MEMORY("usedMemory"),
    VERSION("version"),
    ACCESS_CODE("accessCode"),
    METRIC_VALUES("metricValues"),
    STOPPING("stopping"),
    IS_DELETED("isDeleted"),
    DELETED("deleted"),
    PING_DATE("pingDate"),
    START_TIME("startTime"),
    GIT_COMMIT_ID("gitCommitId"),
    UPDATE_STATUS("updateStatus"),
    UPDATE_MSG("updateMsg"),
    UPDATE_TIME("updateTime"),
    UPDATE_VERSION("updateVersion"),
    UPDATE_PING_TIME("updatePingTime"),
    PRO_GRES("progres"),
    WORKER_STATUS("workerStatus"),
    SINGLETON_LOCK("singletonLock"),
    LICENSE_BIND("licenseBind"),
    WORKER_DATE("workerDate"),
    AUDIT_LOG_PUSH_MAX_DELAY("auditLogPushMaxDelay");

    final String field;

    WorkerField(String fieldName) {
        this.field = fieldName;
    }

    @Override
    public String field() {
        return field;
    }

    public enum ApiServerStatus implements CollectionField {
        WORKER_PROCESS_ID(WorkerField.WORKER_STATUS.field.concat(".worker_process_id")),
        WORKER_PROCESS_START_TIME(WorkerField.WORKER_STATUS.field.concat(".worker_process_start_time")),
        WORKER_PROCESS_END_TIME(WorkerField.WORKER_STATUS.field.concat(".worker_process_end_time")),
        EXIT_CODE(WorkerField.WORKER_STATUS.field.concat(".exit_code")),
        PID(WorkerField.WORKER_STATUS.field.concat(".pid")),
        ACTIVE_TIME(WorkerField.WORKER_STATUS.field.concat(".activeTime")),
        STATUS(WorkerField.WORKER_STATUS.field.concat(".status")),
        WORKERS(WorkerField.WORKER_STATUS.field.concat(".workers")),
        METRIC_VALUES(WorkerField.WORKER_STATUS.field.concat(".metricValues")),
        UPDATE_CPU_MEM(WorkerField.WORKER_STATUS.field.concat(".updateCpuMem")),
        AUDIT_LOG_PUSH_MAX_DELAY(WorkerField.WORKER_STATUS.field.concat(".auditLogPushMaxDelay"));
        final String field;

        ApiServerStatus(String field) {
            this.field = field;
        }

        @Override
        public String field() {
            return field;
        }
    }

    public enum PlatformInfo implements CollectionField {
        REGION(WorkerField.PLATFORM_INFO.field.concat(".region")),
        ZONE(WorkerField.PLATFORM_INFO.field.concat(".zone")),
        SOURCE_TYPE(WorkerField.PLATFORM_INFO.field.concat(".sourceType")),
        DRS_INSTANCES(WorkerField.PLATFORM_INFO.field.concat(".DRS_instances")),
        IP_TYPE(WorkerField.PLATFORM_INFO.field.concat(".IP_type")),
        VPC(WorkerField.PLATFORM_INFO.field.concat(".vpc")),
        ECS(WorkerField.PLATFORM_INFO.field.concat(".ecs")),
        CHECKED_VPC(WorkerField.PLATFORM_INFO.field.concat(".checkedVpc")),
        STRATEGY_EXISTENCE(WorkerField.PLATFORM_INFO.field.concat(".strategyExistence")),
        DRS_REGION_NAME(WorkerField.PLATFORM_INFO.field.concat(".DRS_regionName")),
        DRS_ZONE_NAME(WorkerField.PLATFORM_INFO.field.concat(".DRS_zoneName")),
        REGION_NAME(WorkerField.PLATFORM_INFO.field.concat(".regionName")),
        ZONE_NAME(WorkerField.PLATFORM_INFO.field.concat(".zoneName")),
        AGENT_TYPE(WorkerField.PLATFORM_INFO.field.concat(".agentType")),
        IS_THROUGH(WorkerField.PLATFORM_INFO.field.concat(".isThrough")),
        BIDIRECTIONAL(WorkerField.PLATFORM_INFO.field.concat(".bidirectional"));
        final String field;

        PlatformInfo(String field) {
            this.field = field;
        }


        @Override
        public String field() {
            return field;
        }
    }
}
