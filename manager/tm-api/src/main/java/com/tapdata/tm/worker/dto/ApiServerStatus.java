package com.tapdata.tm.worker.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/17 14:42 Create
 * @description
 */
public class ApiServerStatus {

    @JsonProperty("worker_process_id")
    @Field("worker_process_id")
    Integer workerProcessId;

    @JsonProperty("worker_process_start_time")
    @Field("worker_process_start_time")
    Long workerProcessStartTime;

    @JsonProperty("worker_process_end_time")
    @Field("worker_process_end_time")
    Long workerProcessEndTime;

    @JsonProperty("exit_code")
    @Field("exit_code")
    Integer exitCode;



    Integer pid;

    Long activeTime;

    String status;

    Map<String, ApiServerWorkerInfo> workers;

    MetricInfo metricValues;

    private Boolean updateCpuMem;

    Long auditLogPushMaxDelay;

    public void setWorkers(Object obj) {
        if (obj instanceof Map map) {
            workers = (Map<String, ApiServerWorkerInfo>) map;
        } else if (obj instanceof List list) {
            setWorkersList((List<ApiServerWorkerInfo>)list);
        } else {
            workers = new HashMap<>();
        }
    }

    public void setWorkersList(List<ApiServerWorkerInfo> workers) {
        if (workers == null) {
            this.workers = new HashMap<>();
            return;
        }
        Map<String, ApiServerWorkerInfo> newWorkers = new HashMap<>();
        for (ApiServerWorkerInfo worker : workers) {
            newWorkers.put(worker.getOid(), worker);
        }
        setWorkers(newWorkers);
    }

    public Integer getWorkerProcessId() {
        return workerProcessId;
    }

    public void setWorkerProcessId(Integer workerProcessId) {
        this.workerProcessId = workerProcessId;
    }

    public Long getWorkerProcessStartTime() {
        return workerProcessStartTime;
    }

    public void setWorkerProcessStartTime(Long workerProcessStartTime) {
        this.workerProcessStartTime = workerProcessStartTime;
    }

    public Long getWorkerProcessEndTime() {
        return workerProcessEndTime;
    }

    public void setWorkerProcessEndTime(Long workerProcessEndTime) {
        this.workerProcessEndTime = workerProcessEndTime;
    }

    public Integer getExitCode() {
        return exitCode;
    }

    public void setExitCode(Integer exitCode) {
        this.exitCode = exitCode;
    }

    public Integer getPid() {
        return pid;
    }

    public void setPid(Integer pid) {
        this.pid = pid;
    }

    public Long getActiveTime() {
        return activeTime;
    }

    public void setActiveTime(Long activeTime) {
        this.activeTime = activeTime;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Map<String, ApiServerWorkerInfo> getWorkers() {
        return workers;
    }

    public MetricInfo getMetricValues() {
        return metricValues;
    }

    public void setMetricValues(MetricInfo metricValues) {
        this.metricValues = metricValues;
    }

    public Boolean getUpdateCpuMem() {
        return updateCpuMem;
    }

    public void setUpdateCpuMem(Boolean updateCpuMem) {
        this.updateCpuMem = updateCpuMem;
    }

    public Long getAuditLogPushMaxDelay() {
        return auditLogPushMaxDelay;
    }

    public void setAuditLogPushMaxDelay(Long auditLogPushMaxDelay) {
        this.auditLogPushMaxDelay = auditLogPushMaxDelay;
    }
}
