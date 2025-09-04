package com.tapdata.tm.worker.service;

import com.tapdata.tm.apiCalls.service.WorkerCallService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.worker.dto.ApiServerInfo;
import com.tapdata.tm.worker.dto.ApiServerWorkerInfo;
import com.tapdata.tm.worker.dto.MetricInfo;
import com.tapdata.tm.worker.entity.Worker;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/4 10:29 Create
 * @description
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class ApiWorkerServer {
    MongoTemplate mongoOperations;

    public ApiServerInfo getWorkers(String processId) {
        ApiServerInfo server = new ApiServerInfo();
        Worker serverInfo = null;
        List<ApiServerWorkerInfo> workers;
        try {
            serverInfo = getServerInfo(processId);
            workers = pullWorkerInfo(serverInfo);
        } catch (Exception e) {
            log.warn(e.getMessage());
            workers = new ArrayList<>();
        }
        server.setWorkers(workers);
        withInServer(serverInfo, server);
        return server;
    }

    public void withInServer(Worker serverInfo, ApiServerInfo server) {
        Optional.ofNullable(serverInfo).map(Worker::getHostname).ifPresent(server::setName);
        Optional.ofNullable(serverInfo).map(Worker::getProcessId).ifPresent(server::setProcessId);
        if (null != serverInfo && serverInfo.getWorker_status() instanceof Map<?,?> status) {
            server.setMetricValues(toMetricInfo(status.get("metricValues")));
            Optional.ofNullable(status.get("worker_process_id"))
                    .filter(Number.class::isInstance)
                    .map(e -> ((Number) e).intValue())
                    .ifPresent(server::setWorkerPid);
            Optional.ofNullable(status.get("status"))
                    .filter(String.class::isInstance)
                    .map(e -> (String) e)
                    .ifPresent(server::setStatus);
            Optional.ofNullable(status.get("worker_process_start_time"))
                    .filter(Number.class::isInstance)
                    .map(e -> ((Number) e).longValue())
                    .ifPresent(server::setWorkerProcessStartTime);
            Optional.ofNullable(status.get("worker_process_end_time"))
                    .filter(Number.class::isInstance)
                    .map(e -> ((Number) e).longValue())
                    .ifPresent(server::setWorkerProcessEndTime);
            Optional.ofNullable(status.get("exit_code"))
                    .ifPresent(server::setExitCode);
        }
    }


    public Worker getServerInfo(String processId) {
        if (StringUtils.isBlank(processId)) {
            throw new BizException("api.call.metric.process.id.required");
        }
        Criteria cWorker = Criteria.where("process_id").is(processId)
                .and("worker_type").is("api-server")
                .and(WorkerCallService.Tag.DELETE).ne(true);
        Query qWorker = Query.query(cWorker).limit(1);
        Worker server = mongoOperations.findOne(qWorker, Worker.class, "Workers");
        if (server == null) {
            throw new BizException("api.call.metric.server.not.found", processId);
        }
        return server;
    }

    List<ApiServerWorkerInfo> pullWorkerInfo(Worker server) {
        Object workerStatus = server.getWorker_status();
        List<ApiServerWorkerInfo> workerList = new ArrayList<>();
        if (workerStatus instanceof Map<?, ?> status && status.get("workers") instanceof Map<?, ?> workers) {
            workers.forEach((k, v) -> {
                if (v instanceof Map<?, ?> infoMap) {
                    ApiServerWorkerInfo info = new ApiServerWorkerInfo();
                    Optional.ofNullable(infoMap.get("id"))
                            .filter(Integer.class::isInstance)
                            .map(e -> (Integer) e)
                            .ifPresent(info::setId);
                    Optional.ofNullable(infoMap.get("pid"))
                            .filter(Integer.class::isInstance)
                            .map(e -> (Integer) e)
                            .ifPresent(info::setPid);
                    Optional.ofNullable(infoMap.get("oid"))
                            .filter(String.class::isInstance)
                            .map(e -> (String) e)
                            .ifPresent(info::setOid);
                    Optional.ofNullable(infoMap.get("name"))
                            .filter(String.class::isInstance)
                            .map(e -> (String) e)
                            .ifPresent(info::setName);
                    Optional.ofNullable(infoMap.get("worker_status"))
                            .filter(String.class::isInstance)
                            .map(e -> (String) e)
                            .ifPresent(info::setWorkerStatus);
                    Optional.ofNullable(infoMap.get("worker_start_time"))
                            .filter(Number.class::isInstance)
                            .map(e -> ((Number) e).longValue())
                            .ifPresent(info::setWorkerStartTime);
                    Optional.ofNullable(infoMap.get("sort"))
                            .filter(Number.class::isInstance)
                            .map(e -> ((Number) e).intValue())
                            .ifPresent(info::setSort);
                    Optional.ofNullable(infoMap.get("metricValues"))
                            .filter(Map.class::isInstance)
                            .map(e -> (Map<String, Object>) e)
                            .map(this::toMetricInfo)
                            .ifPresent(info::setMetricValues);
                    workerList.add(info);
                }

            });
        }
        return workerList;
    }

    public MetricInfo toMetricInfo(Object value) {
        MetricInfo info = new MetricInfo();
        if (value instanceof Map<?, ?> infoMap) {
            Optional.ofNullable(infoMap.get("HeapMemoryUsage"))
                    .filter(Number.class::isInstance)
                    .map(e -> ((Number) e).longValue())
                    .ifPresent(info::setHeapMemoryUsage);
            Optional.ofNullable(infoMap.get("CpuUsage"))
                    .filter(Number.class::isInstance)
                    .map(e -> ((Number) e).longValue())
                    .ifPresent(info::setCpuUsage);
            Optional.ofNullable(infoMap.get("lastUpdateTime"))
                    .filter(Number.class::isInstance)
                    .map(e -> ((Number) e).longValue())
                    .ifPresent(info::setLastUpdateTime);
        }

        return info;
    }

    public Map<String, String> workerMap(Worker server) {
        return pullWorkerInfo(server).stream().collect(Collectors.toMap(ApiServerWorkerInfo::getOid, ApiServerWorkerInfo::getName, (s1, s2) -> s2));
    }

    public List<ApiServerInfo> getApiServerWorkerInfo() {
        Criteria cWorker = Criteria.where("worker_type").is("api-server")
                .and(WorkerCallService.Tag.DELETE).ne(true);
        Query qWorker = Query.query(cWorker);
        List<Worker> server = mongoOperations.find(qWorker, Worker.class, "Workers");
        if (server.isEmpty()) {
            return new ArrayList<>();
        }
        return server.stream()
                .filter(Objects::nonNull)
                .map(e -> {
                    ApiServerInfo info = new ApiServerInfo();
                    info.setName(e.getHostname());
                    withInServer(e, info);
                    return info;
                })
                .toList();
    }
}
