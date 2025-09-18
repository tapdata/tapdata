package com.tapdata.tm.worker.service;

import com.tapdata.tm.apiCalls.service.WorkerCallServiceImpl;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.worker.dto.ApiServerInfo;
import com.tapdata.tm.worker.dto.ApiServerStatus;
import com.tapdata.tm.worker.dto.ApiServerWorkerInfo;
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
        Optional.ofNullable(serverInfo).map(Worker::getPingTime).ifPresent(server::setPingTime);
        Optional.ofNullable(serverInfo).map(Worker::getWorkerStatus)
                .ifPresent(status -> {
                    server.setMetricValues(status.getMetricValues());
                    Optional.ofNullable(status.getPid())
                            .ifPresent(server::setPid);
                    Optional.ofNullable(status.getWorkerProcessId())
                            .ifPresent(server::setWorkerPid);
                    Optional.ofNullable(status.getStatus())
                            .ifPresent(server::setStatus);
                    Optional.ofNullable(status.getWorkerProcessStartTime())
                            .ifPresent(server::setWorkerProcessStartTime);
                    Optional.ofNullable(status.getWorkerProcessEndTime())
                            .ifPresent(server::setWorkerProcessEndTime);
                    Optional.ofNullable(status.getExitCode())
                            .ifPresent(server::setExitCode);
                });
    }


    public Worker getServerInfo(String processId) {
        if (StringUtils.isBlank(processId)) {
            throw new BizException("api.call.metric.process.id.required");
        }
        Criteria cWorker = Criteria.where("process_id").is(processId)
                .and("worker_type").is("api-server")
                .and(WorkerCallServiceImpl.Tag.DELETE).ne(true);
        Query qWorker = Query.query(cWorker).limit(1);
        Worker server = mongoOperations.findOne(qWorker, Worker.class, "Workers");
        if (server == null) {
            throw new BizException("api.call.metric.server.not.found", processId);
        }
        return server;
    }

    List<ApiServerWorkerInfo> pullWorkerInfo(Worker server) {
        ApiServerStatus workerStatus = server.getWorkerStatus();
        List<ApiServerWorkerInfo> workerList = new ArrayList<>();
        Map<String, ApiServerWorkerInfo> workers = workerStatus.getWorkers();
        if (null != workers && !workers.isEmpty()) {
            workerList.addAll(workers.values());
        }
        return workerList;
    }

    public Map<String, String> workerMap(Worker server) {
        return pullWorkerInfo(server).stream()
                .collect(Collectors.toMap(ApiServerWorkerInfo::getOid, e -> Optional.ofNullable(e.getName()).orElse("-"), (s1, s2) -> s2));
    }

    public List<ApiServerInfo> getApiServerWorkerInfo() {
        Criteria cWorker = Criteria.where("worker_type").is("api-server")
                .and(WorkerCallServiceImpl.Tag.DELETE).ne(true);
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
