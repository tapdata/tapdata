package com.tapdata.tm.cluster.util;

import com.tapdata.tm.cluster.dto.ClusterStateDto;
import com.tapdata.tm.cluster.dto.Component;
import com.tapdata.tm.worker.dto.ApiServerStatus;
import com.tapdata.tm.worker.entity.Worker;

import java.util.List;
import java.util.function.Consumer;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/5/8 19:04 Create
 * @description
 */
public final class ApiStatusUtil {
    private ApiStatusUtil() {

    }

    public static Boolean clusterStopped(ClusterStateDto workerClusterStatus, boolean workerAlive) {
        if (workerAlive && "stopped".equals(workerClusterStatus.getStatus())) {
            workerClusterStatus.setStatus("running");
        }
       return "stopped".equals(workerClusterStatus.getStatus()) && !workerAlive;
    }

    public static void statusOfApi(boolean clusterStopped, Worker apiInfo, Component workerClusterStatus, Consumer<String> apiStatusFromAgent, Consumer<String> apiStatusFromApiServer) {
        if (null == apiInfo || null == apiInfo.getWorkerStatus()) {
            apiStatusFromApiServer.accept("stopped");
        } else {
            ApiServerStatus workerStatus = apiInfo.getWorkerStatus();
            String status = workerStatus.getStatus();
            Long activeTime = workerStatus.getActiveTime();
            if (null != activeTime && activeTime >= System.currentTimeMillis() - 15_000L) {
                apiStatusFromApiServer.accept(status);
            } else {
                apiStatusFromApiServer.accept("stopped");
            }
        }
        if (null != workerClusterStatus) {
            apiStatusFromAgent.accept(clusterStopped ? "stopped" : workerClusterStatus.getStatus());
        }
    }
}
