package com.tapdata.tm.cluster.util;

import com.tapdata.tm.Settings.constant.SettingsEnum;
import com.tapdata.tm.cluster.dto.ClusterStateDto;
import com.tapdata.tm.cluster.dto.Component;
import com.tapdata.tm.worker.entity.Worker;

import java.util.Date;
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
        // 双信号判定：Worker.ping_time（REST /health 写入）与 ClusterState.ttl（WebSocket
        // statusInfo 异步执行器写入）来自不同写路径。MongoDB primary 切换瞬时阻塞通常只
        // 影响其中一条链路，要求两者同时过期才能判 stopped，避免健康节点误报。
        // 同时 ClusterState.status 仅作为持久化字段由 ClusterSchedule.stopCluster() 异步
        // 翻转，不再阻塞 UI 反应。
        if (workerAlive && "stopped".equals(workerClusterStatus.getStatus())) {
            workerClusterStatus.setStatus("running");
        }
        if (workerAlive) return false;
        Date ttl = workerClusterStatus.getTtl();
        return ttl == null || ttl.before(new Date());
    }

    public static void statusOfApi(boolean clusterStopped, Worker apiInfo, Component workerClusterStatus, Consumer<String> apiStatusFromAgent, Consumer<String> apiStatusFromApiServer) {
        // status 与 serviceStatus 同源对齐：apiserver 自身 REST 心跳（3s/次）通常远早于 agent 的
        // WebSocket statusInfo 慢通道把 ClusterState.apiServer.status 写库。启动期若仅复读 DB 陈值，
        // 会出现 serviceStatus=running 而 status=stopped 持续数十秒的分裂现象。当 apiserver 自报心跳
        // 仍新鲜且 agent 未挂时，把 status 一并提升为活信号；其余情况保留 DB 值，交由 statusInfo /
        // stopCluster 的既有路径自然收敛。
        boolean apiAlive = !clusterStopped && isApiWorkerFresh(apiInfo);
        String liveApiStatus = apiAlive ? apiInfo.getWorkerStatus().getStatus() : null;

        if (null != workerClusterStatus) {
            apiStatusFromAgent.accept(apiAlive ? liveApiStatus : workerClusterStatus.getStatus());
        }
        if (clusterStopped) {
            apiStatusFromApiServer.accept("stopped");
            return;
        }
        apiStatusFromApiServer.accept(apiAlive ? liveApiStatus : "stopped");
    }

    private static boolean isApiWorkerFresh(Worker apiInfo) {
        if (null == apiInfo || null == apiInfo.getWorkerStatus() || null == apiInfo.getWorkerStatus().getMetricValues()) {
            return false;
        }
        Object activeTime = apiInfo.getWorkerStatus().getMetricValues().getLastUpdateTime();
        long threshold = System.currentTimeMillis() - (SettingsEnum.WORKER_HEART_OVERTIME.getIntValue(30) * 1_000L);
        // MetricInfo.lastUpdateTime 是 Object，BSON 编码后可能落地为 Number / Date / String 三种。
        // 对齐 MetricInfo.toUsage() 的三态处理（同文件里已存在的样板），否则 Date 类型会让
        // instanceof Number 静默失败，serviceStatus 永远卡 stopped。
        Long activeMillis = toEpochMillis(activeTime);
        return activeMillis != null && activeMillis >= threshold;
    }

    private static Long toEpochMillis(Object value) {
        if (value instanceof Number n) return n.longValue();
        if (value instanceof Date d) return d.getTime();
        if (value instanceof String s) {
            try {
                return Long.parseLong(s);
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        return null;
    }
}
