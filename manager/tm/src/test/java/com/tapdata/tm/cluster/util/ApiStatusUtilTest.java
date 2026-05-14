package com.tapdata.tm.cluster.util;

import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.constant.SettingUtil;
import com.tapdata.tm.cluster.dto.ClusterStateDto;
import com.tapdata.tm.cluster.dto.Component;
import com.tapdata.tm.worker.dto.ApiServerStatus;
import com.tapdata.tm.worker.dto.MetricInfo;
import com.tapdata.tm.worker.entity.Worker;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ApiStatusUtilTest {

    private ClusterStateDto dto(String status, Date ttl) {
        ClusterStateDto d = new ClusterStateDto();
        d.setStatus(status);
        d.setTtl(ttl);
        return d;
    }

    private static Date ttlInFuture() {
        return new Date(System.currentTimeMillis() + 30_000L);
    }

    private static Date ttlInPast() {
        return new Date(System.currentTimeMillis() - 1_000L);
    }

    @Test
    void clusterStopped_workerDeadAndTtlExpired_returnsTrue() {
        // 死机：两条心跳通道都过期 → 报 stopped
        assertTrue(ApiStatusUtil.clusterStopped(dto("running", ttlInPast()), false));
    }

    @Test
    void clusterStopped_workerDeadAndTtlNull_returnsTrue() {
        // 兼容历史数据无 ttl 的情况
        assertTrue(ApiStatusUtil.clusterStopped(dto("running", null), false));
    }

    @Test
    void clusterStopped_workerDeadButTtlFresh_returnsFalse() {
        // 关键回归点：MongoDB 切换导致 ping_time 写入瞬时阻塞，但 statusInfo 仍刷新了 ttl
        // 不应误报健康节点为 stopped
        assertFalse(ApiStatusUtil.clusterStopped(dto("running", ttlInFuture()), false));
    }

    @Test
    void clusterStopped_workerAliveAndStatusRunning_returnsFalse() {
        ClusterStateDto d = dto("running", ttlInFuture());
        assertFalse(ApiStatusUtil.clusterStopped(d, true));
        assertEquals("running", d.getStatus());
    }

    @Test
    void clusterStopped_workerAliveAndStatusStopped_recoversToRunning() {
        // 历史 status=stopped 残留：worker 仍存活时强制翻转回 running
        ClusterStateDto d = dto("stopped", ttlInFuture());
        assertFalse(ApiStatusUtil.clusterStopped(d, true));
        assertEquals("running", d.getStatus());
    }

    @Test
    void clusterStopped_workerAliveOverridesExpiredTtl() {
        // ping_time 是权威信号；只要 worker 存活，ttl 过期也不算 stopped（避免被异步 stopCluster 滞后影响）
        assertFalse(ApiStatusUtil.clusterStopped(dto("running", ttlInPast()), true));
    }

    private static Worker apiWorker(String workerStatus, Object lastUpdateTime) {
        Worker worker = new Worker();
        ApiServerStatus status = new ApiServerStatus();
        status.setStatus(workerStatus);
        MetricInfo metric = new MetricInfo();
        metric.setLastUpdateTime(lastUpdateTime);
        status.setMetricValues(metric);
        worker.setWorkerStatus(status);
        return worker;
    }

    private static Component apiComponent(String persistedStatus) {
        Component c = new Component();
        c.setStatus(persistedStatus);
        return c;
    }

    private static MockedStatic<SettingUtil> stubHeartOvertime(int seconds) {
        MockedStatic<SettingUtil> mocked = Mockito.mockStatic(SettingUtil.class);
        mocked.when(() -> SettingUtil.getValue(CategoryEnum.WORKER.getValue(), KeyEnum.WORKER_HEART_TIMEOUT.getValue()))
                .thenReturn(String.valueOf(seconds));
        return mocked;
    }

    @Test
    void statusOfApi_apiWorkerFresh_promotesStatusToRunning() {
        // 启动期典型场景：DB 持久 status 仍为 stopped（agent 慢通道未上报），
        // 但 apiserver 自身 REST 心跳已新鲜 → status / serviceStatus 都应翻 running
        try (MockedStatic<SettingUtil> ignored = stubHeartOvertime(30)) {
            Worker apiInfo = apiWorker("running", System.currentTimeMillis());
            Component api = apiComponent("stopped");
            AtomicReference<String> setStatus = new AtomicReference<>();
            AtomicReference<String> setServiceStatus = new AtomicReference<>();

            ApiStatusUtil.statusOfApi(false, apiInfo, api, setStatus::set, setServiceStatus::set);

            assertEquals("running", setStatus.get());
            assertEquals("running", setServiceStatus.get());
        }
    }

    @Test
    void statusOfApi_apiWorkerStale_keepsPersistedStatus() {
        // apiserver 自身超过 30s 未心跳：status 沿用 DB 陈值，serviceStatus 报 stopped
        try (MockedStatic<SettingUtil> ignored = stubHeartOvertime(30)) {
            long stale = System.currentTimeMillis() - 60_000L;
            Worker apiInfo = apiWorker("running", stale);
            Component api = apiComponent("running");
            AtomicReference<String> setStatus = new AtomicReference<>();
            AtomicReference<String> setServiceStatus = new AtomicReference<>();

            ApiStatusUtil.statusOfApi(false, apiInfo, api, setStatus::set, setServiceStatus::set);

            assertEquals("running", setStatus.get());
            assertEquals("stopped", setServiceStatus.get());
        }
    }

    @Test
    void statusOfApi_clusterStopped_doesNotPromoteStatus() {
        // agent 已挂：status 沿用 DB（让 stopCluster 调度器接管），serviceStatus 强制 stopped
        Worker apiInfo = apiWorker("running", System.currentTimeMillis());
        Component api = apiComponent("running");
        AtomicReference<String> setStatus = new AtomicReference<>();
        AtomicReference<String> setServiceStatus = new AtomicReference<>();

        ApiStatusUtil.statusOfApi(true, apiInfo, api, setStatus::set, setServiceStatus::set);

        assertEquals("running", setStatus.get());
        assertEquals("stopped", setServiceStatus.get());
    }

    @Test
    void statusOfApi_apiInfoNull_keepsPersistedStatus() {
        // Worker 实体缺失（极端启动 / 集群异常）：保留 DB status，serviceStatus 报 stopped
        Component api = apiComponent("stopped");
        AtomicReference<String> setStatus = new AtomicReference<>();
        AtomicReference<String> setServiceStatus = new AtomicReference<>();

        ApiStatusUtil.statusOfApi(false, null, api, setStatus::set, setServiceStatus::set);

        assertEquals("stopped", setStatus.get());
        assertEquals("stopped", setServiceStatus.get());
    }

    @Test
    void statusOfApi_workerClusterStatusNull_skipsStatusCallback() {
        // ApiMetricsChartQuery 第 328 行调用：workerClusterStatus 为 null，setStatus 不应被调用
        try (MockedStatic<SettingUtil> ignored = stubHeartOvertime(30)) {
            Worker apiInfo = apiWorker("running", System.currentTimeMillis());
            AtomicReference<String> setStatus = new AtomicReference<>();
            AtomicReference<String> setServiceStatus = new AtomicReference<>();

            ApiStatusUtil.statusOfApi(false, apiInfo, null, setStatus::set, setServiceStatus::set);

            assertNull(setStatus.get());
            assertEquals("running", setServiceStatus.get());
        }
    }
}
