package com.tapdata.tm.cluster.util;

import com.tapdata.tm.cluster.dto.ClusterStateDto;
import org.junit.jupiter.api.Test;

import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
}
