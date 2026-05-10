package com.tapdata.tm.cluster.util;

import com.tapdata.tm.cluster.dto.ClusterStateDto;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ApiStatusUtilTest {

    private ClusterStateDto withStatus(String status) {
        ClusterStateDto dto = new ClusterStateDto();
        dto.setStatus(status);
        return dto;
    }

    @Test
    void clusterStopped_workerDeadAndStatusRunning_returnsTrue() {
        // 关键回归点：Worker 失活后立即返回 true，不再等待 ClusterState.status 翻转
        ClusterStateDto dto = withStatus("running");
        assertTrue(ApiStatusUtil.clusterStopped(dto, false));
    }

    @Test
    void clusterStopped_workerDeadAndStatusStopped_returnsTrue() {
        ClusterStateDto dto = withStatus("stopped");
        assertTrue(ApiStatusUtil.clusterStopped(dto, false));
    }

    @Test
    void clusterStopped_workerAliveAndStatusRunning_returnsFalse() {
        ClusterStateDto dto = withStatus("running");
        assertFalse(ApiStatusUtil.clusterStopped(dto, true));
        assertEquals("running", dto.getStatus());
    }

    @Test
    void clusterStopped_workerAliveAndStatusStopped_recoversToRunningAndReturnsFalse() {
        ClusterStateDto dto = withStatus("stopped");
        assertFalse(ApiStatusUtil.clusterStopped(dto, true));
        assertEquals("running", dto.getStatus(), "alive worker should flip stale stopped status back to running");
    }
}
