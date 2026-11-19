package com.tapdata.tm.proxyprocess;

import com.tapdata.tm.proxyprocess.constant.AgentProcessStatus;
import com.tapdata.tm.proxyprocess.dto.AgentProcessDto;
import com.tapdata.tm.proxyprocess.entity.AgentProcessEntity;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.mapping.Document;

import java.lang.reflect.Field;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class AgentProcessModelTest {

    @Test
    void agentProcessEntityMapsCollectionAndRuntimeFields() throws NoSuchFieldException {
        AgentProcessEntity entity = new AgentProcessEntity();
        Date startTime = new Date();
        Date statusReportTime = new Date(startTime.getTime() + 1_000);
        Date lastRegisterTime = new Date(statusReportTime.getTime() + 1_000);

        entity.setAgentName("proxy-host-1");
        entity.setAgentType("Db2 for z cdc reader");
        entity.setAgentId("agent-1");
        entity.setAdvertiseIp("10.0.0.10");
        entity.setAdvertisePort(8080);
        entity.setStartTime(startTime.getTime());
        entity.setStatus(AgentProcessStatus.RUNNING);
        entity.setStatusReportTime(statusReportTime.getTime());
        entity.setPid(12345L);
        entity.setCpuUsage(12.5D);
        entity.setMemoryUsage(35.2D);
        entity.setErrorReason(null);
        entity.setToken("internal-token");
        entity.setLastRegisterTime(lastRegisterTime.getTime());

        assertEquals("AgentProcess", AgentProcessEntity.class.getAnnotation(Document.class).value());
        assertEquals("proxy-host-1", entity.getAgentName());
        assertEquals("Db2 for z cdc reader", entity.getAgentType());
        assertEquals("agent-1", entity.getAgentId());
        assertEquals("10.0.0.10", entity.getAdvertiseIp());
        assertEquals(8080, entity.getAdvertisePort());
        assertEquals(startTime.getTime(), entity.getStartTime());
        assertEquals(AgentProcessStatus.RUNNING, entity.getStatus());
        assertEquals(statusReportTime.getTime(), entity.getStatusReportTime());
        assertEquals(12345L, entity.getPid());
        assertEquals(12.5D, entity.getCpuUsage());
        assertEquals(35.2D, entity.getMemoryUsage());
        assertEquals("internal-token", entity.getToken());
        assertEquals(lastRegisterTime.getTime(), entity.getLastRegisterTime());
        assertEquals(Long.class, AgentProcessEntity.class.getDeclaredField("startTime").getType());
        assertEquals(Long.class, AgentProcessEntity.class.getDeclaredField("statusReportTime").getType());
        assertEquals(Long.class, AgentProcessEntity.class.getDeclaredField("lastRegisterTime").getType());
    }

    @Test
    void frontEndDtoDoesNotExposeToken() {
        boolean hasToken = false;
        for (Field field : AgentProcessDto.class.getDeclaredFields()) {
            if ("token".equals(field.getName())) {
                hasToken = true;
                break;
            }
        }

        assertFalse(hasToken);
    }

    @Test
    void statusConstantsUseStableWireValues() {
        assertEquals("RUNNING", AgentProcessStatus.RUNNING);
        assertEquals("OFFLINE", AgentProcessStatus.OFFLINE);
        assertEquals("FAILED", AgentProcessStatus.FAILED);
    }
}
