package com.tapdata.tm.ws.handler;

import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.userLog.service.ConnectionAuditService;
import com.tapdata.tm.ws.dto.MessageInfo;
import com.tapdata.tm.ws.dto.WebSocketContext;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class PipeHandlerTest {

    @Test
    void shouldRecordConnectionTestResultAndForwardOriginalMessage() {
        MessageQueueService queueService = mock(MessageQueueService.class);
        ConnectionAuditService auditService = mock(ConnectionAuditService.class);
        PipeHandler handler = new PipeHandler(queueService, mock(DataSourceService.class));
        handler.setConnectionAuditService(auditService);

        Map<String, Object> result = new HashMap<>();
        result.put("id", "connection-id");
        result.put("status", "ready");
        Map<String, Object> data = new HashMap<>();
        data.put("type", "testConnectionResult");
        data.put("status", "SUCCESS");
        data.put("result", result);
        MessageInfo messageInfo = new MessageInfo();
        messageInfo.setSender("engine");
        messageInfo.setReceiver("browser-session");
        messageInfo.setData(data);

        handler.handleMessage(new WebSocketContext("engine", "", messageInfo));

        verify(auditService).completeConnectionTest("connection-id", "browser-session", true);
        ArgumentCaptor<MessageQueueDto> captor = ArgumentCaptor.forClass(MessageQueueDto.class);
        verify(queueService).sendMessage(captor.capture());
        assertEquals(data, captor.getValue().getData());
    }

    @Test
    void shouldRecordFailedConnectionTestWhenEngineReturnsError() {
        ConnectionAuditService auditService = mock(ConnectionAuditService.class);
        PipeHandler handler = new PipeHandler(mock(MessageQueueService.class), mock(DataSourceService.class));
        handler.setConnectionAuditService(auditService);
        Map<String, Object> data = new HashMap<>();
        data.put("type", "testConnectionResult");
        data.put("status", "ERROR");
        MessageInfo messageInfo = new MessageInfo();
        messageInfo.setSender("engine");
        messageInfo.setReceiver("browser-session");
        messageInfo.setData(data);

        handler.handleMessage(new WebSocketContext("engine", "", messageInfo));

        verify(auditService).completeConnectionTest(null, "browser-session", false);
    }
}
