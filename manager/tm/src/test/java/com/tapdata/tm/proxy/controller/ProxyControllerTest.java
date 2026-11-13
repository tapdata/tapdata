package com.tapdata.tm.proxy.controller;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.proxyprocess.dto.AgentProcessDto;
import com.tapdata.tm.proxyprocess.dto.AgentProcessQueryDto;
import com.tapdata.tm.proxyprocess.service.AgentProcessService;
import com.tapdata.tm.proxy.service.impl.RemoteCaller;
import com.tapdata.tm.proxy.utils.RemoteCallerUtil;
import io.tapdata.modules.api.net.data.FileMeta;
import io.tapdata.pdk.apis.entity.message.CommandInfo;
import org.apache.http.entity.ContentType;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.springframework.http.HttpHeaders;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.util.ReflectionTestUtils;

import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/12/10 11:43
 */
public class ProxyControllerTest {

    @Test
    void testResponseForFileMeta() throws IOException {
        byte[] data = new byte[1024];
        FileMeta fileMeta = FileMeta.builder().transferFile(true)
                .filename("test.log.zip")
                .fileSize(1024L)
                .fileInputStream(new ByteArrayInputStream(data))
                .code("ok").build();


        HttpServletResponse response = mock(HttpServletResponse.class);
        AtomicInteger counter = new AtomicInteger(0);
        when(response.getOutputStream()).thenReturn(new ServletOutputStream() {
            @Override
            public boolean isReady() {
                return false;
            }

            @Override
            public void setWriteListener(WriteListener listener) {

            }

            @Override
            public void write(int b) throws IOException {

            }

            @Override
            public void write(@NotNull byte[] b, int off, int len) throws IOException {
                counter.incrementAndGet();
            }
        });
        RemoteCallerUtil.responseForFileMeta(fileMeta, response, mock(Logger.class));

        Assertions.assertEquals(1, counter.get());
        verify(response, times(1)).setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_OCTET_STREAM.getMimeType());
        verify(response, times(1)).setHeader(HttpHeaders.CONTENT_LENGTH, "1024");

    }

    @Test
    void testDownloadFile() {
        ProxyController proxyController = new ProxyController();

        ProxyController spyProxyController = spy(proxyController);
        doNothing().when(spyProxyController).call(any(), any(), any());
        spyProxyController.downloadFile("test.log", "agentId", null, null);
        verify(spyProxyController, times(1)).call(any(), any(), any());
    }

    @Test
    void shouldHandleAgentProcessListLocally() {
        AgentProcessService agentProcessService = mock(AgentProcessService.class);
        RemoteCaller remoteCaller = mock(RemoteCaller.class);
        Page<AgentProcessDto> expected = new Page<>(1, List.of(new AgentProcessDto()));
        when(agentProcessService.list(any(AgentProcessQueryDto.class))).thenReturn(expected);

        ProxyController controller = controller(agentProcessService, remoteCaller);
        CommandInfo commandInfo = new CommandInfo();
        commandInfo.setType("agent-process");
        commandInfo.setCommand("list");
        commandInfo.setArgMap(new HashMap<>(Map.of(
                "agentType", "Db2 for z cdc reader",
                "status", "RUNNING",
                "proxyProcessId", "507f1f77bcf86cd799439011",
                "page", 2,
                "size", 1000
        )));

        ResponseMessage<?> response = controller.command(commandInfo, new MockHttpServletRequest(),
                new MockHttpServletResponse());

        assertSame(expected, response.getData());
        ArgumentCaptor<AgentProcessQueryDto> queryCaptor = ArgumentCaptor.forClass(AgentProcessQueryDto.class);
        verify(agentProcessService).list(queryCaptor.capture());
        assertEquals("Db2 for z cdc reader", queryCaptor.getValue().getAgentType());
        assertEquals("RUNNING", queryCaptor.getValue().getStatus());
        assertEquals("507f1f77bcf86cd799439011", queryCaptor.getValue().getProxyProcessId());
        assertEquals(2, queryCaptor.getValue().getPage());
        assertEquals(1000, queryCaptor.getValue().getPageSize());
        verifyNoInteractions(remoteCaller);
    }

    @Test
    void shouldKeepNonListAgentProcessCommandOnRemoteCaller() {
        AgentProcessService agentProcessService = mock(AgentProcessService.class);
        RemoteCaller remoteCaller = mock(RemoteCaller.class);
        ProxyController controller = controller(agentProcessService, remoteCaller);
        CommandInfo commandInfo = command("agent-process", "start");
        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();

        controller.command(commandInfo, request, response);

        verify(remoteCaller).executeEngineMessage(commandInfo, request, response);
        verifyNoInteractions(agentProcessService);
    }

    @Test
    void shouldKeepConnectionCommandOnRemoteCaller() {
        AgentProcessService agentProcessService = mock(AgentProcessService.class);
        RemoteCaller remoteCaller = mock(RemoteCaller.class);
        ProxyController controller = controller(agentProcessService, remoteCaller);
        CommandInfo commandInfo = command("connection", "DescribeUserProjects");
        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();

        controller.command(commandInfo, request, response);

        verify(remoteCaller).executeEngineMessage(commandInfo, request, response);
        verifyNoInteractions(agentProcessService);
    }

    private ProxyController controller(AgentProcessService agentProcessService, RemoteCaller remoteCaller) {
        ProxyController controller = spy(new ProxyController());
        ReflectionTestUtils.setField(controller, "agentProcessService", agentProcessService);
        ReflectionTestUtils.setField(controller, "remoteCaller", remoteCaller);
        doReturn(mock(UserDetail.class)).when(controller).getLoginUser();
        return controller;
    }

    private CommandInfo command(String type, String command) {
        CommandInfo commandInfo = new CommandInfo();
        commandInfo.setType(type);
        commandInfo.setCommand(command);
        return commandInfo;
    }

}
