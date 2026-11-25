package com.tapdata.tm.proxyprocess.controller;

import com.tapdata.tm.proxyprocess.dto.ProxyProcessCommandResult;
import com.tapdata.tm.proxyprocess.dto.ProxyProcessHttpRequest;
import com.tapdata.tm.proxyprocess.service.AgentProcessService;
import com.tapdata.tm.proxyprocess.service.ProxyProcessCommandService;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class ProxyProcessCommandControllerTest {
    @Test
    void commandShouldDelegateToService() {
        AgentProcessService service = mock(AgentProcessService.class);
        ProxyProcessCommandService commandService = mock(ProxyProcessCommandService.class);
        AgentProcessController controller = new AgentProcessController(service, commandService);
        ProxyProcessHttpRequest request = new ProxyProcessHttpRequest();
        ProxyProcessCommandResult result = new ProxyProcessCommandResult();
        when(commandService.execute("proxy-process-1", "start", request)).thenReturn(result);

        assertEquals(result, controller.command("proxy-process-1", "start", request).getData());
        verify(commandService).execute("proxy-process-1", "start", request);
    }
}
