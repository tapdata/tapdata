package com.tapdata.tm.proxyprocess.controller;

import com.tapdata.tm.proxyprocess.dto.AgentProcessRegisterDto;
import com.tapdata.tm.proxyprocess.dto.AgentProcessRegisterResponseDto;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.proxyprocess.dto.AgentProcessDto;
import com.tapdata.tm.proxyprocess.dto.AgentProcessQueryDto;
import com.tapdata.tm.proxyprocess.service.AgentProcessService;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class AgentProcessControllerTest {
    @Test
    void registerShouldDelegateToService() {
        AgentProcessService service = mock(AgentProcessService.class);
        AgentProcessController controller = new AgentProcessController(service);
        AgentProcessRegisterDto request = new AgentProcessRegisterDto();
        AgentProcessRegisterResponseDto response = new AgentProcessRegisterResponseDto();
        when(service.register(request)).thenReturn(response);

        assertEquals(response, controller.register(request).getData());
        verify(service).register(request);
    }

    @Test
    void listAndDeleteShouldDelegateToService() {
        AgentProcessService service = mock(AgentProcessService.class);
        AgentProcessController controller = new AgentProcessController(service);
        AgentProcessQueryDto query = new AgentProcessQueryDto();
        Page<AgentProcessDto> page = Page.empty();
        when(service.list(query)).thenReturn(page);

        assertEquals(page, controller.list(query).getData());
        controller.delete("507f1f77bcf86cd799439011");

        verify(service).list(query);
        verify(service).delete("507f1f77bcf86cd799439011");
    }
}
