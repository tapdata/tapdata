package com.tapdata.tm.proxyprocess.controller;

import com.tapdata.tm.proxyprocess.service.ProxyProcessCommandService;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.List;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.base.security.LoginUserResolver;
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
        ProxyProcessCommandService commandService = mock(ProxyProcessCommandService.class);
        AgentProcessController controller = new AgentProcessController(service, commandService);
        AgentProcessRegisterDto request = new AgentProcessRegisterDto();
        AgentProcessRegisterResponseDto response = new AgentProcessRegisterResponseDto();
        UserDetail user = new UserDetail("6062c8fd764d49c7d626f7e3",
                "6062c8fd764d49c7d626f7e3", "reader-caller", "", List.of());
        when(service.register(request, user)).thenReturn(response);

        withLoginUser(controller, user);
        assertEquals(response, controller.register(request).getData());
        verify(service).register(request, user);
    }

    @Test
    void listAndDeleteShouldDelegateToService() {
        AgentProcessService service = mock(AgentProcessService.class);
        ProxyProcessCommandService commandService = mock(ProxyProcessCommandService.class);
        AgentProcessController controller = new AgentProcessController(service, commandService);
        AgentProcessQueryDto query = new AgentProcessQueryDto();
        Page<AgentProcessDto> page = Page.empty();
        when(service.list(query)).thenReturn(page);

        assertEquals(page, controller.list(query).getData());
        controller.delete("507f1f77bcf86cd799439011");

        verify(service).list(query);
        verify(service).delete("507f1f77bcf86cd799439011");
    }

    /*
     * The endpoints no longer trust a hand-passed header: they ask the base
     * controller for the caller TM's login resolver authenticated.
     */
    private static void withLoginUser(AgentProcessController controller, UserDetail user) {
        jakarta.servlet.http.HttpServletRequest request =
                mock(jakarta.servlet.http.HttpServletRequest.class);
        when(request.getAttribute(LoginUserResolver.LOGIN_USER_ATTRIBUTE)).thenReturn(user);
        org.springframework.web.context.request.RequestContextHolder.setRequestAttributes(
                new org.springframework.web.context.request.ServletRequestAttributes(request));
        LoginUserResolver resolver = mock(LoginUserResolver.class);
        when(resolver.resolve(any(), any())).thenReturn(user);
        try {
            java.lang.reflect.Field field =
                    com.tapdata.tm.base.controller.BaseController.class
                            .getDeclaredField("loginUserResolver");
            field.setAccessible(true);
            field.set(controller, resolver);
        } catch (ReflectiveOperationException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    @AfterEach
    void clearRequestContext() {
        org.springframework.web.context.request.RequestContextHolder.resetRequestAttributes();
    }
}
