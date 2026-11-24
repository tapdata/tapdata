package com.tapdata.tm.inspect.service;

import com.tapdata.tm.commons.inspect.CustomQueryConfig;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.inspect.controller.InspectController;
import com.tapdata.tm.inspect.service.InspectResultService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import java.util.concurrent.CompletableFuture;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class CustomQueryControllerRedesignTest {
    @Test void previewStartsAsyncAndReturnsItsResultOnDispatch() throws Exception {
        UserDetail user = mock(UserDetail.class); when(user.getUserId()).thenReturn("alice");
        InspectController controller = new InspectController(mock(InspectResultService.class)) {
            @Override public UserDetail getLoginUser() { return user; }
        };
        CustomQueryInspectSupport authorization = mock(CustomQueryInspectSupport.class);
        CustomQueryPreviewRpc rpc = mock(CustomQueryPreviewRpc.class);
        CompletableFuture<CustomQueryConfig.Snapshot> result = new CompletableFuture<>();
        when(rpc.testAsync(any(),eq("alice"))).thenReturn(result);
        controller.setCustomQueryInspectSupport(authorization); controller.setCustomQueryPreviewRpc(rpc);
        MockMvc mvc = MockMvcBuilders.standaloneSetup(controller).build();
        MvcResult pending = mvc.perform(post("/api/Inspects/custom-query/test")
                .contentType("application/json").content("{\"connectionId\":\"c\",\"query\":\"select 1 v\"}"))
                .andExpect(request().asyncStarted()).andReturn();
        verify(authorization).authorize(any(),same(user));
        assertFalse(result.isDone());
        result.complete(new CustomQueryConfig.Snapshot());
        mvc.perform(asyncDispatch(pending)).andExpect(status().isOk());
        assertNotNull(InspectController.class.getMethod("setCustomQueryPreviewRpc",CustomQueryPreviewRpc.class).getAnnotation(Autowired.class));
        assertNotNull(InspectController.class.getMethod("setCustomQueryInspectSupport",CustomQueryInspectSupport.class).getAnnotation(Autowired.class));
    }
}
