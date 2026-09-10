package com.tapdata.tm.task.controller;

import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dql.service.DqlTaskImpactService;
import com.tapdata.tm.dql.vo.DqlTaskImpactRequestVo;
import com.tapdata.tm.dql.vo.DqlTaskImpactVo;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TaskControllerDqlImpactTest {

    @Test
    void delegatesDqlImpactCheckWithCurrentUser() {
        TaskController controller = spy(new TaskController());
        DqlTaskImpactService impactService = mock(DqlTaskImpactService.class);
        UserDetail userDetail = mock(UserDetail.class);
        DqlTaskImpactRequestVo request = new DqlTaskImpactRequestVo();
        request.setTaskIds(List.of("64f000000000000000000001"));
        List<DqlTaskImpactVo> expected = List.of(new DqlTaskImpactVo("64f000000000000000000001", true, 2L));

        controller.setDqlTaskImpactService(impactService);
        doReturn(userDetail).when(controller).getLoginUser();
        when(impactService.check(request, userDetail)).thenReturn(expected);

        ResponseMessage<List<DqlTaskImpactVo>> response = controller.dqlEventImpact(request);

        assertEquals(expected, response.getData());
        verify(impactService).check(eq(request), eq(userDetail));
    }
}
