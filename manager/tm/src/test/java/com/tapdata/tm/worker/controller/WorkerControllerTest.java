package com.tapdata.tm.worker.controller;

import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.accessToken.service.AccessTokenService;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.config.component.ProductComponent;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.constant.Operation;
import com.tapdata.tm.userLog.service.UserLogService;
import com.tapdata.tm.worker.dto.TcmInfo;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class WorkerControllerTest {
    WorkerController workerController;
    WorkerService workerService;
    @Nested
    class TestQueryAllBindWorker{
        @Test
        void testQueryAllBindWorkerSimple(){
            workerService = mock(WorkerService.class);
            workerController = spy(new WorkerController(workerService,mock(UserLogService.class),mock(SettingsService.class)));
            List<Worker> workers = mock(ArrayList.class);
            when(workerService.queryAllBindWorker()).thenReturn(workers);
            ResponseMessage<List<Worker>> actual = workerController.queryAllBindWorker();
            assertEquals(workers,actual.getData());
        }
    }
    @Nested
    class TestUnbindByProcessId{
        @Test
        void testUnbindByProcessId(){
            workerService = mock(WorkerService.class);
            workerController = spy(new WorkerController(workerService,mock(UserLogService.class),mock(SettingsService.class)));
            String processId ="111";
            when(workerService.unbindByProcessId(processId)).thenReturn(true);
            ResponseMessage<Boolean> actual = workerController.unbindByProcessId(processId);
            assertEquals(true,actual.getData());
        }
    }

    @Nested
    class TestUpdateByWhere{
        UserService userService = mock(UserService.class);
        AccessTokenService accessTokenService = mock(AccessTokenService.class);
        ProductComponent productComponent = mock(ProductComponent.class);
        UserLogService userLogService = mock(UserLogService.class);
       @BeforeEach
       void before(){
           workerService = mock(WorkerService.class);
           workerController = spy(new WorkerController(workerService,userLogService,mock(SettingsService.class)));
           ReflectionTestUtils.setField(workerController, "userService", userService);
           ReflectionTestUtils.setField(workerController, "accessTokenService", accessTokenService);
           ReflectionTestUtils.setField(workerController, "productComponent", productComponent);
       }
        @Test
        void testOperationStop(){
            Map filter = JsonUtil.parseJson("{\"tcm.agentId\": \"" + "test" + "\",\"$or\":[{\"updateStatus\":\"\"},{\"updateStatus\":\"done\"},{\"updateStatus\":\"error\"},{\"updateStatus\":\"fail\"},{\"updateStatus\":{\"$exists\":false}}]}", Map.class);
            String mockWhere = JsonUtil.toJson(filter);
            Map<String, Object> bodyParameters = new HashMap<>();
            bodyParameters.put("where",mockWhere);
            bodyParameters.put("stopping",true);
            bodyParameters.put("isTCM",true);
            String mockBody = JsonUtil.toJson(bodyParameters);
            Filter mockFilter = new Filter();
            mockFilter.setWhere(workerController.parseWhere(mockWhere));
            try (MockedStatic<RequestContextHolder> holderMockedStatic = Mockito.mockStatic(RequestContextHolder.class)){
                MockHttpServletRequest mockHttpServletRequest = new MockHttpServletRequest();
                mockHttpServletRequest.addHeader("user_id","test");
                ServletRequestAttributes servletRequestAttributes = new ServletRequestAttributes(mockHttpServletRequest);
                holderMockedStatic.when(RequestContextHolder::currentRequestAttributes).thenReturn(servletRequestAttributes);
                UserDetail mockUserDetail = mock(UserDetail.class);
                when(userService.loadUserByExternalId("test")).thenReturn(mockUserDetail);
                WorkerDto mockWorkerDto = new WorkerDto();
                TcmInfo mockTcmInfo = new TcmInfo();
                mockTcmInfo.setAgentName("test");
                mockTcmInfo.setAgentId("test");
                mockWorkerDto.setTcmInfo(mockTcmInfo);
                mockWorkerDto.setProcessId("test");
                when(workerService.findOne(mockFilter,mockUserDetail)).thenReturn(mockWorkerDto);
                doNothing().when(userLogService).addUserLog(Modular.AGENT,com.tapdata.tm.userLog.constant.Operation.STOP,mockUserDetail,mockTcmInfo.getAgentName());
                workerController.updateByWhere(mockWhere,mockBody);
                verify(workerService,times(1)).sendStopWorkWs(mockWorkerDto.getProcessId(), mockUserDetail);
            }
        }
        @Test
        void testOperationDelete(){
            Map filter = JsonUtil.parseJson("{\"tcm.agentId\": \"" + "test" + "\",\"$or\":[{\"updateStatus\":\"\"},{\"updateStatus\":\"done\"},{\"updateStatus\":\"error\"},{\"updateStatus\":\"fail\"},{\"updateStatus\":{\"$exists\":false}}]}", Map.class);
            String mockWhere = JsonUtil.toJson(filter);
            Map<String, Object> bodyParameters = new HashMap<>();
            bodyParameters.put("where",mockWhere);
            bodyParameters.put("isDeleted",true);
            bodyParameters.put("isTCM",true);
            String mockBody = JsonUtil.toJson(bodyParameters);
            Filter mockFilter = new Filter();
            mockFilter.setWhere(workerController.parseWhere(mockWhere));
            try (MockedStatic<RequestContextHolder> holderMockedStatic = Mockito.mockStatic(RequestContextHolder.class)){
                MockHttpServletRequest mockHttpServletRequest = new MockHttpServletRequest();
                mockHttpServletRequest.addHeader("user_id","test");
                ServletRequestAttributes servletRequestAttributes = new ServletRequestAttributes(mockHttpServletRequest);
                holderMockedStatic.when(RequestContextHolder::currentRequestAttributes).thenReturn(servletRequestAttributes);
                UserDetail mockUserDetail = mock(UserDetail.class);
                when(userService.loadUserByExternalId("test")).thenReturn(mockUserDetail);
                WorkerDto mockWorkerDto = new WorkerDto();
                TcmInfo mockTcmInfo = new TcmInfo();
                mockTcmInfo.setAgentName("test");
                mockTcmInfo.setAgentId("test");
                mockWorkerDto.setTcmInfo(mockTcmInfo);
                mockWorkerDto.setProcessId("test");
                when(workerService.findOne(mockFilter,mockUserDetail)).thenReturn(mockWorkerDto);
                doNothing().when(userLogService).addUserLog(Modular.AGENT, Operation.DELETE,mockUserDetail,mockTcmInfo.getAgentName());
                workerController.updateByWhere(mockWhere,mockBody);
                verify(workerService,times(1)).sendStopWorkWs(mockWorkerDto.getProcessId(), mockUserDetail);
            }
        }

        @Test
        void testOperationNull(){
            Map filter = JsonUtil.parseJson("{\"tcm.agentId\": \"" + "test" + "\",\"$or\":[{\"updateStatus\":\"\"},{\"updateStatus\":\"done\"},{\"updateStatus\":\"error\"},{\"updateStatus\":\"fail\"},{\"updateStatus\":{\"$exists\":false}}]}", Map.class);
            String mockWhere = JsonUtil.toJson(filter);
            Map<String, Object> bodyParameters = new HashMap<>();
            bodyParameters.put("where",mockWhere);
            bodyParameters.put("isTCM",true);
            String mockBody = JsonUtil.toJson(bodyParameters);
            Filter mockFilter = new Filter();
            mockFilter.setWhere(workerController.parseWhere(mockWhere));
            try (MockedStatic<RequestContextHolder> holderMockedStatic = Mockito.mockStatic(RequestContextHolder.class)){
                MockHttpServletRequest mockHttpServletRequest = new MockHttpServletRequest();
                mockHttpServletRequest.addHeader("user_id","test");
                ServletRequestAttributes servletRequestAttributes = new ServletRequestAttributes(mockHttpServletRequest);
                holderMockedStatic.when(RequestContextHolder::currentRequestAttributes).thenReturn(servletRequestAttributes);
                UserDetail mockUserDetail = mock(UserDetail.class);
                when(userService.loadUserByExternalId("test")).thenReturn(mockUserDetail);
                WorkerDto mockWorkerDto = new WorkerDto();
                TcmInfo mockTcmInfo = new TcmInfo();
                mockTcmInfo.setAgentName("test");
                mockTcmInfo.setAgentId("test");
                mockWorkerDto.setTcmInfo(mockTcmInfo);
                mockWorkerDto.setProcessId("test");
                when(workerService.findOne(mockFilter,mockUserDetail)).thenReturn(mockWorkerDto);
                doNothing().when(userLogService).addUserLog(Modular.AGENT, Operation.DELETE,mockUserDetail,mockTcmInfo.getAgentName());
                workerController.updateByWhere(mockWhere,mockBody);
                verify(workerService,times(0)).sendStopWorkWs(mockWorkerDto.getProcessId(), mockUserDetail);
            }
        }

        @Test
        void testOperationNoTCM(){
            Map filter = JsonUtil.parseJson("{\"tcm.agentId\": \"" + "test" + "\",\"$or\":[{\"updateStatus\":\"\"},{\"updateStatus\":\"done\"},{\"updateStatus\":\"error\"},{\"updateStatus\":\"fail\"},{\"updateStatus\":{\"$exists\":false}}]}", Map.class);
            String mockWhere = JsonUtil.toJson(filter);
            Map<String, Object> bodyParameters = new HashMap<>();
            bodyParameters.put("where",mockWhere);
            String mockBody = JsonUtil.toJson(bodyParameters);
            Filter mockFilter = new Filter();
            mockFilter.setWhere(workerController.parseWhere(mockWhere));
            try (MockedStatic<RequestContextHolder> holderMockedStatic = Mockito.mockStatic(RequestContextHolder.class)){
                MockHttpServletRequest mockHttpServletRequest = new MockHttpServletRequest();
                mockHttpServletRequest.addHeader("user_id","test");
                ServletRequestAttributes servletRequestAttributes = new ServletRequestAttributes(mockHttpServletRequest);
                holderMockedStatic.when(RequestContextHolder::currentRequestAttributes).thenReturn(servletRequestAttributes);
                UserDetail mockUserDetail = mock(UserDetail.class);
                when(userService.loadUserByExternalId("test")).thenReturn(mockUserDetail);
                WorkerDto mockWorkerDto = new WorkerDto();
                TcmInfo mockTcmInfo = new TcmInfo();
                mockTcmInfo.setAgentName("test");
                mockTcmInfo.setAgentId("test");
                mockWorkerDto.setTcmInfo(mockTcmInfo);
                mockWorkerDto.setProcessId("test");
                when(workerService.findOne(mockFilter,mockUserDetail)).thenReturn(mockWorkerDto);
                doNothing().when(userLogService).addUserLog(Modular.AGENT, Operation.DELETE,mockUserDetail,mockTcmInfo.getAgentName());
                workerController.updateByWhere(mockWhere,mockBody);
                verify(workerService,times(0)).sendStopWorkWs(mockWorkerDto.getProcessId(), mockUserDetail);
            }
        }


    }
}
