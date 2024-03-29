package com.tapdata.tm.externalStorage;

import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.externalStorage.ExternalStorageType;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.dto.DataSourceTypeDto;
import com.tapdata.tm.ds.repository.DataSourceDefinitionRepository;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.externalStorage.service.ExternalStorageService;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.SpringContextHelper;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.ws.dto.MessageInfo;
import com.tapdata.tm.ws.dto.WebSocketContext;
import com.tapdata.tm.ws.enums.MessageType;
import com.tapdata.tm.ws.handler.TestConnectionHandler;
import com.tapdata.tm.ws.handler.TestExternalStorageHandler;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class TestExternalStorageHandlerTest {
    static AgentGroupService agentGroupService;
    static MessageQueueService messageQueueService;
    static DataSourceService dataSourceService;
    static UserService userService;
    static WorkerService workerService;
    static DataSourceDefinitionService dataSourceDefinitionService;

    static  UserDetail userDetail;


    @BeforeAll
     static void init() {
        messageQueueService = Mockito.mock(MessageQueueService.class);
        dataSourceService = Mockito.mock(DataSourceService.class);
        userService = Mockito.mock(UserService.class);
        workerService = Mockito.mock(WorkerService.class);
        dataSourceDefinitionService = Mockito.mock(DataSourceDefinitionService.class);
        userDetail = Mockito.mock(UserDetail.class);
        agentGroupService = Mockito.mock(AgentGroupService.class);

    }


    @Test
     void wrapMessageInfoMongodbTest() {

        TestExternalStorageHandler testExternalStorageHandler = new TestExternalStorageHandler(messageQueueService,
                dataSourceService, userService, workerService, dataSourceDefinitionService);

        ExternalStorageService externalStorageService = Mockito.mock(ExternalStorageService.class);
        Map<String, Object> externalStorageConfig = new HashMap<>();
        externalStorageConfig.put("id", "test1234");
        try (MockedStatic<SpringContextHelper> springContextHelperMockedStatic = mockStatic(SpringContextHelper.class)) {
            springContextHelperMockedStatic.when(() -> SpringContextHelper.getBean(eq(ExternalStorageService.class))).thenReturn(externalStorageService);
            ExternalStorageDto externalStorageDto = new ExternalStorageDto();
            externalStorageDto.setType(ExternalStorageType.mongodb.name());
            when(externalStorageService.findNotCheckById("test1234")).thenReturn(externalStorageDto);
            List<DataSourceTypeDto> list = new ArrayList<>();
            DataSourceTypeDto dataSourceTypeDto = new DataSourceTypeDto();
            dataSourceTypeDto.setPdkType("mongodb");
            dataSourceTypeDto.setPdkHash("1223456");
            dataSourceTypeDto.setPdkId("Mongodb");
            list.add(dataSourceTypeDto);
            Filter filter = new Filter(Where
                    .where("type", "MongoDB")
                    .and("tag", "All")
                    .and("authentication", "All")
            );
            when(dataSourceDefinitionService.dataSourceTypesV2(userDetail, filter)).thenReturn(list);
            MessageInfo messageInfo = testExternalStorageHandler.wrapMessageInfo(userDetail, externalStorageConfig, MessageType.TEST_EXTERNAL_STORAGE);
            assertEquals("Mongodb", messageInfo.getData().get("database_type"));
        }

    }

    @Test
     void wrapMessageInfoRocksDbTest() {
        TestExternalStorageHandler testExternalStorageHandler = new TestExternalStorageHandler(messageQueueService,
                dataSourceService, userService, workerService, dataSourceDefinitionService);

        ExternalStorageService externalStorageService = Mockito.mock(ExternalStorageService.class);
        Map<String, Object> externalStorageConfig = new HashMap<>();
        externalStorageConfig.put("id", "test1234");
        try (MockedStatic<SpringContextHelper> springContextHelperMockedStatic = mockStatic(SpringContextHelper.class)) {
            springContextHelperMockedStatic.when(() -> SpringContextHelper.getBean(eq(ExternalStorageService.class))).thenReturn(externalStorageService);
            ExternalStorageDto externalStorageDto = new ExternalStorageDto();
            externalStorageDto.setType(ExternalStorageType.rocksdb.name());
            when(externalStorageService.findNotCheckById("test1234")).thenReturn(externalStorageDto);
            when(dataSourceDefinitionService.dataSourceTypesV2(userDetail, new Filter())).thenReturn(new ArrayList<>());
            MessageInfo messageInfo = testExternalStorageHandler.wrapMessageInfo(userDetail, externalStorageConfig, MessageType.TEST_EXTERNAL_STORAGE);
            assertEquals("rocksdb", messageInfo.getData().get("testType"));
        }

    }

    @Test
     void handleMessageNoPdkHashTest() throws Exception {
        DataSourceDefinitionRepository repository = Mockito.mock(DataSourceDefinitionRepository.class);
        MockDataSourceDefinitionService dataSourceDefinitionService = new MockDataSourceDefinitionService(repository);
        TestConnectionHandler testConnectionHandler = new TestConnectionHandler(agentGroupService, messageQueueService,
                dataSourceService, userService, workerService, dataSourceDefinitionService);
        when(userService.loadUserById(null)).thenReturn(userDetail);
        MessageInfo messageInfo = new MessageInfo();
        Map data = new HashMap();
        data.put("config", new HashMap());
        messageInfo.setData(data);
        WebSocketContext context = new WebSocketContext("test", "test", messageInfo);
        testConnectionHandler.handleMessage(context);
        boolean actualData = dataSourceDefinitionService.isFlag();
        assertFalse(actualData);

    }


    @Test
     void handleMessagePdkHashTest() throws Exception {
        MessageQueueService messageQueueService = Mockito.mock(MessageQueueService.class);
        DataSourceService dataSourceService = Mockito.mock(DataSourceService.class);
        UserService userService = Mockito.mock(UserService.class);
        WorkerService workerService = Mockito.mock(WorkerService.class);
        DataSourceDefinitionRepository repository = Mockito.mock(DataSourceDefinitionRepository.class);
        MockDataSourceDefinitionService dataSourceDefinitionService = new MockDataSourceDefinitionService(repository);
        TestConnectionHandler testConnectionHandler = new TestConnectionHandler(agentGroupService, messageQueueService,
                dataSourceService, userService, workerService, dataSourceDefinitionService);
        UserDetail userDetail = Mockito.mock(UserDetail.class);
        when(userService.loadUserById(null)).thenReturn(userDetail);
        MessageInfo messageInfo = new MessageInfo();
        Map data = new HashMap();
        data.put("config", new HashMap());
        data.put("pdkHash", "123456");
        messageInfo.setData(data);
        WebSocketContext context = new WebSocketContext("test", "test", messageInfo);
        testConnectionHandler.handleMessage(context);
        boolean actualData = dataSourceDefinitionService.isFlag();
        assertTrue(actualData);
    }
}
