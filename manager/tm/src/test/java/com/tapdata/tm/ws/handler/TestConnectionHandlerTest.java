package com.tapdata.tm.ws.handler;

import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.ws.dto.MessageInfo;
import com.tapdata.tm.ws.dto.WebSocketContext;
import com.tapdata.tm.ws.dto.WebSocketResult;
import com.tapdata.tm.ws.endpoint.WebSocketManager;
import lombok.SneakyThrows;
import org.apache.commons.collections.map.HashedMap;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;

import static com.tapdata.tm.utils.MongoUtils.toObjectId;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class TestConnectionHandlerTest {
    private TestConnectionHandler testConnectionHandler;
    private WebSocketContext context;
    private MessageInfo messageInfo;
    private UserService userService;
    private DataSourceService dataSourceService;
    private MessageQueueService messageQueueService;
    private DataSourceDefinitionService dataSourceDefinitionService;
    private AgentGroupService agentGroupService;
    private WorkerService workerService;
    @BeforeEach
    void buildTestConnectionHandler(){
        testConnectionHandler = mock(TestConnectionHandler.class);
        context = mock(WebSocketContext.class);
        messageInfo = new MessageInfo();
        messageInfo.setType("testConnection");
        when(context.getMessageInfo()).thenReturn(messageInfo);
        userService = mock(UserService.class);
        ReflectionTestUtils.setField(testConnectionHandler,"userService",userService);
        dataSourceService = mock(DataSourceService.class);
        ReflectionTestUtils.setField(testConnectionHandler,"dataSourceService",dataSourceService);
        messageQueueService = mock(MessageQueueService.class);
        ReflectionTestUtils.setField(testConnectionHandler,"messageQueueService",messageQueueService);
        dataSourceDefinitionService = mock(DataSourceDefinitionService.class);
        ReflectionTestUtils.setField(testConnectionHandler,"dataSourceDefinitionService",dataSourceDefinitionService);
        agentGroupService = mock(AgentGroupService.class);
        ReflectionTestUtils.setField(testConnectionHandler,"agentGroupService",agentGroupService);
        workerService = mock(WorkerService.class);
        ReflectionTestUtils.setField(testConnectionHandler,"workerService",workerService);
    }
    @Nested
    class HandleMessageTest{
        @Test
        @SneakyThrows
        @DisplayName("test handle message without user id")
        void test1(){
            try (MockedStatic<WebSocketManager> webSocketManagerMockedStatic = Mockito
                    .mockStatic(WebSocketManager.class)) {
                webSocketManagerMockedStatic.when(()->WebSocketManager.sendMessage(any(),anyString())).thenAnswer(invocation -> null);
                try (MockedStatic<WebSocketResult> webSocketResultMockedStatic = Mockito
                        .mockStatic(WebSocketResult.class)) {
                    webSocketResultMockedStatic.when(() -> WebSocketResult.fail(any())).thenReturn(mock(WebSocketResult.class));
                    Map<String, Object> data = new HashedMap();
                    data.put("type","testConnection");
                    messageInfo.setData(data);
                    when(context.getSender()).thenReturn("sender");
                    doCallRealMethod().when(testConnectionHandler).handleMessage(context);
                    testConnectionHandler.handleMessage(context);
                    assertEquals("testConnection",messageInfo.getData().get("type"));
                    assertEquals("pipe",messageInfo.getType());
                    webSocketResultMockedStatic.verify(()->WebSocketResult.fail(any()),times(1));
//                    webSocketManagerMockedStatic.verify(()-> WebSocketManager.sendMessage("sender",WebSocketResult.fail("UserId is blank")),times(1));
                }
            }
        }
        @Test
        @SneakyThrows
        @DisplayName("test handle message without user detail")
        void test2(){
            try (MockedStatic<WebSocketManager> webSocketManagerMockedStatic = Mockito
                .mockStatic(WebSocketManager.class)) {
            webSocketManagerMockedStatic.when(()->WebSocketManager.sendMessage(any(),anyString())).thenAnswer(invocation -> null);
            Map<String, Object> data = new HashedMap();
            data.put("type","testConnection");
            messageInfo.setData(data);
            when(context.getUserId()).thenReturn("111");
            when(context.getSender()).thenReturn("sender");
            doCallRealMethod().when(testConnectionHandler).handleMessage(context);
            testConnectionHandler.handleMessage(context);
            assertEquals("testConnection",messageInfo.getData().get("type"));
            assertEquals("pipe",messageInfo.getType());
            webSocketManagerMockedStatic.verify(()-> WebSocketManager.sendMessage("sender","UserDetail is null"),times(1));
            }
        }
        @Test
        @SneakyThrows
        @DisplayName("test handle message for mongo")
        void test3(){
            when(context.getUserId()).thenReturn("111");
            when(userService.loadUserById(toObjectId("111"))).thenReturn(mock(UserDetail.class));
            Map<String, Object> data = new HashedMap();
            Map config = new LinkedHashMap();
            config.put("uri","mongodb://root:******@localhost/test");
            config.put("id","111111");
            data.put("id","111");
            data.put("name","mongo");
            data.put("config",config);
            data.put("database_type","MongoDB");
            messageInfo.setData(data);
            DataSourceConnectionDto dataSourceConnectionDto = new DataSourceConnectionDto();
            Map dataSourceConfig = new HashMap();
            dataSourceConfig.put("uri","mongodb://root:123456@localhost/test");
            dataSourceConnectionDto.setConfig(dataSourceConfig);
            when(dataSourceService.findById(toObjectId("111"))).thenReturn(dataSourceConnectionDto);
            doCallRealMethod().when(testConnectionHandler).handleMessage(context);
            testConnectionHandler.handleMessage(context);
            Map config1 = (Map)data.get("config");
            assertEquals(dataSourceConfig.get("uri"),config1.get("uri"));
        }
        @Test
        @SneakyThrows
        @DisplayName("test handle message for mysql")
        void test4(){
            when(context.getUserId()).thenReturn("111");
            when(userService.loadUserById(toObjectId("111"))).thenReturn(mock(UserDetail.class));
            Map<String, Object> data = new HashedMap();
            Map config = new LinkedHashMap();
            config.put("host","localhost");
            config.put("port",3306);
            config.put("database","test");
            config.put("username","root");
            data.put("id","111");
            data.put("name","mongo");
            data.put("config",config);
            data.put("database_type","Mysql");
            messageInfo.setData(data);
            DataSourceConnectionDto dataSourceConnectionDto = new DataSourceConnectionDto();
            Map dataSourceConfig = new HashMap();
            dataSourceConfig.put("password","123456");
            dataSourceConnectionDto.setConfig(dataSourceConfig);
            when(dataSourceService.findById(toObjectId("111"))).thenReturn(dataSourceConnectionDto);
            doCallRealMethod().when(testConnectionHandler).handleMessage(context);
            testConnectionHandler.handleMessage(context);
            Map config1 = (Map)data.get("config");
            assertEquals(dataSourceConfig.get("password"),config1.get("password"));
        }
        @Test
        @SneakyThrows
        @DisplayName("test handle message with blank database type")
        void test5(){
            when(context.getUserId()).thenReturn("111");
            when(userService.loadUserById(toObjectId("111"))).thenReturn(mock(UserDetail.class));
            Map<String, Object> data = new HashedMap();
            Map config = new LinkedHashMap();
            config.put("host","localhost");
            config.put("port",3306);
            config.put("database","test");
            config.put("username","root");
            data.put("id","111");
            data.put("name","mongo");
            data.put("config",config);
            messageInfo.setData(data);
            DataSourceDefinitionDto definitionDto = new DataSourceDefinitionDto();
            definitionDto.setType("Mysql");
            UserDetail userDetail = userService.loadUserById(toObjectId("111"));
            String pdkHash = (String) data.get("pdkHash");
            when(dataSourceDefinitionService.findByPdkHash(pdkHash, Integer.MAX_VALUE, userDetail, "type")).thenReturn(definitionDto);
            DataSourceConnectionDto dataSourceConnectionDto = new DataSourceConnectionDto();
            Map dataSourceConfig = new HashMap();
            dataSourceConfig.put("password","123456");
            dataSourceConnectionDto.setConfig(dataSourceConfig);
            when(dataSourceService.findById(toObjectId("111"))).thenReturn(dataSourceConnectionDto);
            doCallRealMethod().when(testConnectionHandler).handleMessage(context);
            testConnectionHandler.handleMessage(context);
            Map config1 = (Map)data.get("config");
            assertEquals(dataSourceConfig.get("password"),config1.get("password"));
        }
    }
    @Nested
    class CheckPriorityEngineTest{
        @Test
        void test_main(){
            List<Worker> workers = new ArrayList<>();
            Worker worker1 = new Worker();
            worker1.setProcessId("work1");
            Worker worker2 = new Worker();
            worker2.setProcessId("work2");
            workers.add(worker1);
            workers.add(worker2);
            doCallRealMethod().when(testConnectionHandler).checkPriorityEngine(workers,"work2");
            List<Worker> result = testConnectionHandler.checkPriorityEngine(workers,"work2");
            Assertions.assertTrue(result.contains(worker2));
        }
        @Test
        void test_priorityProcessIsNull(){
            List<Worker> workers = new ArrayList<>();
            Worker worker1 = new Worker();
            worker1.setProcessId("work1");
            Worker worker2 = new Worker();
            worker2.setProcessId("work2");
            workers.add(worker1);
            workers.add(worker2);
            doCallRealMethod().when(testConnectionHandler).checkPriorityEngine(workers,null);
            List<Worker> result = testConnectionHandler.checkPriorityEngine(workers,null);
            Assertions.assertTrue(result.contains(worker1));
            Assertions.assertTrue(result.contains(worker2));
        }
        @Test
        void test_priorityProcessIsNotExist(){
            List<Worker> workers = new ArrayList<>();
            Worker worker1 = new Worker();
            worker1.setProcessId("work1");
            Worker worker2 = new Worker();
            worker2.setProcessId("work2");
            workers.add(worker1);
            workers.add(worker2);
            doCallRealMethod().when(testConnectionHandler).checkPriorityEngine(workers,"work3");
            List<Worker> result = testConnectionHandler.checkPriorityEngine(workers,"work3");
            Assertions.assertTrue(result.contains(worker1));
            Assertions.assertTrue(result.contains(worker2));
        }

    }
}
