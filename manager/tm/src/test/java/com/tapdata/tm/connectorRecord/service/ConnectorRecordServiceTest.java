package com.tapdata.tm.connectorRecord.service;

import com.mongodb.client.result.DeleteResult;
import com.tapdata.tm.commons.base.dto.SchedulableDto;
import com.tapdata.tm.commons.metrics.ConnectorRecordDto;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.connectorRecord.entity.ConnectorRecordEntity;
import com.tapdata.tm.connectorRecord.repository.ConnectorRecordRepository;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.worker.vo.CalculationEngineVo;
import com.tapdata.tm.ws.dto.MessageInfo;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.OngoingStubbing;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ConnectorRecordServiceTest {

    @Mock
    private ConnectorRecordRepository mockRepository;
    @Mock
    private MongoTemplate mockMongoTemplate;

    private ConnectorRecordService connectorRecordServiceUnderTest;

    @BeforeEach
    void setUp() {
        connectorRecordServiceUnderTest = new ConnectorRecordService(mockRepository);
        ReflectionTestUtils.setField(connectorRecordServiceUnderTest, "mongoTemplate", mockMongoTemplate);
    }


    @Test
    void testUploadConnectorRecord() {
        // Setup
        final ConnectorRecordDto connectorRecordDto = new ConnectorRecordDto();
        connectorRecordDto.setPdkHash("pdkHash");
        connectorRecordDto.setStatus("status");
        connectorRecordDto.setDownloadSpeed("downloadSpeed");
        connectorRecordDto.setDownFiledMessage("downFiledMessage");

        final UserDetail userDetail = new UserDetail("userId", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        final ConnectorRecordEntity expectedResult = new ConnectorRecordEntity();
        expectedResult.setPdkHash("pdkHash");
        expectedResult.setPdkId("pdkId");
        expectedResult.setStatus("status");
        expectedResult.setDownloadSpeed("downloadSpeed");
        expectedResult.setDownFiledMessage("downFiledMessage");

        when(mockMongoTemplate.findOne(Query.query(Criteria.where("pdkHash").is(connectorRecordDto.getPdkHash())), Document.class, "DatabaseTypes"))
                .thenReturn(new Document("pdkId", "pdkId"));

        when(mockRepository.insert(expectedResult,userDetail)).thenReturn(expectedResult);

        // Run the test
        final ConnectorRecordEntity result = connectorRecordServiceUnderTest.uploadConnectorRecord(connectorRecordDto,
                userDetail);

        // Verify the results
        assertThat(result).isEqualTo(expectedResult);
    }

    @Test
    void testUploadConnectorRecord_MongoTemplateReturnsNull() {
        // Setup
        final ConnectorRecordDto connectorRecordDto = new ConnectorRecordDto();
        connectorRecordDto.setPdkHash("pdkHash");
        connectorRecordDto.setStatus("status");
        connectorRecordDto.setDownloadSpeed("downloadSpeed");
        connectorRecordDto.setDownFiledMessage("downFiledMessage");

        final UserDetail userDetail = new UserDetail("userId", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        when(mockMongoTemplate.findOne(Query.query(Criteria.where("pdkHash").is(connectorRecordDto.getPdkHash())), Document.class, "DatabaseTypes")).thenReturn(null);

        // Run the test
        final ConnectorRecordEntity result = connectorRecordServiceUnderTest.uploadConnectorRecord(connectorRecordDto,
                userDetail);

        // Verify the results
        assertThat(result).isNull();
    }

    @Test
    void testUploadConnectorRecord_MongoTemplateReturnPdkIdsNull() {
        // Setup
        final ConnectorRecordDto connectorRecordDto = new ConnectorRecordDto();
        connectorRecordDto.setPdkHash("pdkHash");
        connectorRecordDto.setStatus("status");
        connectorRecordDto.setDownloadSpeed("downloadSpeed");
        connectorRecordDto.setDownFiledMessage("downFiledMessage");

        final UserDetail userDetail = new UserDetail("userId", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        when(mockMongoTemplate.findOne(Query.query(Criteria.where("pdkHash").is(connectorRecordDto.getPdkHash())), Document.class, "DatabaseTypes"))
                .thenReturn(new Document("pdkId", null));

        // Run the test
        final ConnectorRecordEntity result = connectorRecordServiceUnderTest.uploadConnectorRecord(connectorRecordDto,
                userDetail);

        // Verify the results
        assertThat(result).isNull();
    }

    @Test
    void testQueryByConnectionId(){
        ConnectorRecordEntity connectorRecordEntity = new ConnectorRecordEntity();
        connectorRecordEntity.setConnectionId("123");
        connectorRecordEntity.setFlag(false);
        connectorRecordEntity.setStatus(ConnectorRecordDto.StatusEnum.FINISH.getStatus());
        connectorRecordEntity.setFileSize(45049480L);
        connectorRecordEntity.setProgress(100L);
        when(mockMongoTemplate.findOne(Query.query(Criteria.where("connectionId").is("123")), ConnectorRecordEntity.class)).thenReturn(connectorRecordEntity);
        ConnectorRecordEntity result = connectorRecordServiceUnderTest.queryByConnectionId("123");
        assertThat(result).isEqualTo(connectorRecordEntity);
    }

    @Test
    void testDeleteByConnectionId(){
        DeleteResult deleteResult = new DeleteResult() {
            @Override
            public boolean wasAcknowledged() {
                return false;
            }

            @Override
            public long getDeletedCount() {
                return 1;
            }
        };
        when(mockMongoTemplate.remove(Query.query(Criteria.where("connectionId").is("123")), ConnectorRecordEntity.class)).thenReturn(deleteResult);
        connectorRecordServiceUnderTest.deleteByConnectionId("123");
        verify(mockMongoTemplate,times(1)).remove(Query.query(Criteria.where("connectionId").is("123")), ConnectorRecordEntity.class);
    }

    /**
     * agentTags is null
     */
    @Test
    void testAddAgentTagsIsNull(){
        Map<String,Object> platformInfo=new HashMap<>();
        List<String> list = connectorRecordServiceUnderTest.addAgentTags(platformInfo);
        assertThat(list.size()).isEqualTo(0);
    }

    @Test
    void testAddAgentTagsIsNotNull(){
        Map<String,Object> data=new HashMap<>();
        Map<String,Object> platformInfo=new HashMap<>();
        platformInfo.put("region","-");
        data.put("platformInfo",platformInfo);
        List<String> tags = connectorRecordServiceUnderTest.addAgentTags(data);
        assertThat(tags.get(0)).isEqualTo("-");
    }

    @Test
    void testAddAgentTagsInternet(){
        Map<String,Object> data=new HashMap<>();
        Map<String,Object> platformInfo=new HashMap<>();
        platformInfo.put("internet",true);
        data.put("platformInfo",platformInfo);
        List<String> tags = connectorRecordServiceUnderTest.addAgentTags(data);
        assertThat(tags.get(0)).isEqualTo("internet");
    }

    @Test
    void testGetReceiver(){
        final UserDetail userDetail = new UserDetail("userId", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        Map<String,Object> data=new HashMap<>();
        data.put("accessNodeType","AUTOMATIC_PLATFORM_ALLOCATION");
        WorkerService workerService = Mockito.mock(WorkerService.class);
        CalculationEngineVo calculationEngineVo=new CalculationEngineVo();
        calculationEngineVo.setProcessId("ProcessId");
        when(workerService.scheduleTaskToEngine(any(),any(),anyString(), anyString())).thenReturn(calculationEngineVo);
        ReflectionTestUtils.setField(connectorRecordServiceUnderTest, "workerService", workerService);
        AtomicReference<String> receiver = connectorRecordServiceUnderTest.getReceiver(data, null, userDetail);
        assertThat(receiver.get()).isEqualTo("ProcessId");
    }
    @DisplayName("MANUALLY_SPECIFIED_BY_THE_USER have receiver")
    @Test
    void testGetReceiver1(){
        final UserDetail userDetail = new UserDetail("userId", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        Map<String,Object> data=new HashMap<>();
        data.put("accessNodeType","MANUALLY_SPECIFIED_BY_THE_USER");
        data.put("accessNodeProcessId","pid");
        WorkerService workerService = Mockito.mock(WorkerService.class);
        List<Worker> workers=new ArrayList<>();
        Worker worker=new Worker();
        worker.setProcessId("pid");
        workers.add(worker);
        ReflectionTestUtils.setField(connectorRecordServiceUnderTest, "workerService", workerService);
        when(workerService.findAvailableAgentByAccessNode(userDetail, Lists.newArrayList("pid"))).thenReturn(workers);
        AtomicReference<String> receiver = connectorRecordServiceUnderTest.getReceiver(data, null, userDetail);
        assertThat(receiver.get()).isEqualTo("pid");
    }

    @DisplayName("MANUALLY_SPECIFIED_BY_THE_USER no have receiver")
    @Test
    void testGetReceiver2(){
        final UserDetail userDetail = new UserDetail("userId", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        Map<String,Object> data=new HashMap<>();
        data.put("accessNodeType","MANUALLY_SPECIFIED_BY_THE_USER");
        data.put("accessNodeProcessId","pid");
        WorkerService workerService = Mockito.mock(WorkerService.class);
        List<Worker> workers=new ArrayList<>();
        ReflectionTestUtils.setField(connectorRecordServiceUnderTest, "workerService", workerService);
        when( workerService.findAvailableAgentByAccessNode(userDetail, Lists.newArrayList("pid"))).thenReturn(workers);
        connectorRecordServiceUnderTest.getReceiver(data, null, userDetail);
        assertThat(data.get("status").toString()).isEqualTo("error");
    }

    @Test
    void testSendMessage(){
        final UserDetail userDetail = new UserDetail("userId", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        MessageInfo messageInfo=new MessageInfo();
        Map<String,Object> data=new HashMap<>();
        data.put("accessNodeType","MANUALLY_SPECIFIED_BY_THE_USER");
        data.put("accessNodeProcessId","pid");
        messageInfo.setData(data);
        WorkerService workerService = Mockito.mock(WorkerService.class);
        List<Worker> workers=new ArrayList<>();
        Worker worker=new Worker();
        worker.setProcessId("pid");
        workers.add(worker);
        ReflectionTestUtils.setField(connectorRecordServiceUnderTest, "workerService", workerService);
        when(workerService.findAvailableAgentByAccessNode(userDetail, Lists.newArrayList("pid"))).thenReturn(workers);
        MessageQueueService messageQueueService = Mockito.mock(MessageQueueService.class);
        ReflectionTestUtils.setField(connectorRecordServiceUnderTest, "messageQueueService", messageQueueService);
        connectorRecordServiceUnderTest.sendMessage(messageInfo,userDetail);
        verify(messageQueueService,times(1)).sendMessage(any(MessageQueueDto.class));
    }
}
