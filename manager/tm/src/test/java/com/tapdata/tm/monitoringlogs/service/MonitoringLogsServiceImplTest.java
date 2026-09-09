package com.tapdata.tm.monitoringlogs.service;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import com.tapdata.tm.monitoringlogs.entity.MonitoringLogsEntity;
import com.tapdata.tm.monitoringlogs.param.MonitoringLogQueryParam;
import com.tapdata.tm.monitoringlogs.repository.MonitoringLogsRepository;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/11/20 14:02
 */
public class MonitoringLogsServiceImplTest {
    MonitoringLogsServiceImpl monitoringLogsService;
    MongoTemplate mongo;

    @BeforeEach
    void init() {
        mongo = mock(MongoTemplate.class);
        monitoringLogsService = mock(MonitoringLogsServiceImpl.class);
        ReflectionTestUtils.setField(monitoringLogsService, "mongoOperations", mongo);
    }

    @Test
    void testQuery() throws ExecutionException, InterruptedException {

        MonitoringLogsRepository monitoringLogsRepository = mock(MonitoringLogsRepository.class);
        MongoTemplate mongoOperations = mock(MongoTemplate.class);

        when(mongoOperations.count(any(Query.class), eq(MonitoringLogsEntity.class))).thenReturn(0L);

        MonitoringLogsServiceImpl monitoringLogsService =
                new MonitoringLogsServiceImpl(monitoringLogsRepository, CompletableFuture.completedFuture(mongoOperations));

        MonitoringLogQueryParam param = new MonitoringLogQueryParam();
        param.setTaskId("673d825967ac3c7c5785ae02");
        param.setStart(1732421532000L);
        param.setType("testRun");
        param.setOrder("asc");

        param.setIncludeLogTags(Arrays.asList("tag1", "tag2"));

        Page<MonitoringLogsDto> page = monitoringLogsService.query(param);


        param.setIncludeLogTags(null);
        param.setExcludeLogTags(Arrays.asList("tag1", "tag2"));

        page = monitoringLogsService.query(param);

        verify(mongoOperations, times(2)).count(any(Query.class), eq(MonitoringLogsEntity.class));

    }

    @Nested
    class QueryTest {

        @BeforeEach
        void init() {
            when(mongo.count(any(), any(Class.class))).thenReturn(0L);
            when(monitoringLogsService.query(any(MonitoringLogQueryParam.class))).thenCallRealMethod();
        }

        @Test
        void testNodeIdIsNotEmpty() {
            MonitoringLogQueryParam param = new MonitoringLogQueryParam();
            param.setTaskId(new ObjectId().toHexString());
            param.setNodeId(new ObjectId().toHexString());
            param.setType("testRun");
            param.setStart(System.currentTimeMillis());
            param.setOrder(MonitoringLogQueryParam.ORDER_ASC);
            Page<MonitoringLogsDto> query = monitoringLogsService.query(param);
            Assertions.assertEquals(0L, query.getTotal());
            Assertions.assertTrue(query.getItems().isEmpty());

        }

        @Test
        void testScriptNodeIdIsNotEmpty() {
            MonitoringLogQueryParam param = new MonitoringLogQueryParam();
            param.setTaskId(new ObjectId().toHexString());
            param.setNodeId(new ObjectId().toHexString());
            param.setType("testRun");
            param.setStart(System.currentTimeMillis());
            param.setOrder(MonitoringLogQueryParam.ORDER_ASC);
            param.setIncludeLogTags(Arrays.asList("src=user_script"));
            Page<MonitoringLogsDto> query = monitoringLogsService.query(param);
            Assertions.assertEquals(0L, query.getTotal());
            Assertions.assertTrue(query.getItems().isEmpty());

        }
    }

    @Nested
    class GetJsNodeLogTest {
        MongoTemplate mongoOperations;
        MonitoringLogsServiceImpl service;

        @BeforeEach
        void setUp() throws ExecutionException, InterruptedException {
            mongoOperations = mock(MongoTemplate.class);
            service = new MonitoringLogsServiceImpl(
                    mock(MonitoringLogsRepository.class),
                    CompletableFuture.completedFuture(mongoOperations));
            MonitoringLogsEntity entity = new MonitoringLogsEntity();
            entity.setTaskId("transform-id");
            entity.setDate(new Date());
            entity.setLevel("INFO");
            entity.setMessage("PDK connector node stopped");
            when(mongoOperations.find(any(Query.class), eq(MonitoringLogsEntity.class))).thenReturn(List.of(entity));
        }

        @Test
        void formatsEnglishLogWithoutChineseWrapperOrDefaultName() {
            List<TaskDagCheckLog> logs = service.getJsNodeLog(
                    "transform-id", "task-name", "Enhanced JS node", Locale.US);

            String log = logs.get(0).getLog();
            assertEquals(1, logs.size());
            assertTrue(log.contains("[task-name]"));
            assertTrue(log.contains("[Enhanced JS node]"));
            assertTrue(log.contains("PDK connector node stopped"));
            assertFalse(log.contains("增强JS"));
            assertFalse(log.contains("节点"));
            assertFalse(log.contains("【"));
        }

        @Test
        void formatsChineseLogWithoutDuplicatedNodeSuffix() {
            List<TaskDagCheckLog> logs = service.getJsNodeLog(
                    "transform-id", "task-name", "增强JS节点", Locale.CHINA);

            String log = logs.get(0).getLog();
            assertTrue(log.contains("【task-name】"));
            assertTrue(log.contains("【增强JS节点】"));
            assertTrue(log.contains("PDK connector node stopped"));
            assertFalse(log.contains("增强JS节点节点"));
        }
    }
}
