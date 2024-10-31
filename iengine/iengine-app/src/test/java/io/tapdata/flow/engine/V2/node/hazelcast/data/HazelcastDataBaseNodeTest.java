package io.tapdata.flow.engine.V2.node.hazelcast.data;

import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.core.type.TypeReference;
import com.tapdata.constant.JSONUtil;
import com.tapdata.entity.SyncStage;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.dataflow.TableBatchReadStatus;
import com.tapdata.entity.dataflow.batch.BatchOffsetUtil;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.flow.engine.V2.util.SyncTypeEnum;
import io.tapdata.observable.logging.ObsLogger;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.*;

public class HazelcastDataBaseNodeTest {
    class SubHazelcastDataBaseNodeTest extends HazelcastDataBaseNode {

        public SubHazelcastDataBaseNodeTest(DataProcessorContext dataProcessorContext) {
            super(dataProcessorContext);
        }
    }

    @Nested
    class FoundAllSyncProgressTest {
        SubHazelcastDataBaseNodeTest hazelcastDataBaseNodeTest;

        @BeforeEach
        void init() {
            TaskDto taskDto = new TaskDto();
            taskDto.setType(SyncTypeEnum.INITIAL_SYNC.getSyncType());
            DataProcessorContext dataProcessorContext = DataProcessorContext.newBuilder().withTaskDto(taskDto).build();
            hazelcastDataBaseNodeTest = new SubHazelcastDataBaseNodeTest(dataProcessorContext);
        }

        @DisplayName("test get sync Progress successful")
        @SneakyThrows
        @Test
        void test1() {
            Map<String, String> syncProgressMap = genSyncProgress();
            Map<String, Object> attrs = new HashMap<>();
            attrs.put("syncProgress", syncProgressMap);
            Map<String, SyncProgress> stringSyncProgressMap = hazelcastDataBaseNodeTest.foundAllSyncProgress(attrs);
            assertEquals(stringSyncProgressMap.containsKey("sourceId,targetId"), true);
        }

        @DisplayName("test Serialize syncProgress json appear Error")
        @SneakyThrows
        @Test
        void test2() {
            hazelcastDataBaseNodeTest = mock(SubHazelcastDataBaseNodeTest.class);
            doCallRealMethod().when(hazelcastDataBaseNodeTest).foundAllSyncProgress(anyMap());
            Map<String, String> syncProgressMap = genSyncProgress();
            Map<String, Object> attrs = new HashMap<>();
            attrs.put("syncProgress", syncProgressMap);
            try (MockedStatic<JSONUtil> jsonUtilMockedStatic = mockStatic(JSONUtil.class)) {
                jsonUtilMockedStatic.when(() -> {
                    JSONUtil.json2List(anyString(), any(Class.class));
                }).thenCallRealMethod();
                jsonUtilMockedStatic.when(() -> {
                    JSONUtil.json2POJO(anyString(), any(TypeReference.class));
                }).thenThrow(new IOException("ERROR"));
                Map<String, SyncProgress> stringSyncProgressMap = hazelcastDataBaseNodeTest.foundAllSyncProgress(attrs);
            } catch (RuntimeException e) {
                assertEquals(e.getMessage().contains("Init sync progress failed"), true);
            }
        }

        @DisplayName("test Serialize list json appear Error")
        @SneakyThrows
        @Test
        void test3() {
            hazelcastDataBaseNodeTest = mock(SubHazelcastDataBaseNodeTest.class);
            doCallRealMethod().when(hazelcastDataBaseNodeTest).foundAllSyncProgress(anyMap());
            Map<String, String> syncProgressMap = genSyncProgress();
            Map<String, Object> attrs = new HashMap<>();
            attrs.put("syncProgress", syncProgressMap);
            try (MockedStatic<JSONUtil> jsonUtilMockedStatic = mockStatic(JSONUtil.class)) {
                jsonUtilMockedStatic.when(() -> {
                    JSONUtil.json2List(anyString(), any(Class.class));
                }).thenThrow(new IOException("io error"));
                hazelcastDataBaseNodeTest.foundAllSyncProgress(attrs);
            } catch (RuntimeException e) {
                assertEquals(e.getMessage().contains("Init sync progress failed"), true);
            }
        }

        @DisplayName("test attr is empty")
        @Test
        void test4() {
            Map<String, Object> attrs = new HashMap<>();
            Map<String, SyncProgress> stringSyncProgressMap = hazelcastDataBaseNodeTest.foundAllSyncProgress(attrs);
            assertEquals(stringSyncProgressMap.size() == 0, true);
        }

        @DisplayName("test syncProgress is null")
        @Test
        void test5() {
            Map<String, Object> attrs = new HashMap<>();
            attrs.put("syncProgress", null);
            ObsLogger obsLogger = mock(ObsLogger.class);
            ReflectionTestUtils.setField(hazelcastDataBaseNodeTest, "obsLogger", obsLogger);
            hazelcastDataBaseNodeTest.foundAllSyncProgress(attrs);
            verify(obsLogger, times(1)).info(anyString());
        }

        @SneakyThrows
        @NotNull
        private Map<String, String> genSyncProgress() {
            List<String> keyList = new ArrayList<>();
            keyList.add("sourceId");
            keyList.add("targetId");
            String jsonString = JSON.toJSONString(keyList);
            SyncProgress syncProgress = new SyncProgress();
            Map<String, String> syncProgressMap = new HashMap<>();
            Map<String, Object> batchOffset = new HashMap<>();
            batchOffset.put(BatchOffsetUtil.BATCH_READ_CONNECTOR_STATUS, TableBatchReadStatus.OVER);
            syncProgress.setBatchOffsetObj(batchOffset);
            syncProgress.setBatchOffset(PdkUtil.encodeOffset(batchOffset));
            String syncProgressString = JSONUtil.obj2Json(syncProgress);
            syncProgressMap.put(jsonString, syncProgressString);
            return syncProgressMap;
        }
    }

    @Nested
    class FoundNodeSyncProgressTest {
        SubHazelcastDataBaseNodeTest hazelcastDataBaseNodeTest;

        @BeforeEach
        void init() {
            TaskDto taskDto = new TaskDto();
            taskDto.setType(SyncTypeEnum.INITIAL_SYNC.getSyncType());
            DatabaseNode databaseNode = new DatabaseNode();
            databaseNode.setId("targetNode");
            DataProcessorContext dataProcessorContext = DataProcessorContext.newBuilder().withTaskDto(taskDto).withNode(databaseNode).build();
            hazelcastDataBaseNodeTest = new SubHazelcastDataBaseNodeTest(dataProcessorContext);
        }

        @Test
        void test1() {
            Map<String, SyncProgress> syncProgressMap = new HashMap<>();
            SyncProgress syncProgress = new SyncProgress();
            syncProgress.setEventSerialNo(null);
            syncProgressMap.put("source1,targetNode", syncProgress);
            SyncProgress syncProgressResult = hazelcastDataBaseNodeTest.foundNodeSyncProgress(syncProgressMap);
            assertEquals(syncProgressResult, syncProgress);
        }

        @Test
        void test2() {
            Map<String, SyncProgress> syncProgressMap = new HashMap<>();
            SyncProgress source1Progress = new SyncProgress();
            syncProgressMap.put("source1,targetNode", source1Progress);
            source1Progress.setEventSerialNo(10L);
            SyncProgress source2Progress = new SyncProgress();
            syncProgressMap.put("source2,targetNode", source2Progress);
            source2Progress.setEventSerialNo(1L);
            SyncProgress syncProgressResult = hazelcastDataBaseNodeTest.foundNodeSyncProgress(syncProgressMap);
            assertEquals(syncProgressResult.getEventSerialNo(), 1L);
        }
    }
}
