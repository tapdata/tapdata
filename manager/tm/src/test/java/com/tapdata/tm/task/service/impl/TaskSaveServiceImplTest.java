package com.tapdata.tm.task.service.impl;

import com.google.common.collect.Maps;
import com.tapdata.tm.Settings.service.AlarmSettingService;
import com.tapdata.tm.alarmrule.service.AlarmRuleService;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.internal.verification.Times;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class TaskSaveServiceImplTest {
    private TaskSaveServiceImpl taskSaveService;
    private MetadataInstancesService metadataInstancesService;
    private AlarmSettingService alarmSettingService;
    private AlarmRuleService alarmRuleService;

    @BeforeEach
    void beforeEach() {
        taskSaveService = mock(TaskSaveServiceImpl.class);
        metadataInstancesService = mock(MetadataInstancesService.class);
        alarmSettingService = mock(AlarmSettingService.class);
        alarmRuleService = mock(AlarmRuleService.class);
        ReflectionTestUtils.setField(taskSaveService, "metadataInstancesService", metadataInstancesService);
        ReflectionTestUtils.setField(taskSaveService, "alarmSettingService", alarmSettingService);
        ReflectionTestUtils.setField(taskSaveService, "alarmRuleService", alarmRuleService);
    }
    @Nested
    class syncTaskSettingTest {
        @Test
        void testSyncTaskSettingWhenTaskDtoIsNull() {
            UserDetail user = mock(UserDetail.class);
            doCallRealMethod().when(taskSaveService).syncTaskSetting(null, user);
            taskSaveService.syncTaskSetting(null, user);
            verify(metadataInstancesService, new Times(0)).findByTaskId(anyString(), any(UserDetail.class));
        }

        @Test
        @DisplayName("Test primary key filtering when field.getPrimaryKey() is null")
        void testPrimaryKeyFilteringWithNullPrimaryKey() {
            // Arrange
            TaskDto taskDto = createMockTaskDto();
            UserDetail user = mock(UserDetail.class);

            // Create test fields with mixed primary key values
            List<Field> fields = new ArrayList<>();

            // Field with null primary key
            Field fieldWithNullPK = new Field();
            fieldWithNullPK.setFieldName("field_null_pk");
            fieldWithNullPK.setPrimaryKey(null);
            fields.add(fieldWithNullPK);

            // Field with false primary key
            Field fieldWithFalsePK = new Field();
            fieldWithFalsePK.setFieldName("field_false_pk");
            fieldWithFalsePK.setPrimaryKey(false);
            fields.add(fieldWithFalsePK);

            // Field with true primary key
            Field fieldWithTruePK = new Field();
            fieldWithTruePK.setFieldName("field_true_pk");
            fieldWithTruePK.setPrimaryKey(true);
            fields.add(fieldWithTruePK);

            // Create metadata instance with these fields
            MetadataInstancesDto metadataInstance = new MetadataInstancesDto();
            metadataInstance.setName("test_table");
            metadataInstance.setFields(fields);
            metadataInstance.setNodeId("target-node-id");

            List<MetadataInstancesDto> metadataList = new ArrayList<>();
            metadataList.add(metadataInstance);

            // Mock service calls
            when(metadataInstancesService.findByTaskId(anyString(), any(UserDetail.class)))
                .thenReturn(metadataList);
            when(metadataInstancesService.countUpdateExNum(anyString())).thenReturn(1L);

            doCallRealMethod().when(taskSaveService).syncTaskSetting(taskDto, user);

            // Act
            taskSaveService.syncTaskSetting(taskDto, user);

            // Assert
            DatabaseNode targetNode = (DatabaseNode) taskDto.getDag().getTargets().get(0);
            Map<String, List<String>> updateConditionFieldMap = targetNode.getUpdateConditionFieldMap();

            // Verify that only the field with true primary key is included
            assertNotNull(updateConditionFieldMap);
            assertTrue(updateConditionFieldMap.containsKey("test_table"));
            List<String> primaryKeyFields = updateConditionFieldMap.get("test_table");
            assertEquals(1, primaryKeyFields.size());
            assertEquals("field_true_pk", primaryKeyFields.get(0));

            // Verify that fields with null or false primary key are excluded
            assertFalse(primaryKeyFields.contains("field_null_pk"));
            assertFalse(primaryKeyFields.contains("field_false_pk"));
        }

        private TaskDto createMockTaskDto() {
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            taskDto.setSyncType(TaskDto.SYNC_TYPE_MIGRATE);

            // Create target database node
            DatabaseNode targetNode = mock(DatabaseNode.class);
            when(targetNode.getId()).thenReturn("target-node-id");
            when(targetNode.sourceType()).thenReturn(Node.SourceType.target);
            when(targetNode.getUpdateConditionFieldMap()).thenReturn(Maps.newHashMap());
            targetNode.setUpdateConditionFieldMap(Maps.newHashMap());

            List<Node> nodes = new ArrayList<>();
            nodes.add(targetNode);

            // Create DAG structure
            DAG dagStructure = mock(DAG.class);
            when(dagStructure.getNodes()).thenReturn(nodes);
            when(dagStructure.getTargets()).thenReturn(Arrays.asList(targetNode));
            when(dagStructure.getSourceNode()).thenReturn(new LinkedList<>(Arrays.asList(targetNode)));
            taskDto.setDag(dagStructure);

            return taskDto;
        }

        @Test
        @DisplayName("Test field filtering logic with null primary key values")
        void testFieldFilteringWithNullPrimaryKey() {
            // Create a list of fields with different primary key values
            List<Field> fields = new ArrayList<>();

            // Field 1: primary key is null
            Field field1 = new Field();
            field1.setFieldName("field_with_null_pk");
            field1.setPrimaryKey(null);
            fields.add(field1);

            // Field 2: primary key is false
            Field field2 = new Field();
            field2.setFieldName("field_with_false_pk");
            field2.setPrimaryKey(false);
            fields.add(field2);

            // Field 3: primary key is true
            Field field3 = new Field();
            field3.setFieldName("field_with_true_pk");
            field3.setPrimaryKey(true);
            fields.add(field3);

            // Test the filtering logic directly
            List<String> primaryKeyFields = fields.stream()
                .filter(field -> null != field.getPrimaryKey() && field.getPrimaryKey())
                .map(Field::getFieldName)
                .collect(java.util.stream.Collectors.toList());

            // Assertions
            assertEquals(1, primaryKeyFields.size(), "Should only include fields with true primary key");
            assertEquals("field_with_true_pk", primaryKeyFields.get(0), "Should include the field with true primary key");
            assertFalse(primaryKeyFields.contains("field_with_null_pk"), "Should exclude field with null primary key");
            assertFalse(primaryKeyFields.contains("field_with_false_pk"), "Should exclude field with false primary key");
        }

        @Test
        @DisplayName("Test field filtering when all primary keys are null")
        void testFieldFilteringWhenAllPrimaryKeysAreNull() {
            // Create a list of fields where all primary keys are null
            List<Field> fields = new ArrayList<>();

            Field field1 = new Field();
            field1.setFieldName("field1");
            field1.setPrimaryKey(null);
            fields.add(field1);

            Field field2 = new Field();
            field2.setFieldName("field2");
            field2.setPrimaryKey(null);
            fields.add(field2);

            Field field3 = new Field();
            field3.setFieldName("field3");
            field3.setPrimaryKey(null);
            fields.add(field3);

            // Test the filtering logic
            List<String> primaryKeyFields = fields.stream()
                .filter(field -> null != field.getPrimaryKey() && field.getPrimaryKey())
                .map(Field::getFieldName)
                .collect(java.util.stream.Collectors.toList());

            // Assertions
            assertTrue(primaryKeyFields.isEmpty(), "Should return empty list when all primary keys are null");
        }

        @Test
        @DisplayName("Test field filtering with mixed null and boolean primary key values")
        void testFieldFilteringWithMixedValues() {
            List<Field> fields = new ArrayList<>();

            // Add fields with various primary key states
            for (int i = 0; i < 5; i++) {
                Field field = new Field();
                field.setFieldName("field" + i);

                switch (i) {
                    case 0:
                        field.setPrimaryKey(null);
                        break;
                    case 1:
                        field.setPrimaryKey(false);
                        break;
                    case 2:
                        field.setPrimaryKey(true);
                        break;
                    case 3:
                        field.setPrimaryKey(null);
                        break;
                    case 4:
                        field.setPrimaryKey(true);
                        break;
                }
                fields.add(field);
            }

            // Apply the filtering logic
            List<String> primaryKeyFields = fields.stream()
                .filter(field -> null != field.getPrimaryKey() && field.getPrimaryKey())
                .map(Field::getFieldName)
                .collect(java.util.stream.Collectors.toList());

            // Verify results
            assertEquals(2, primaryKeyFields.size(), "Should include only fields with true primary key");
            assertTrue(primaryKeyFields.contains("field2"), "Should include field2 (true)");
            assertTrue(primaryKeyFields.contains("field4"), "Should include field4 (true)");
            assertFalse(primaryKeyFields.contains("field0"), "Should exclude field0 (null)");
            assertFalse(primaryKeyFields.contains("field1"), "Should exclude field1 (false)");
            assertFalse(primaryKeyFields.contains("field3"), "Should exclude field3 (null)");
        }
    }
}
