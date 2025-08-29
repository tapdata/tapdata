package com.tapdata.tm.metadatainstance.service;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadataInstancesCompare.param.MetadataInstancesApplyParam;
import com.tapdata.tm.metadataInstancesCompare.repository.MetadataInstancesCompareRepository;
import com.tapdata.tm.metadatainstance.vo.MetadataInstancesCompareResult;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.MongoUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MetadataInstancesCompareServiceImplTest {

    @Mock
    private MetadataInstancesCompareRepository repository;

    @Mock
    private MetadataInstancesService metadataInstancesService;

    @Mock
    private TaskService taskService;

    private MetadataInstancesCompareServiceImpl service;

    private UserDetail userDetail;
    private String nodeId;
    private String taskId;
    private String tableName;
    private String qualifiedName;

    @BeforeEach
    void setUp() {
        userDetail = new UserDetail("userId", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));;
        userDetail.setUserId("testUserId");
        nodeId = "testNodeId";
        taskId = "507f1f77bcf86cd799439011";
        tableName = "testTable";
        qualifiedName = "testSchema.testTable";

        // Set up the service dependencies using reflection
        service = spy(new MetadataInstancesCompareServiceImpl(repository));
        ReflectionTestUtils.setField(service, "metadataInstancesService", metadataInstancesService);
        ReflectionTestUtils.setField(service, "taskService", taskService);
    }

    @Nested
    @DisplayName("saveMetadataInstancesCompareApply Tests")
    class SaveMetadataInstancesCompareApplyTests {

        @Test
        @DisplayName("Should handle save all apply when all is true")
        void testSaveMetadataInstancesCompareApply_All() {
            // Given
            List<MetadataInstancesApplyParam> params = createMockApplyParams();
            List<MetadataInstancesCompareDto> compareDtos = createMockCompareDtos();

            when(service.findAll(any(Query.class))).thenReturn(compareDtos);
            doReturn(1L).when(service).deleteAll(any(Query.class));
            doReturn(compareDtos).when(service).save(anyList(), any(UserDetail.class));
            when(metadataInstancesService.findAll(any(Query.class))).thenReturn(createMockMetadataInstancesDtos());
            when(metadataInstancesService.bulkUpsetByWhere(anyList(), any(UserDetail.class))).thenReturn(mock(Pair.class));

            // When
            service.saveMetadataInstancesCompareApply(params, userDetail, true, nodeId);

            // Then
            verify(service).deleteAll(argThat(query ->
                query.toString().contains("nodeId") &&
                query.toString().contains(MetadataInstancesCompareDto.TYPE_APPLY)
            ));
            verify(service).save(anyList(), eq(userDetail));
        }

        @Test
        @DisplayName("Should handle save partial apply when all is false")
        void testSaveMetadataInstancesCompareApply_Partial() {
            // Given
            List<MetadataInstancesApplyParam> params = createMockApplyParams();
            List<MetadataInstancesCompareDto> compareDtos = createMockCompareDtos();

            when(service.findAll(any(Query.class))).thenReturn(compareDtos);
            doReturn(compareDtos).when(service).save(anyList(), any(UserDetail.class));
            when(metadataInstancesService.findAll(any(Query.class))).thenReturn(createMockMetadataInstancesDtos());
            when(metadataInstancesService.bulkUpsetByWhere(anyList(), any(UserDetail.class))).thenReturn(mock(Pair.class));

            // When
            service.saveMetadataInstancesCompareApply(params, userDetail, false, nodeId);

            // Then
            verify(service, atLeastOnce()).findAll(any(Query.class));
            verify(service, atLeastOnce()).save(anyList(), eq(userDetail));
        }

        @Test
        @DisplayName("Should handle empty compare dtos in save all")
        void testSaveMetadataInstancesCompareApply_EmptyCompareDtos() {
            // Given
            List<MetadataInstancesApplyParam> params = createMockApplyParams();

            when(service.findAll(any(Query.class))).thenReturn(Collections.emptyList());

            // When
            service.saveMetadataInstancesCompareApply(params, userDetail, true, nodeId);

            // Then
            verify(service).findAll(any(Query.class));
            verify(service, never()).deleteAll(any(Query.class));
            verify(service, never()).save(anyList(), any(UserDetail.class));
        }

        @Test
        @DisplayName("Should handle empty apply params in partial save")
        void testSaveMetadataInstancesCompareApply_EmptyApplyParams() {
            // Given
            List<MetadataInstancesApplyParam> params = Collections.emptyList();

            // When
            service.saveMetadataInstancesCompareApply(params, userDetail, false, nodeId);

            // Then
            verify(service, never()).findAll(any(Query.class));
            verify(service, never()).save(anyList(), any(UserDetail.class));
        }
    }

    @Nested
    @DisplayName("deleteMetadataInstancesCompareApply Tests")
    class DeleteMetadataInstancesCompareApplyTests {

        @Test
        @DisplayName("Should handle delete all apply when all is true")
        void testDeleteMetadataInstancesCompareApply_All() {
            // Given
            List<MetadataInstancesApplyParam> params = createMockApplyParams();
            List<MetadataInstancesCompareDto> compareDtos = createMockCompareDtos();

            when(service.findAll(any(Query.class))).thenReturn(compareDtos);
            doReturn(1L).when(service).deleteAll(any(Query.class));
            when(metadataInstancesService.findAll(any(Query.class))).thenReturn(createMockMetadataInstancesDtos());
            when(metadataInstancesService.bulkUpsetByWhere(anyList(), any(UserDetail.class))).thenReturn(mock(Pair.class));
            // When
            service.deleteMetadataInstancesCompareApply(params, userDetail, true, false,nodeId);

            // Then
            verify(service).deleteAll(any(Query.class));
        }

        @Test
        @DisplayName("Should handle delete partial apply when all is false")
        void testDeleteMetadataInstancesCompareApply_Partial() {
            // Given
            List<MetadataInstancesApplyParam> params = createMockApplyParams();
            List<MetadataInstancesCompareDto> existingDtos = createMockCompareDtos();

            when(service.findAll(any(Query.class))).thenReturn(existingDtos);
            doReturn(10L).when(service).deleteAll(any(Query.class));
            doReturn(existingDtos).when(service).save(anyList(), any(UserDetail.class));
            when(metadataInstancesService.findAll(any(Query.class))).thenReturn(createMockMetadataInstancesDtos());
            when(metadataInstancesService.bulkUpsetByWhere(anyList(), any(UserDetail.class))).thenReturn(mock(Pair.class));

            // When
            service.deleteMetadataInstancesCompareApply(params, userDetail, false, false,nodeId);

            // Then
            verify(service, atLeastOnce()).findAll(any(Query.class));
            verify(service).deleteAll(any(Query.class));
        }


        @Test
        @DisplayName("Should handle empty apply params in partial delete")
        void testDeleteMetadataInstancesCompareApply_EmptyApplyParams() {
            // Given
            List<MetadataInstancesApplyParam> params = Collections.emptyList();

            // When
            service.deleteMetadataInstancesCompareApply(params, userDetail, false, false,nodeId);

            // Then
            verify(service, never()).findAll(any(Query.class));
            verify(service, never()).deleteAll(any(Query.class));
        }
    }

    @Nested
    @DisplayName("getMetadataInstancesCompareResult Tests")
    class GetMetadataInstancesCompareResultTests {

        @Test
        @DisplayName("Should return compare result with pagination")
        void testGetMetadataInstancesCompareResult_Success() {
            // Given
            String tableFilter = "test";
            int page = 1;
            int pageSize = 10;

            List<MetadataInstancesCompareDto> compareDtos = createMockCompareDtos();
            List<MetadataInstancesCompareDto> applyDtos = createMockApplyDtos();

            MetadataInstancesCompareDto metadataInstancesCompareStatus = MetadataInstancesCompareDto.createMetadataInstancesCompareDtoStatus(nodeId);
            metadataInstancesCompareStatus.setStatus(MetadataInstancesCompareDto.STATUS_DONE);
            doReturn(metadataInstancesCompareStatus).when(service).findOne(any(Query.class));

            doReturn(compareDtos, applyDtos).when(service).findAll(any(Query.class));

            // When
            MetadataInstancesCompareResult result = service.getMetadataInstancesCompareResult(nodeId, taskId, tableFilter, page, pageSize);

            // Then
            assertNotNull(result);
            assertNotNull(result.getCompareDtos());
            assertNotNull(result.getInvalidApplyDtos());
            assertEquals(2, result.getCompareDtos().getItems().size());

            verify(service, times(2)).findAll(any(Query.class));
        }

        @Test
        @DisplayName("Should handle empty table filter")
        void testGetMetadataInstancesCompareResult_EmptyTableFilter() {
            // Given
            String tableFilter = "";
            int page = 1;
            int pageSize = 10;

            List<MetadataInstancesCompareDto> compareDtos = createMockCompareDtos();
            List<MetadataInstancesCompareDto> applyDtos = createMockApplyDtos();

            MetadataInstancesCompareDto metadataInstancesCompareStatus = MetadataInstancesCompareDto.createMetadataInstancesCompareDtoStatus(nodeId);
            metadataInstancesCompareStatus.setStatus(MetadataInstancesCompareDto.STATUS_DONE);
            doReturn(metadataInstancesCompareStatus).when(service).findOne(any(Query.class));

            doReturn(compareDtos, applyDtos).when(service).findAll(any(Query.class));

            // When
            MetadataInstancesCompareResult result = service.getMetadataInstancesCompareResult(nodeId, taskId, tableFilter, page, pageSize);

            // Then
            assertNotNull(result);
            verify(service, times(2)).findAll(any(Query.class));
        }

        @Test
        @DisplayName("Should handle null table filter")
        void testGetMetadataInstancesCompareResult_NullTableFilter() {
            // Given
            String tableFilter = null;
            int page = 1;
            int pageSize = 10;

            List<MetadataInstancesCompareDto> compareDtos = createMockCompareDtos();
            List<MetadataInstancesCompareDto> applyDtos = createMockApplyDtos();

            MetadataInstancesCompareDto metadataInstancesCompareStatus = MetadataInstancesCompareDto.createMetadataInstancesCompareDtoStatus(nodeId);
            metadataInstancesCompareStatus.setStatus(MetadataInstancesCompareDto.STATUS_DONE);
            doReturn(metadataInstancesCompareStatus).when(service).findOne(any(Query.class));

            doReturn(compareDtos, applyDtos).when(service).findAll(any(Query.class));

            // When
            MetadataInstancesCompareResult result = service.getMetadataInstancesCompareResult(nodeId, taskId, tableFilter, page, pageSize);

            // Then
            assertNotNull(result);
            verify(service, times(2)).findAll(any(Query.class));
        }

        @Test
        @DisplayName("Should handle empty compare dtos")
        void testGetMetadataInstancesCompareResult_EmptyCompareDtos() {
            // Given
            String tableFilter = "test";
            int page = 1;
            int pageSize = 10;

            List<MetadataInstancesCompareDto> applyDtos = createMockApplyDtos();

            MetadataInstancesCompareDto metadataInstancesCompareStatus = MetadataInstancesCompareDto.createMetadataInstancesCompareDtoStatus(nodeId);
            metadataInstancesCompareStatus.setStatus(MetadataInstancesCompareDto.STATUS_DONE);
            doReturn(metadataInstancesCompareStatus).when(service).findOne(any(Query.class));
            doReturn(Collections.emptyList(), applyDtos).when(service).findAll(any(Query.class));


            // When
            MetadataInstancesCompareResult result = service.getMetadataInstancesCompareResult(nodeId, taskId, tableFilter, page, pageSize);

            // Then
            assertNotNull(result);
            assertTrue(result.getCompareDtos().getItems().isEmpty());
        }
    }

    @Nested
    @DisplayName("getApplyRules Tests")
    class GetApplyRulesTests {

        @Test
        @DisplayName("Should return apply rules when task exists and has apply rules")
        void testGetApplyRules_Success() {
            // Given
            TaskDto taskDto = createMockTaskDto();
            DataParentNode targetNode = createMockDataParentNode();
            targetNode.setApplyCompareRule(true);
            targetNode.setApplyCompareRules(Arrays.asList("rule1", "rule2", "rule3"));

            try (MockedStatic<MongoUtils> mongoUtilsMock = mockStatic(MongoUtils.class)) {
                mongoUtilsMock.when(() -> MongoUtils.toObjectId(taskId)).thenReturn(new ObjectId(taskId));
                when(taskService.findOne(any(Query.class))).thenReturn(taskDto);
                when(taskDto.getDag().getNode(nodeId)).thenReturn(targetNode);

                // When
                List<String> result = service.getApplyRules(nodeId, taskId);

                // Then
                assertNotNull(result);
                assertEquals(3, result.size());
                assertTrue(result.contains("rule1"));
                assertTrue(result.contains("rule3"));
                assertTrue(result.contains("rule2"));
            }
        }

        @Test
        @DisplayName("Should return null when task not found")
        void testGetApplyRules_TaskNotFound() {
            // Given
            try (MockedStatic<MongoUtils> mongoUtilsMock = mockStatic(MongoUtils.class)) {
                mongoUtilsMock.when(() -> MongoUtils.toObjectId(taskId)).thenReturn(new ObjectId(taskId));
                when(taskService.findOne(any(Query.class))).thenReturn(null);

                // When
                List<String> result = service.getApplyRules(nodeId, taskId);

                // Then
                assertEquals(0,result.size());
            }
        }

        @Test
        @DisplayName("Should return null when apply compare rule is false")
        void testGetApplyRules_ApplyCompareRuleFalse() {
            // Given
            TaskDto taskDto = createMockTaskDto();
            DataParentNode targetNode = createMockDataParentNode();
            targetNode.setApplyCompareRule(false);

            try (MockedStatic<MongoUtils> mongoUtilsMock = mockStatic(MongoUtils.class)) {
                mongoUtilsMock.when(() -> MongoUtils.toObjectId(taskId)).thenReturn(new ObjectId(taskId));
                when(taskService.findOne(any(Query.class))).thenReturn(taskDto);
                when(taskDto.getDag().getNode(nodeId)).thenReturn(targetNode);

                // When
                List<String> result = service.getApplyRules(nodeId, taskId);

                // Then
                assertEquals(0,result.size());
            }
        }

        @Test
        @DisplayName("Should return null when apply compare rule is null")
        void testGetApplyRules_ApplyCompareRuleNull() {
            // Given
            TaskDto taskDto = createMockTaskDto();
            DataParentNode targetNode = createMockDataParentNode();

            try (MockedStatic<MongoUtils> mongoUtilsMock = mockStatic(MongoUtils.class)) {
                mongoUtilsMock.when(() -> MongoUtils.toObjectId(taskId)).thenReturn(new ObjectId(taskId));
                when(taskService.findOne(any(Query.class))).thenReturn(taskDto);
                when(taskDto.getDag().getNode(nodeId)).thenReturn(targetNode);

                // When
                List<String> result = service.getApplyRules(nodeId, taskId);

                // Then
                assertEquals(0,result.size());
            }
        }

        @Test
        @DisplayName("Should return empty list when no rules are enabled")
        void testGetApplyRules_NoEnabledRules() {
            // Given
            TaskDto taskDto = createMockTaskDto();
            DataParentNode targetNode = createMockDataParentNode();
            targetNode.setApplyCompareRule(true);
            targetNode.setApplyCompareRules(new ArrayList<>());

            try (MockedStatic<MongoUtils> mongoUtilsMock = mockStatic(MongoUtils.class)) {
                mongoUtilsMock.when(() -> MongoUtils.toObjectId(taskId)).thenReturn(new ObjectId(taskId));
                when(taskService.findOne(any(Query.class))).thenReturn(taskDto);
                when(taskDto.getDag().getNode(nodeId)).thenReturn(targetNode);

                // When
                List<String> result = service.getApplyRules(nodeId, taskId);

                // Then
                assertNotNull(result);
                assertTrue(result.isEmpty());
            }
        }
    }

    @Nested
    @DisplayName("getInvalidApplyDtos Tests")
    class GetInvalidApplyDtosTests {

        @Test
        @DisplayName("Should return invalid apply dtos when field not in compare dtos")
        void testGetInvalidApplyDtos_FieldNotInCompareDtos() {
            // Given
            List<MetadataInstancesCompareDto> compareDtos = createMockCompareDtos();
            List<MetadataInstancesCompareDto> applyDtos = createMockApplyDtos();

            // Add invalid field to apply dtos
            applyDtos.get(0).getDifferenceFieldList().add(DifferenceField.buildMissingField("invalidField", createMockField("invalidField", "varchar(255)")));

            // When
            List<MetadataInstancesCompareDto> result = service.getInvalidApplyDtos(compareDtos, applyDtos,null);

            // Then
            assertNotNull(result);
            assertEquals(1, result.size());
            assertEquals(1, result.get(0).getDifferenceFieldList().size());
            assertEquals("invalidField", result.get(0).getDifferenceFieldList().get(0).getColumnName());
        }

        @Test
        @DisplayName("Should return invalid apply dtos when type mismatch")
        void testGetInvalidApplyDtos_TypeMismatch() {
            // Given
            List<MetadataInstancesCompareDto> compareDtos = createMockCompareDtos();
            List<MetadataInstancesCompareDto> applyDtos = createMockApplyDtos();

            // Modify apply dto to have different type
            DifferenceField invalidField = DifferenceField.buildMissingField("field1", createMockField("field1", "varchar(255)"));
            invalidField.setType(DifferenceTypeEnum.Additional); // Different from compare dto
            applyDtos.get(0).getDifferenceFieldList().set(0, invalidField);

            // When
            List<MetadataInstancesCompareDto> result = service.getInvalidApplyDtos(compareDtos, applyDtos,null);

            // Then
            assertNotNull(result);
            assertEquals(1, result.size());
            assertEquals(1, result.get(0).getDifferenceFieldList().size());
        }

        @Test
        @DisplayName("Should return empty list when all apply dtos are valid")
        void testGetInvalidApplyDtos_AllValid() {
            // Given
            List<MetadataInstancesCompareDto> compareDtos = createMockCompareDtos();
            List<MetadataInstancesCompareDto> applyDtos = createMockApplyDtos();

            // When
            List<MetadataInstancesCompareDto> result = service.getInvalidApplyDtos(compareDtos, applyDtos,null);

            // Then
            assertNotNull(result);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should handle empty compare dtos")
        void testGetInvalidApplyDtos_EmptyCompareDtos() {
            // Given
            List<MetadataInstancesCompareDto> applyDtos = createMockApplyDtos();

            // When
            List<MetadataInstancesCompareDto> result = service.getInvalidApplyDtos(Collections.emptyList(), applyDtos,null);

            // Then
            assertNotNull(result);
            assertFalse(result.isEmpty());
        }

        @Test
        @DisplayName("Should handle empty apply dtos")
        void testGetInvalidApplyDtos_EmptyApplyDtos() {
            // Given
            List<MetadataInstancesCompareDto> compareDtos = createMockCompareDtos();

            // When
            List<MetadataInstancesCompareDto> result = service.getInvalidApplyDtos(compareDtos, Collections.emptyList(),null);

            // Then
            assertNotNull(result);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should handle table not in compare dtos")
        void testGetInvalidApplyDtos_TableNotInCompareDtos() {
            // Given
            List<MetadataInstancesCompareDto> compareDtos = createMockCompareDtos();
            List<MetadataInstancesCompareDto> applyDtos = createMockApplyDtos();

            // Change table name to make it not match
            applyDtos.get(0).setTableName("nonExistentTable");
            applyDtos.get(0).setQualifiedName("qualified.nonExistentTable");

            // When
            List<MetadataInstancesCompareDto> result = service.getInvalidApplyDtos(compareDtos, applyDtos,null);

            // Then
            assertNotNull(result);
            assertEquals(1, result.size());
            assertEquals("nonExistentTable", result.get(0).getTableName());
        }
    }

    // Helper methods for creating mock data
    private List<MetadataInstancesCompareDto> createMockCompareDtos() {
        List<MetadataInstancesCompareDto> dtos = new ArrayList<>();

        MetadataInstancesCompareDto dto1 = new MetadataInstancesCompareDto();
        dto1.setNodeId(nodeId);
        dto1.setTableName("table1");
        dto1.setQualifiedName("qualified.table1");
        dto1.setType(MetadataInstancesCompareDto.TYPE_COMPARE);
        dto1.setDifferenceFieldList(createMockDifferenceFields());

        MetadataInstancesCompareDto dto2 = new MetadataInstancesCompareDto();
        dto2.setNodeId(nodeId);
        dto2.setTableName("table2");
        dto2.setQualifiedName("qualified.table2");
        dto2.setType(MetadataInstancesCompareDto.TYPE_COMPARE);
        dto2.setDifferenceFieldList(createMockDifferenceFields());

        dtos.add(dto1);
        dtos.add(dto2);
        return dtos;
    }

    private List<MetadataInstancesCompareDto> createMockApplyDtos() {
        List<MetadataInstancesCompareDto> dtos = new ArrayList<>();

        MetadataInstancesCompareDto dto1 = new MetadataInstancesCompareDto();
        dto1.setNodeId(nodeId);
        dto1.setTableName("table1");
        dto1.setQualifiedName("qualified.table1");
        dto1.setType(MetadataInstancesCompareDto.TYPE_APPLY);
        dto1.setDifferenceFieldList(createMockDifferenceFields());

        dtos.add(dto1);
        return dtos;
    }

    private List<DifferenceField> createMockDifferenceFields() {
        List<DifferenceField> fields = new ArrayList<>();

        Field sourceField1 = createMockField("field1", "varchar(255)");
        Field targetField1 = createMockField("field1", "text");
        fields.add(DifferenceField.buildDifferentField("field1", sourceField1, targetField1));

        Field sourceField2 = createMockField("field2", "int");
        fields.add(DifferenceField.buildMissingField("field2", sourceField2));

        Field targetField3 = createMockField("field3", "datetime");
        fields.add(DifferenceField.buildAdditionalField("field3", targetField3));

        return fields;
    }

    private Field createMockField(String fieldName, String dataType) {
        Field field = new Field();
        field.setFieldName(fieldName);
        field.setDataType(dataType);
        field.setTapType("TapString");
        return field;
    }

    private List<MetadataInstancesApplyParam> createMockApplyParams() {
        List<MetadataInstancesApplyParam> params = new ArrayList<>();

        MetadataInstancesApplyParam param1 = new MetadataInstancesApplyParam();
        param1.setQualifiedName("qualified.table1");
        param1.setFieldNames(Arrays.asList("field1", "field2"));

        MetadataInstancesApplyParam param2 = new MetadataInstancesApplyParam();
        param2.setQualifiedName("qualified.table2");
        param2.setFieldNames(Arrays.asList("field3"));

        params.add(param1);
        params.add(param2);
        return params;
    }

    private List<MetadataInstancesDto> createMockMetadataInstancesDtos() {
        List<MetadataInstancesDto> dtos = new ArrayList<>();

        MetadataInstancesDto dto1 = new MetadataInstancesDto();
        dto1.setQualifiedName("qualified.table1");
        dto1.setFields(createMockFields());

        MetadataInstancesDto dto2 = new MetadataInstancesDto();
        dto2.setQualifiedName("qualified.table2");
        dto2.setFields(createMockFields());

        dtos.add(dto1);
        dtos.add(dto2);
        return dtos;
    }

    private List<Field> createMockFields() {
        List<Field> fields = new ArrayList<>();

        Field field1 = createMockField("field1", "varchar(255)");
        Field field2 = createMockField("field2", "int");
        Field field3 = createMockField("field3", "datetime");

        fields.add(field1);
        fields.add(field2);
        fields.add(field3);
        return fields;
    }

    private TaskDto createMockTaskDto() {
        TaskDto taskDto = mock(TaskDto.class);
        DAG dag = mock(DAG.class);
        when(taskDto.getDag()).thenReturn(dag);
        return taskDto;
    }

    private DataParentNode createMockDataParentNode() {
        DataParentNode node = new DatabaseNode();
        return node;
    }

    @Nested
    @DisplayName("Integration Tests")
    class IntegrationTests {

        @Test
        @DisplayName("Should handle complete save and delete workflow")
        void testCompleteWorkflow() {
            // Given
            List<MetadataInstancesApplyParam> params = createMockApplyParams();
            List<MetadataInstancesCompareDto> compareDtos = createMockCompareDtos();
            List<MetadataInstancesCompareDto> applyDtos = createMockApplyDtos();

            doReturn(compareDtos, applyDtos,compareDtos).when(service).findAll(any(Query.class));
            doReturn(1L).when(service).deleteAll(any(Query.class));
            doReturn(compareDtos).when(service).save(anyList(), any(UserDetail.class));
            when(metadataInstancesService.findAll(any(Query.class))).thenReturn(createMockMetadataInstancesDtos(),createMockMetadataInstancesDtos());
            doReturn(mock(Pair.class)).when(metadataInstancesService).bulkUpsetByWhere(anyList(), any(UserDetail.class));

            // When - Save apply configurations
            service.saveMetadataInstancesCompareApply(params, userDetail, false, nodeId);

            // Then - Verify save operations
            verify(service, atLeastOnce()).findAll(any(Query.class));
            verify(service, atLeastOnce()).save(anyList(), eq(userDetail));

            // When - Delete apply configurations
            service.deleteMetadataInstancesCompareApply(params, userDetail, false, false,nodeId);

            // Then - Verify delete operations
            verify(service, atLeastOnce()).deleteAll(any(Query.class));
        }

        @Test
        @DisplayName("Should handle get compare result with invalid apply dtos")
        void testGetCompareResultWithInvalidApplyDtos() {
            // Given
            String tableFilter = "test";
            int page = 1;
            int pageSize = 10;

            List<MetadataInstancesCompareDto> compareDtos = createMockCompareDtos();
            List<MetadataInstancesCompareDto> applyDtos = createMockApplyDtos();

            // Add invalid field to apply dtos
            applyDtos.get(0).getDifferenceFieldList().add(DifferenceField.buildMissingField("invalidField", createMockField("invalidField", "varchar(255)")));
            MetadataInstancesCompareDto metadataInstancesCompareStatus = MetadataInstancesCompareDto.createMetadataInstancesCompareDtoStatus(nodeId);
            metadataInstancesCompareStatus.setStatus(MetadataInstancesCompareDto.STATUS_DONE);
            doReturn(metadataInstancesCompareStatus).when(service).findOne(any(Query.class));
            doReturn(compareDtos, applyDtos).when(service).findAll(any(Query.class));

            // When
            MetadataInstancesCompareResult result = service.getMetadataInstancesCompareResult(nodeId, taskId, tableFilter, page, pageSize);

            // Then
            assertNotNull(result);
            assertNotNull(result.getInvalidApplyDtos());
            assertEquals(1, result.getInvalidApplyDtos().size());
            assertEquals("invalidField", result.getInvalidApplyDtos().get(0).getDifferenceFieldList().get(0).getColumnName());
        }

        @Test
        @DisplayName("Should handle edge cases with null and empty values")
        void testEdgeCases() {
            // Test with null parameters
            assertDoesNotThrow(() -> service.saveMetadataInstancesCompareApply(null, userDetail, false, nodeId));
            assertDoesNotThrow(() -> service.deleteMetadataInstancesCompareApply(null, userDetail, false, false,nodeId));

            // Test with empty parameters
            assertDoesNotThrow(() -> service.saveMetadataInstancesCompareApply(Collections.emptyList(), userDetail, false, nodeId));
            assertDoesNotThrow(() -> service.deleteMetadataInstancesCompareApply(Collections.emptyList(), userDetail, false, false,nodeId));

            // Test getInvalidApplyDtos with null parameters
            List<MetadataInstancesCompareDto> result1 = service.getInvalidApplyDtos(null, null,null);
            assertNotNull(result1);
            assertTrue(result1.isEmpty());

            List<MetadataInstancesCompareDto> result2 = service.getInvalidApplyDtos(createMockCompareDtos(), null,null);
            assertNotNull(result2);
            assertTrue(result2.isEmpty());

            List<MetadataInstancesCompareDto> result3 = service.getInvalidApplyDtos(null, createMockApplyDtos(),null);
            assertNotNull(result3);
            assertFalse(result3.isEmpty());
        }
    }
}