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
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
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
            MetadataInstancesCompareResult result = service.getMetadataInstancesCompareResult(nodeId, taskId, tableFilter, page, pageSize,null);

            // Then
            assertNotNull(result);
            assertNotNull(result.getCompareDtos());
            assertNotNull(result.getInvalidApplyDtos());
            assertEquals(2, result.getCompareDtos().getItems().size());

            verify(service, times(4)).findAll(any(Query.class));
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
            MetadataInstancesCompareResult result = service.getMetadataInstancesCompareResult(nodeId, taskId, tableFilter, page, pageSize,null);

            // Then
            assertNotNull(result);
            verify(service, times(4)).findAll(any(Query.class));
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
            MetadataInstancesCompareResult result = service.getMetadataInstancesCompareResult(nodeId, taskId, tableFilter, page, pageSize,null);

            // Then
            assertNotNull(result);
            verify(service, times(4)).findAll(any(Query.class));
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
            MetadataInstancesCompareResult result = service.getMetadataInstancesCompareResult(nodeId, taskId, tableFilter, page, pageSize,null);

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
            List<MetadataInstancesCompareDto> result = service.getInvalidApplyDtos(compareDtos, applyDtos,null,null);

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
            List<MetadataInstancesCompareDto> result = service.getInvalidApplyDtos(compareDtos, applyDtos,null,null);

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
            List<MetadataInstancesCompareDto> result = service.getInvalidApplyDtos(compareDtos, applyDtos,null,null);

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
            List<MetadataInstancesCompareDto> result = service.getInvalidApplyDtos(Collections.emptyList(), applyDtos,null,null);

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
            List<MetadataInstancesCompareDto> result = service.getInvalidApplyDtos(compareDtos, Collections.emptyList(),null,null);

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
            List<MetadataInstancesCompareDto> result = service.getInvalidApplyDtos(compareDtos, applyDtos,null,null);

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
        dto1.setName("table1");
        dto1.setFields(createMockFields());

        MetadataInstancesDto dto2 = new MetadataInstancesDto();
        dto2.setName("table2");
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
            doReturn(compareDtos, applyDtos,compareDtos, applyDtos).when(service).findAll(any(Query.class));

            // When
            MetadataInstancesCompareResult result = service.getMetadataInstancesCompareResult(nodeId, taskId, tableFilter, page, pageSize,null);

            // Then
            assertNotNull(result);
            assertNotNull(result.getInvalidApplyDtos());
            assertEquals(1, result.getInvalidApplyDtos().size());
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
            List<MetadataInstancesCompareDto> result1 = service.getInvalidApplyDtos(null, null,null,null);
            assertNotNull(result1);
            assertTrue(result1.isEmpty());

            List<MetadataInstancesCompareDto> result2 = service.getInvalidApplyDtos(createMockCompareDtos(), null,null,null);
            assertNotNull(result2);
            assertTrue(result2.isEmpty());

            List<MetadataInstancesCompareDto> result3 = service.getInvalidApplyDtos(null, createMockApplyDtos(),null,null);
            assertNotNull(result3);
            assertFalse(result3.isEmpty());
        }
    }

    @Nested
    @DisplayName("geMetadataInstancesCompareDtoByType Tests")
    class GeMetadataInstancesCompareDtoByTypeTests {
        @BeforeEach
        void setUp() {
            // Set up the service dependencies using reflection
            when(repository.getMongoOperations()).thenReturn(mock(MongoTemplate.class));
        }

        @Test
        @DisplayName("Should return filtered results with pagination")
        void testGeMetadataInstancesCompareDtoByType_WithPagination() {
            // Given
            List<String> types = Arrays.asList("Additional", "Missing");
            String tableFilter = "test";
            int page = 1;
            int pageSize = 10;

            List<MetadataInstancesCompareDto> expectedResults = createMockCompareDtos();
            AggregationResults<MetadataInstancesCompareDto> aggregationResults = mock(AggregationResults.class);
            when(aggregationResults.getMappedResults()).thenReturn(expectedResults);
            when(repository.getMongoOperations().aggregate(any(Aggregation.class), eq("MetadataInstancesCompare"), eq(MetadataInstancesCompareDto.class)))
                    .thenReturn(aggregationResults);

            // When
            List<MetadataInstancesCompareDto> result = service.geMetadataInstancesCompareDtoByType(nodeId, page, pageSize, types, tableFilter);

            // Then
            assertNotNull(result);
            assertEquals(expectedResults.size(), result.size());
            verify(repository.getMongoOperations()).aggregate(any(Aggregation.class), eq("MetadataInstancesCompare"), eq(MetadataInstancesCompareDto.class));
        }

        @Test
        @DisplayName("Should return all results without pagination when pageSize is 0")
        void testGeMetadataInstancesCompareDtoByType_WithoutPagination() {
            // Given
            List<String> types = Arrays.asList("Additional");
            int page = 1;
            int pageSize = 0; // No pagination

            List<MetadataInstancesCompareDto> expectedResults = createMockCompareDtos();
            AggregationResults<MetadataInstancesCompareDto> aggregationResults = mock(AggregationResults.class);
            when(aggregationResults.getMappedResults()).thenReturn(expectedResults);
            when(repository.getMongoOperations().aggregate(any(Aggregation.class), eq("MetadataInstancesCompare"), eq(MetadataInstancesCompareDto.class)))
                    .thenReturn(aggregationResults);

            // When
            List<MetadataInstancesCompareDto> result = service.geMetadataInstancesCompareDtoByType(nodeId, page, pageSize, types, null);

            // Then
            assertNotNull(result);
            assertEquals(expectedResults.size(), result.size());
        }

        @Test
        @DisplayName("Should handle empty types list")
        void testGeMetadataInstancesCompareDtoByType_EmptyTypes() {
            // Given
            List<String> types = Collections.emptyList();
            AggregationResults<MetadataInstancesCompareDto> aggregationResults = mock(AggregationResults.class);
            when(aggregationResults.getMappedResults()).thenReturn(Collections.emptyList());
            when(repository.getMongoOperations().aggregate(any(Aggregation.class), eq("MetadataInstancesCompare"), eq(MetadataInstancesCompareDto.class)))
                    .thenReturn(aggregationResults);

            // When
            List<MetadataInstancesCompareDto> result = service.geMetadataInstancesCompareDtoByType(nodeId, 1, 10, types, null);

            // Then
            assertNotNull(result);
            assertTrue(result.isEmpty());
        }
    }

    @Nested
    @DisplayName("getMetadataInstancesComparesByType Tests")
    class GetMetadataInstancesComparesByTypeTests {

        @Test
        @DisplayName("Should merge auto apply and user apply DTOs correctly")
        void testGetMetadataInstancesComparesByType_MergeResults() {
            // Given
            List<String> types = Arrays.asList("Additional", "Missing");
            List<MetadataInstancesCompareDto> autoApplyDtos = createMockCompareDtos();
            List<MetadataInstancesCompareDto> userApplyDtos = createMockApplyDtos();

            // Mock geMetadataInstancesCompareDtoByType call
            doReturn(autoApplyDtos).when(service).geMetadataInstancesCompareDtoByType(nodeId, 0, 0, types, null);
            doReturn(userApplyDtos).when(service).findAll(any(Query.class));

            // When
            Map<String, List<DifferenceField>> result = service.getMetadataInstancesComparesByType(nodeId, types);

            // Then
            assertNotNull(result);
            assertFalse(result.isEmpty());
            verify(service).geMetadataInstancesCompareDtoByType(nodeId, 0, 0, types, null);
            verify(service).findAll(any(Query.class));
        }

        @Test
        @DisplayName("Should handle empty types list")
        void testGetMetadataInstancesComparesByType_EmptyTypes() {
            // Given
            List<String> types = Collections.emptyList();
            List<MetadataInstancesCompareDto> userApplyDtos = createMockApplyDtos();
            doReturn(userApplyDtos).when(service).findAll(any(Query.class));

            // When
            Map<String, List<DifferenceField>> result = service.getMetadataInstancesComparesByType(nodeId, types);

            // Then
            assertNotNull(result);
            verify(service, never()).geMetadataInstancesCompareDtoByType(anyString(), anyInt(), anyInt(), anyList(), anyString());
        }

        @Test
        @DisplayName("Should handle null types list")
        void testGetMetadataInstancesComparesByType_NullTypes() {
            // Given
            List<MetadataInstancesCompareDto> userApplyDtos = createMockApplyDtos();
            doReturn(userApplyDtos).when(service).findAll(any(Query.class));

            // When
            Map<String, List<DifferenceField>> result = service.getMetadataInstancesComparesByType(nodeId, null);

            // Then
            assertNotNull(result);
            verify(service, never()).geMetadataInstancesCompareDtoByType(anyString(), anyInt(), anyInt(), anyList(), anyString());
        }
    }

    @Nested
    @DisplayName("compareAndGetMetadataInstancesCompareResult Tests")
    class CompareAndGetMetadataInstancesCompareResultTests {

        @Test
        @DisplayName("Should return empty result when task not found")
        void testCompareAndGetMetadataInstancesCompareResult_TaskNotFound() {
            // Given
            when(service.findOne(any(Query.class))).thenReturn(null);
            when(taskService.findOne(any(Query.class), any(UserDetail.class))).thenReturn(null);

            // When
            MetadataInstancesCompareResult result = service.compareAndGetMetadataInstancesCompareResult(nodeId, taskId, userDetail, false);

            // Then
            assertNotNull(result);
            // Should return empty result when validation fails
        }

        @Test
        @DisplayName("Should skip comparison for schema-free connection")
        void testCompareAndGetMetadataInstancesCompareResult_SchemaFreeConnection() {
            // Given
            TaskDto taskDto = createMockTaskDto();
            DataParentNode targetNode = new DatabaseNode();
            targetNode.setConnectionId("connectionId");
            targetNode.setAttrs(Collections.singletonMap("connectionTags", Arrays.asList("schema-free")));

            when(taskDto.getDag().getNode(nodeId)).thenReturn(targetNode);


            when(taskService.findOne(any(Query.class), any(UserDetail.class))).thenReturn(taskDto);

            // When
            MetadataInstancesCompareResult result = service.compareAndGetMetadataInstancesCompareResult(nodeId, taskId, userDetail, false);

            // Then
            assertNotNull(result);

        }

        @Test
        @DisplayName("Should perform comparison when needed")
        void testCompareAndGetMetadataInstancesCompareResult_PerformComparison() {
            // Given
            TaskDto taskDto = createMockTaskDto();
            DataParentNode targetNode = new DatabaseNode();
            targetNode.setConnectionId("connectionId");
            targetNode.setAttrs(Collections.singletonMap("connectionTags", new ArrayList<>()));

            when(taskDto.getDag().getNode(nodeId)).thenReturn(targetNode);
            try(MockedStatic<SchemaUtils> schemaUtilsMockedStatic = mockStatic(SchemaUtils.class)) {
                schemaUtilsMockedStatic.when(() -> SchemaUtils.compareSchema(any(), any())).thenReturn(Arrays.asList(DifferenceField.buildMissingField("field1", createMockField("field1", "varchar(255)"))));
                when(service.findOne(any(Query.class))).thenReturn(null); // No previous comparison
                when(taskService.findOne(any(Query.class), any(UserDetail.class))).thenReturn(taskDto);
                when(metadataInstancesService.findByNodeId(anyString(), any(UserDetail.class), anyString(), anyString(),anyString(),anyString(),anyString(),anyString(),anyString())).thenReturn(createMockMetadataInstancesDtos());
                when(metadataInstancesService.findDatabaseMetadataInstanceLastUpdate(anyString(), any(UserDetail.class))).thenReturn(System.currentTimeMillis());
                when(service.findAll(any(Query.class))).thenReturn(Collections.emptyList());
                when(service.getApplyRules(anyString(), anyString())).thenReturn(Arrays.asList("Additional", "Missing"));
                when(service.getMetadataInstancesComparesByType(anyString(), anyList())).thenReturn(new HashMap<>());
                when(metadataInstancesService.findSourceSchemaBySourceId(anyString(), anyList(), any(UserDetail.class), anyString(), anyString(), anyString(), anyString(), anyString(), anyString())).thenReturn(createMockMetadataInstancesDtos());
                doReturn(Collections.emptyList()).when(service).save(anyList(), any(UserDetail.class));
                doReturn(1L).when(service).upsert(any(Query.class), any(MetadataInstancesCompareDto.class));
                // When
                MetadataInstancesCompareResult result = service.compareAndGetMetadataInstancesCompareResult(nodeId, taskId, userDetail, false);

                // Then
                assertNotNull(result);
                verify(service).upsert(any(Query.class), any(MetadataInstancesCompareDto.class));
            }


        }
    }

    @Nested
    @DisplayName("saveMetadataInstancesCompare Tests")
    class SaveMetadataInstancesCompareTests {

        @Test
        @DisplayName("Should save metadata instances compare successfully")
        void testSaveMetadataInstancesCompare_Success() {
            // Given
            MetadataInstancesDto deductionMetadata = createMockMetadataInstancesDto("table1", "qualified.table1");
            MetadataInstancesDto targetMetadata = createMockMetadataInstancesDto("table1", "qualified.table1");
            List<MetadataInstancesCompareDto> compareDtos = new ArrayList<>();
            Map<String, List<DifferenceField>> applyFields = new HashMap<>();
            applyFields.put("qualified.table1", createMockDifferenceFields());

            // When
            service.saveMetadataInstancesCompare(taskId, nodeId, deductionMetadata, targetMetadata, compareDtos, applyFields);

            // Then
            assertEquals(1, compareDtos.size());
            assertEquals(nodeId, compareDtos.get(0).getNodeId());
            assertEquals("table1", compareDtos.get(0).getTableName());
            assertEquals("qualified.table1", compareDtos.get(0).getQualifiedName());
        }

        @Test
        @DisplayName("Should handle null target metadata")
        void testSaveMetadataInstancesCompare_NullTarget() {
            // Given
            MetadataInstancesDto deductionMetadata = createMockMetadataInstancesDto("table1", "qualified.table1");
            List<MetadataInstancesCompareDto> compareDtos = new ArrayList<>();
            Map<String, List<DifferenceField>> applyFields = new HashMap<>();

            // When
            service.saveMetadataInstancesCompare(taskId, nodeId, deductionMetadata, null, compareDtos, applyFields);

            // Then
            assertTrue(compareDtos.isEmpty());
        }

        @Test
        @DisplayName("Should handle empty apply fields")
        void testSaveMetadataInstancesCompare_EmptyApplyFields() {
            // Given
            MetadataInstancesDto deductionMetadata = createMockMetadataInstancesDto("table1", "qualified.table1");
            MetadataInstancesDto targetMetadata = createMockMetadataInstancesDto("table1", "qualified.table1");
            List<MetadataInstancesCompareDto> compareDtos = new ArrayList<>();
            Map<String, List<DifferenceField>> applyFields = new HashMap<>();

            // When
            service.saveMetadataInstancesCompare(taskId, nodeId, deductionMetadata, targetMetadata, compareDtos, applyFields);

            // Then
            assertEquals(1, compareDtos.size());
        }
    }

    @Nested
    @DisplayName("getAllInvalidApplyDtos Tests")
    class GetAllInvalidApplyDtosTests {

        @Test
        @DisplayName("Should return all invalid apply DTOs with empty difference field list")
        void testGetAllInvalidApplyDtos_Success() {
            // Given
            List<String> applyRules = Arrays.asList("Additional", "Missing");
            MetadataInstancesCompareResult compareResult = new MetadataInstancesCompareResult();

            List<MetadataInstancesCompareDto> compareDtos = createMockCompareDtos();
            List<MetadataInstancesCompareDto> applyDtos = createMockApplyDtos();
            List<MetadataInstancesCompareDto> invalidApplyDtos = createMockApplyDtos();

            when(service.findAll(any(Query.class))).thenReturn(compareDtos, applyDtos);
            doReturn(invalidApplyDtos).when(service).getInvalidApplyDtos(eq(compareDtos), eq(applyDtos), eq(applyRules), eq(compareResult));

            // When
            List<MetadataInstancesCompareDto> result = service.getAllInvalidApplyDtos(nodeId, applyRules, compareResult);

            // Then
            assertNotNull(result);
            assertEquals(invalidApplyDtos.size(), result.size());
            // Verify that difference field lists are cleared
            result.forEach(dto -> assertTrue(dto.getDifferenceFieldList().isEmpty()));
        }

        @Test
        @DisplayName("Should handle empty invalid apply DTOs")
        void testGetAllInvalidApplyDtos_EmptyInvalid() {
            // Given
            List<String> applyRules = Arrays.asList("Additional", "Missing");
            MetadataInstancesCompareResult compareResult = new MetadataInstancesCompareResult();

            List<MetadataInstancesCompareDto> compareDtos = createMockCompareDtos();
            List<MetadataInstancesCompareDto> applyDtos = createMockApplyDtos();

            when(service.findAll(any(Query.class))).thenReturn(compareDtos, applyDtos);
            doReturn(Collections.emptyList()).when(service).getInvalidApplyDtos(eq(compareDtos), eq(applyDtos), eq(applyRules), eq(compareResult));

            // When
            List<MetadataInstancesCompareDto> result = service.getAllInvalidApplyDtos(nodeId, applyRules, compareResult);

            // Then
            assertNotNull(result);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should handle null apply rules")
        void testGetAllInvalidApplyDtos_NullApplyRules() {
            // Given
            MetadataInstancesCompareResult compareResult = new MetadataInstancesCompareResult();

            List<MetadataInstancesCompareDto> compareDtos = createMockCompareDtos();
            List<MetadataInstancesCompareDto> applyDtos = createMockApplyDtos();

            when(service.findAll(any(Query.class))).thenReturn(compareDtos, applyDtos);
            doReturn(Collections.emptyList()).when(service).getInvalidApplyDtos(eq(compareDtos), eq(applyDtos), isNull(), eq(compareResult));

            // When
            List<MetadataInstancesCompareDto> result = service.getAllInvalidApplyDtos(nodeId, null, compareResult);

            // Then
            assertNotNull(result);
            assertTrue(result.isEmpty());
        }
    }

    // Helper method to create mock MetadataInstancesDto
    private MetadataInstancesDto createMockMetadataInstancesDto(String tableName, String qualifiedName) {
        MetadataInstancesDto dto = new MetadataInstancesDto();
        dto.setName(tableName);
        dto.setQualifiedName(qualifiedName);
        dto.setFields(createMockFields());
        return dto;
    }
}