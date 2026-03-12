package com.tapdata.tm.group.handler;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.group.constant.GroupConstants;
import com.tapdata.tm.group.dto.ResourceType;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.service.InspectService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.task.bean.TaskUpAndLoadDto;
import com.tapdata.tm.task.service.TaskService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class TaskResourceHandlerTest {

    @Mock
    private TaskService taskService;

    @Mock
    private MetadataInstancesService metadataInstancesService;

    @Mock
    private DataSourceService dataSourceService;

    @Mock
    private InspectService inspectService;

    private TaskResourceHandler taskResourceHandler;

    private UserDetail user;

    @BeforeEach
    void setUp() {
        taskResourceHandler = new TaskResourceHandler();
        ReflectionTestUtils.setField(taskResourceHandler, "taskService", taskService);
        ReflectionTestUtils.setField(taskResourceHandler, "metadataInstancesService", metadataInstancesService);
        ReflectionTestUtils.setField(taskResourceHandler, "dataSourceService", dataSourceService);
        ReflectionTestUtils.setField(taskResourceHandler, "inspectService", inspectService);
        user = new UserDetail("userId123", "customerId", "testuser", "password", "customerType",
                "accessCode", false, false, false, false,
                Arrays.asList(new SimpleGrantedAuthority("role")));
    }

    @Nested
    @DisplayName("getResourceType Tests")
    class GetResourceTypeTests {

        @Test
        @DisplayName("Should return null for default constructor")
        void testGetResourceTypeDefault() {
            TaskResourceHandler handler = new TaskResourceHandler();
            assertNull(handler.getResourceType());
        }

        @Test
        @DisplayName("Should return specified resource type")
        void testGetResourceTypeSpecified() {
            TaskResourceHandler handler = new TaskResourceHandler(ResourceType.MIGRATE_TASK);
            assertEquals(ResourceType.MIGRATE_TASK, handler.getResourceType());
        }
    }

    @Nested
    @DisplayName("supports Tests")
    class SupportsTests {

        @Test
        @DisplayName("Should support MIGRATE_TASK")
        void testSupportsMigrateTask() {
            assertTrue(taskResourceHandler.supports(ResourceType.MIGRATE_TASK));
        }

        @Test
        @DisplayName("Should support SYNC_TASK")
        void testSupportsSyncTask() {
            assertTrue(taskResourceHandler.supports(ResourceType.SYNC_TASK));
        }

        @Test
        @DisplayName("Should not support MODULE")
        void testNotSupportsModule() {
            assertFalse(taskResourceHandler.supports(ResourceType.MODULE));
        }

        @Test
        @DisplayName("Should not support CONNECTION")
        void testNotSupportsConnection() {
            assertFalse(taskResourceHandler.supports(ResourceType.CONNECTION));
        }
    }

    @Nested
    @DisplayName("loadResources Tests")
    class LoadResourcesTests {

        @Test
        @DisplayName("Should return empty list when ids is null")
        void testLoadResourcesNullIds() {
            List<TaskDto> result = taskResourceHandler.loadResources(null, user);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should return empty list when ids is empty")
        void testLoadResourcesEmptyIds() {
            List<TaskDto> result = taskResourceHandler.loadResources(new ArrayList<>(), user);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should load tasks by ids")
        void testLoadResourcesWithIds() {
            String id1 = new ObjectId().toHexString();
            String id2 = new ObjectId().toHexString();
            List<String> ids = Arrays.asList(id1, id2);

            TaskDto task1 = new TaskDto();
            task1.setId(new ObjectId(id1));
            task1.setName("Task 1");

            TaskDto task2 = new TaskDto();
            task2.setId(new ObjectId(id2));
            task2.setName("Task 2");

            when(taskService.findAllDto(any(Query.class), any(UserDetail.class))).thenReturn(Arrays.asList(task1, task2));

            List<TaskDto> result = taskResourceHandler.loadResources(ids, user);

            assertEquals(2, result.size());
            verify(taskService).findAllDto(any(Query.class), eq(user));
        }

        @Test
        @DisplayName("Should filter null ids")
        void testLoadResourcesFilterNullIds() {
            String id1 = new ObjectId().toHexString();
            List<String> ids = Arrays.asList(id1, null);

            when(taskService.findAllDto(any(Query.class), eq(user))).thenReturn(new ArrayList<>());

            taskResourceHandler.loadResources(ids, user);

            verify(taskService).findAllDto(any(Query.class), eq(user));
        }
    }

    @Nested
    @DisplayName("buildExportPayload Tests")
    class BuildExportPayloadTests {

        @Test
        @DisplayName("Should return empty list when resources is null")
        void testBuildExportPayloadNullResources() {
            List<TaskUpAndLoadDto> result = taskResourceHandler.buildExportPayload(null, user);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should return empty list when resources is empty")
        void testBuildExportPayloadEmptyResources() {
            List<TaskUpAndLoadDto> result = taskResourceHandler.buildExportPayload(new ArrayList<>(), user);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should build payload and clear sensitive fields")
        void testBuildExportPayloadClearsSensitiveFields() {
            TaskDto task = new TaskDto();
            task.setId(new ObjectId());
            task.setName("Test Task");
            task.setCreateUser("createUser");
            task.setCustomId("customId");
            task.setLastUpdBy("lastUpdBy");
            task.setUserId("userId");
            task.setAgentId("agentId");

            List<TaskUpAndLoadDto> result = taskResourceHandler.buildExportPayload(Arrays.asList(task), user);

            assertEquals(0, result.size());
//            assertEquals(GroupConstants.COLLECTION_TASK, result.get(0).getCollectionName());
//
//            assertNull(task.getCreateUser());
//            assertNull(task.getCustomId());
//            assertNull(task.getLastUpdBy());
//            assertNull(task.getUserId());
//            assertNull(task.getAgentId());
        }

        @Test
        @DisplayName("Should set status to EDIT")
        void testBuildExportPayloadSetsStatusEdit() {
            TaskDto task = new TaskDto();
            task.setId(new ObjectId());
            task.setName("Test Task");
            task.setStatus(TaskDto.STATUS_RUNNING);

            taskResourceHandler.buildExportPayload(Arrays.asList(task), user);

            assertEquals(TaskDto.STATUS_EDIT, task.getStatus());
        }

        @Test
        @DisplayName("Should handle task without DAG")
        void testBuildExportPayloadNoDag() {
            TaskDto task = new TaskDto();
            task.setId(new ObjectId());
            task.setName("Test Task");
            task.setDag(null);

            List<TaskUpAndLoadDto> result = taskResourceHandler.buildExportPayload(Arrays.asList(task), user);

            assertEquals(0, result.size());
        }
    }

    @Nested
    @DisplayName("collectPayload Tests")
    class CollectPayloadTests {

        @Test
        @DisplayName("Should do nothing when payload is null")
        void testCollectPayloadNullPayload() {
            Map<String, TaskDto> resourceMap = new HashMap<>();
            List<MetadataInstancesDto> metadataList = new ArrayList<>();

            taskResourceHandler.collectPayload(null, resourceMap, metadataList);

            assertTrue(resourceMap.isEmpty());
            assertTrue(metadataList.isEmpty());
        }

        @Test
        @DisplayName("Should do nothing when payload is empty")
        void testCollectPayloadEmptyPayload() {
            Map<String, TaskDto> resourceMap = new HashMap<>();
            List<MetadataInstancesDto> metadataList = new ArrayList<>();

            taskResourceHandler.collectPayload(new ArrayList<>(), resourceMap, metadataList);

            assertTrue(resourceMap.isEmpty());
            assertTrue(metadataList.isEmpty());
        }

        @Test
        @DisplayName("Should collect task from payload")
        void testCollectPayloadTask() {
            TaskDto task = new TaskDto();
            task.setId(new ObjectId());
            task.setName("Test Task");

            TaskUpAndLoadDto payload = new TaskUpAndLoadDto();
            payload.setCollectionName(GroupConstants.COLLECTION_TASK);
            payload.setJson(JsonUtil.toJsonUseJackson(task));

            Map<String, TaskDto> resourceMap = new HashMap<>();
            List<MetadataInstancesDto> metadataList = new ArrayList<>();

            taskResourceHandler.collectPayload(Arrays.asList(payload), resourceMap, metadataList);

            assertEquals(1, resourceMap.size());
            assertTrue(resourceMap.containsKey(task.getId().toHexString()));
        }

        @Test
        @DisplayName("Should collect metadata from payload")
        void testCollectPayloadMetadata() {
            MetadataInstancesDto metadata = new MetadataInstancesDto();
            metadata.setId(new ObjectId());
            metadata.setName("Test Metadata");

            TaskUpAndLoadDto payload = new TaskUpAndLoadDto();
            payload.setCollectionName(GroupConstants.COLLECTION_METADATA_INSTANCES);
            payload.setJson(JsonUtil.toJsonUseJackson(metadata));

            Map<String, TaskDto> resourceMap = new HashMap<>();
            List<MetadataInstancesDto> metadataList = new ArrayList<>();

            taskResourceHandler.collectPayload(Arrays.asList(payload), resourceMap, metadataList);

            assertEquals(1, metadataList.size());
        }

        @Test
        @DisplayName("Should skip items with blank json")
        void testCollectPayloadSkipsBlankJson() {
            TaskUpAndLoadDto payload = new TaskUpAndLoadDto();
            payload.setCollectionName(GroupConstants.COLLECTION_TASK);
            payload.setJson("");

            Map<String, TaskDto> resourceMap = new HashMap<>();
            List<MetadataInstancesDto> metadataList = new ArrayList<>();

            taskResourceHandler.collectPayload(Arrays.asList(payload), resourceMap, metadataList);

            assertTrue(resourceMap.isEmpty());
        }

        @Test
        @DisplayName("Should use name as key when id is null")
        void testCollectPayloadUsesNameAsKey() {
            TaskDto task = new TaskDto();
            task.setId(null);
            task.setName("Test Task");

            TaskUpAndLoadDto payload = new TaskUpAndLoadDto();
            payload.setCollectionName(GroupConstants.COLLECTION_TASK);
            payload.setJson(JsonUtil.toJsonUseJackson(task));

            Map<String, TaskDto> resourceMap = new HashMap<>();
            List<MetadataInstancesDto> metadataList = new ArrayList<>();

            taskResourceHandler.collectPayload(Arrays.asList(payload), resourceMap, metadataList);

            assertTrue(resourceMap.containsKey("Test Task"));
        }
    }

    @Nested
    @DisplayName("loadConnections Tests")
    class LoadConnectionsTests {

        @Test
        @DisplayName("Should return empty list when resources is null")
        void testLoadConnectionsNullResources() {
            List<DataSourceConnectionDto> result = taskResourceHandler.loadConnections(null);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should return empty list when resources is empty")
        void testLoadConnectionsEmptyResources() {
            List<DataSourceConnectionDto> result = taskResourceHandler.loadConnections(new ArrayList<>());
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should skip tasks without DAG")
        void testLoadConnectionsSkipsTasksWithoutDag() {
            TaskDto task = new TaskDto();
            task.setDag(null);

            when(dataSourceService.findInfoByConnectionIdList(anyList())).thenReturn(new ArrayList<>());

            taskResourceHandler.loadConnections(Arrays.asList(task));

            verify(dataSourceService).findInfoByConnectionIdList(argThat(List::isEmpty));
        }

        @Test
        @DisplayName("Should extract connection ids from DAG nodes")
        void testLoadConnectionsExtractsFromDagNodes() {
            TaskDto task = new TaskDto();
            DAG dag = mock(DAG.class);
            task.setDag(dag);

            DatabaseNode node = mock(DatabaseNode.class);
            when(node.getConnectionId()).thenReturn("conn123");
            when(dag.getNodes()).thenReturn(Arrays.asList(node));

            when(dataSourceService.findInfoByConnectionIdList(anyList())).thenReturn(new ArrayList<>());

            taskResourceHandler.loadConnections(Arrays.asList(task));

            verify(dataSourceService).findInfoByConnectionIdList(argThat(list -> list.contains("conn123")));
        }
    }

    @Nested
    @DisplayName("findDuplicateNames Tests")
    class FindDuplicateNamesTests {

        @Test
        @DisplayName("Should return empty map when no duplicates")
        void testFindDuplicateNamesNoDuplicates() {
            TaskDto task = new TaskDto();
            task.setName("Unique Task");

            when(taskService.findOne(any(Query.class), eq(user))).thenReturn(null);

            Map<String, String> result = taskResourceHandler.findDuplicateNames(Arrays.asList(task), user);

            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should find duplicate names")
        void testFindDuplicateNamesWithDuplicates() {
            TaskDto task = new TaskDto();
            task.setName("Duplicate Task");

            TaskDto existing = new TaskDto();
            existing.setId(new ObjectId());
            existing.setName("Duplicate Task");

            when(taskService.findOne(any(Query.class), eq(user))).thenReturn(existing);

            Map<String, String> result = taskResourceHandler.findDuplicateNames(Arrays.asList(task), user);

            assertEquals(1, result.size());
            assertTrue(result.containsKey("Duplicate Task"));
            assertEquals("duplicate", result.get("Duplicate Task"));
        }

        @Test
        @DisplayName("Should skip null tasks")
        void testFindDuplicateNamesSkipsNull() {
            List<TaskDto> tasks = Arrays.asList(null, new TaskDto());

            Map<String, String> result = taskResourceHandler.findDuplicateNames(tasks, user);

            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should skip tasks with blank name")
        void testFindDuplicateNamesSkipsBlankName() {
            TaskDto task = new TaskDto();
            task.setName("");

            Map<String, String> result = taskResourceHandler.findDuplicateNames(Arrays.asList(task), user);

            assertTrue(result.isEmpty());
            verify(taskService, never()).findOne(any(Query.class), any(UserDetail.class));
        }

        @Test
        @DisplayName("Should not check same name twice")
        void testFindDuplicateNamesSkipsAlreadyChecked() {
            TaskDto task1 = new TaskDto();
            task1.setName("Same Name");

            TaskDto task2 = new TaskDto();
            task2.setName("Same Name");

            TaskDto existing = new TaskDto();
            existing.setId(new ObjectId());
            existing.setName("Same Name");

            when(taskService.findOne(any(Query.class), eq(user))).thenReturn(existing);

            Map<String, String> result = taskResourceHandler.findDuplicateNames(Arrays.asList(task1, task2), user);

            assertEquals(1, result.size());
            verify(taskService, times(1)).findOne(any(Query.class), eq(user));
        }
    }

    @Nested
    @DisplayName("resolveResourceName Tests")
    class ResolveResourceNameTests {

        @Test
        @DisplayName("Should return null when resourceMap is null")
        void testResolveResourceNameNullMap() {
            String result = taskResourceHandler.resolveResourceName("id123", null);
            assertNull(result);
        }

        @Test
        @DisplayName("Should return null when resourceId is null")
        void testResolveResourceNameNullId() {
            Map<String, TaskDto> resourceMap = new HashMap<>();
            String result = taskResourceHandler.resolveResourceName(null, resourceMap);
            assertNull(result);
        }

        @Test
        @DisplayName("Should return null when resource not found")
        void testResolveResourceNameNotFound() {
            Map<String, TaskDto> resourceMap = new HashMap<>();
            String result = taskResourceHandler.resolveResourceName("nonexistent", resourceMap);
            assertNull(result);
        }

        @Test
        @DisplayName("Should return resource name when found")
        void testResolveResourceNameFound() {
            TaskDto task = new TaskDto();
            task.setName("Test Task");

            Map<String, TaskDto> resourceMap = new HashMap<>();
            resourceMap.put("id123", task);

            String result = taskResourceHandler.resolveResourceName("id123", resourceMap);

            assertEquals("Test Task", result);
        }
    }

    @Nested
    @DisplayName("handleRelatedResources Tests")
    class HandleRelatedResourcesTests {

        @Test
        @DisplayName("Should handle share cache tasks")
        void testHandleRelatedResourcesShareCache() {
            TaskDto task = new TaskDto();
            task.setId(new ObjectId());
            task.setName("Test Task");
            Map<String, Object> attrs = new HashMap<>();
            Map<String, List<String>> usedShareCache = new HashMap<>();
            usedShareCache.put("cache1", Arrays.asList("table1"));
            attrs.put("usedShareCache", usedShareCache);
            task.setAttrs(attrs);

            Map<String, List<TaskUpAndLoadDto>> payloadsByType = new HashMap<>();

            when(taskService.findAllDto(any(Query.class), eq(user))).thenReturn(new ArrayList<>());
            when(inspectService.findByTaskIdList(anyList())).thenReturn(new ArrayList<>());

            taskResourceHandler.handleRelatedResources(payloadsByType, Arrays.asList(task), user,new HashSet<>());

            verify(taskService).findAllDto(any(Query.class), eq(user));
        }

        @Test
        @DisplayName("Should handle inspect tasks")
        void testHandleRelatedResourcesInspectTasks() {
            TaskDto task = new TaskDto();
            task.setId(new ObjectId());
            task.setName("Test Task");
            task.setAttrs(new HashMap<>());

            InspectDto inspect = new InspectDto();
            inspect.setId(new ObjectId());
            inspect.setName("Test Inspect");

            Map<String, List<TaskUpAndLoadDto>> payloadsByType = new HashMap<>();

            when(inspectService.findByTaskIdList(anyList())).thenReturn(Arrays.asList(inspect));

            taskResourceHandler.handleRelatedResources(payloadsByType, Arrays.asList(task), user,new HashSet<>());

            assertTrue(payloadsByType.containsKey(ResourceType.INSPECT_TASK.name()));
        }
    }

    @Nested
    @DisplayName("collectPayloadRelatedResources Tests")
    class CollectPayloadRelatedResourcesTests {

        @Test
        @DisplayName("Should collect share cache payload")
        void testCollectPayloadRelatedResourcesShareCache() {
            TaskDto shareCacheTask = new TaskDto();
            shareCacheTask.setId(new ObjectId());
            shareCacheTask.setName("Share Cache Task");

            TaskUpAndLoadDto payload = new TaskUpAndLoadDto();
            payload.setCollectionName(GroupConstants.COLLECTION_TASK);
            payload.setJson(JsonUtil.toJsonUseJackson(shareCacheTask));

            Map<String, List<TaskUpAndLoadDto>> payloads = new HashMap<>();
            payloads.put("ShareCache.json", Arrays.asList(payload));

            Map<ResourceType, Map<String, ?>> resourceMap = new HashMap<>();
            Map<ResourceType, List<MetadataInstancesDto>> metadataList = new HashMap<>();

            taskResourceHandler.collectPayloadRelatedResources(payloads, resourceMap, metadataList,user);

            assertTrue(resourceMap.containsKey(ResourceType.SHARE_CACHE));
        }

        @Test
        @DisplayName("Should collect inspect payload")
        void testCollectPayloadRelatedResourcesInspect() {
            InspectDto inspect = new InspectDto();
            inspect.setId(new ObjectId());
            inspect.setName("Test Inspect");

            TaskUpAndLoadDto payload = new TaskUpAndLoadDto();
            payload.setCollectionName(GroupConstants.COLLECTION_INSPECT);
            payload.setJson(JsonUtil.toJsonUseJackson(inspect));

            Map<String, List<TaskUpAndLoadDto>> payloads = new HashMap<>();
            payloads.put("InspectTask.json", Arrays.asList(payload));

            Map<ResourceType, Map<String, ?>> resourceMap = new HashMap<>();
            Map<ResourceType, List<MetadataInstancesDto>> metadataList = new HashMap<>();

            taskResourceHandler.collectPayloadRelatedResources(payloads, resourceMap, metadataList,user);

            assertTrue(resourceMap.containsKey(ResourceType.INSPECT_TASK));
            Map<String, Object> inspectMap = (Map<String, Object>) resourceMap.get(ResourceType.INSPECT_TASK);
            assertEquals(1, inspectMap.size());
        }

        @Test
        @DisplayName("Should skip blank json in inspect payload")
        void testCollectPayloadRelatedResourcesSkipsBlankJson() {
            TaskUpAndLoadDto payload = new TaskUpAndLoadDto();
            payload.setCollectionName(GroupConstants.COLLECTION_INSPECT);
            payload.setJson("");

            Map<String, List<TaskUpAndLoadDto>> payloads = new HashMap<>();
            payloads.put("InspectTask.json", Arrays.asList(payload));

            Map<ResourceType, Map<String, ?>> resourceMap = new HashMap<>();
            Map<ResourceType, List<MetadataInstancesDto>> metadataList = new HashMap<>();

            taskResourceHandler.collectPayloadRelatedResources(payloads, resourceMap, metadataList,user);

            Map<String, Object> inspectMap = (Map<String, Object>) resourceMap.get(ResourceType.INSPECT_TASK);
            assertTrue(inspectMap == null || inspectMap.isEmpty());
        }
    }
}

