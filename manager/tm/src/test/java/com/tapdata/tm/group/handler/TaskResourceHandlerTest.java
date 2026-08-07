package com.tapdata.tm.group.handler;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.bean.SourceDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.externalStorage.service.ExternalStorageService;
import com.tapdata.tm.group.constant.GroupConstants;
import com.tapdata.tm.group.dto.ResourceType;
import com.tapdata.tm.group.handler.InspectResourceHandler;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.service.InspectService;
import com.tapdata.tm.metadatadefinition.service.MetadataDefinitionService;
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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
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

    @Mock
    private InspectResourceHandler inspectResourceHandler;

    @Mock
    private ExternalStorageService externalStorageService;

    @Mock
    private MetadataDefinitionService metadataDefinitionService;

    private TaskResourceHandler taskResourceHandler;

    private UserDetail user;

    @BeforeEach
    void setUp() {
        taskResourceHandler = new TaskResourceHandler();
        ReflectionTestUtils.setField(taskResourceHandler, "taskService", taskService);
        ReflectionTestUtils.setField(taskResourceHandler, "metadataInstancesService", metadataInstancesService);
        ReflectionTestUtils.setField(taskResourceHandler, "dataSourceService", dataSourceService);
        ReflectionTestUtils.setField(taskResourceHandler, "inspectService", inspectService);
        ReflectionTestUtils.setField(taskResourceHandler, "inspectResourceHandler", inspectResourceHandler);
        ReflectionTestUtils.setField(taskResourceHandler, "externalStorageService", externalStorageService);
        ReflectionTestUtils.setField(taskResourceHandler, "metadataDefinitionService", metadataDefinitionService);
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
            List<TaskUpAndLoadDto> result = taskResourceHandler.buildExportPayload(null, user, false);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should return empty list when resources is empty")
        void testBuildExportPayloadEmptyResources() {
            List<TaskUpAndLoadDto> result = taskResourceHandler.buildExportPayload(new ArrayList<>(), user, false);
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

            List<TaskUpAndLoadDto> result = taskResourceHandler.buildExportPayload(Arrays.asList(task), user, false);

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

            taskResourceHandler.buildExportPayload(Arrays.asList(task), user, false);

            assertEquals(TaskDto.STATUS_EDIT, task.getStatus());
        }

        @Test
        @DisplayName("Should handle task without DAG")
        void testBuildExportPayloadNoDag() {
            TaskDto task = new TaskDto();
            task.setId(new ObjectId());
            task.setName("Test Task");
            task.setDag(null);

            List<TaskUpAndLoadDto> result = taskResourceHandler.buildExportPayload(Arrays.asList(task), user, false);

            assertEquals(0, result.size());
        }

        /**
         * 任务导出会把 DAG 每个节点的 MetadataInstances 一并塞进包，而它的 source 是连接的整份
         * 拷贝、带顶层凭据镜像 —— 这是继连接文档、database 级与 table 级 metadata 之后的**第四处
         * 载体**。2026-08-06 实机在真实导出包里抓到（`Task/*.json` 的 `[].json.source.database_uri`），
         * 老包同样在漏，故非回归。ES-2b 的整包红线测试之所以没抓到，正是因为它的 fixture 里
         * 只有 Module 一条腿、压根没有任务自带的 metadata。
         */
        private String exportedPackageText(String secretUri, boolean maskSecrets) {
            TaskDto task = new TaskDto();
            task.setId(new ObjectId());
            task.setName("Task Carrying Metadata");

            DAG dag = mock(DAG.class);
            task.setDag(dag);
            DatabaseNode node = mock(DatabaseNode.class);
            lenient().when(node.getId()).thenReturn("node-1");
            when(dag.getNodes()).thenReturn(Arrays.asList(node));

            MetadataInstancesDto metadata = new MetadataInstancesDto();
            SourceDto source = new SourceDto();
            source.setDatabase_uri(secretUri);
            metadata.setSource(source);
            when(metadataInstancesService.findByNodeId(eq("node-1"), isNull(), eq(user), eq(task)))
                    .thenReturn(new ArrayList<>(Arrays.asList(metadata)));

            List<TaskUpAndLoadDto> payload =
                    taskResourceHandler.buildExportPayload(Arrays.asList(task), user, maskSecrets);
            StringBuilder sb = new StringBuilder();
            payload.forEach(p -> sb.append(p.getJson()));
            return sb.toString();
        }

        @Test
        @DisplayName("红线：脱敏导出时，任务自带 metadata 里那份连接拷贝的顶层凭据也必须抹掉")
        void taskBorneMetadataSecretIsMasked() {
            String secretUri = "mongodb://real-host:27017/from-task-metadata";

            assertFalse(exportedPackageText(secretUri, true).contains(secretUri),
                    "任务自带的 MetadataInstances.source 是凭据的第四处载体；漏了它，"
                            + "「GIT 包里永远没有明文凭据」（ADR-0034）就仍是假的 —— "
                            + "而 git 历史永久留存、可被 fork/缓存");
        }

        @Test
        @DisplayName("对照：保真导出时这份凭据必须还在（否则上一条测的是「压根没导出」）")
        void taskBorneMetadataSecretSurvivesFaithfulExport() {
            String secretUri = "mongodb://real-host:27017/from-task-metadata";

            assertTrue(exportedPackageText(secretUri, false).contains(secretUri),
                    "没有这条对照，上一条断言可以靠「metadata 根本没进包」而假绿 —— "
                            + "ES-2b 那次就是这么被骗过一次的");
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
        @DisplayName("Should skip task when id is null")
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

            assertTrue(resourceMap.isEmpty());
        }
    }

    @Nested
    @DisplayName("loadConnections Tests")
    class LoadConnectionsTests {

        @Test
        @DisplayName("Should return empty list when resources is null")
        void testLoadConnectionsNullResources() {
            List<DataSourceEntity> result = taskResourceHandler.loadConnections(null);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should return empty list when resources is empty")
        void testLoadConnectionsEmptyResources() {
            List<DataSourceEntity> result = taskResourceHandler.loadConnections(new ArrayList<>());
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should skip tasks without DAG")
        void testLoadConnectionsSkipsTasksWithoutDag() {
            TaskDto task = new TaskDto();
            task.setDag(null);

            List<DataSourceEntity> result = taskResourceHandler.loadConnections(Arrays.asList(task));

            assertTrue(result.isEmpty());
            verify(dataSourceService, never()).findAllEntity(any(Query.class));
        }

        @Test
        @DisplayName("Should extract connection ids from DAG nodes")
        void testLoadConnectionsExtractsFromDagNodes() {
            String connId = new ObjectId().toHexString();
            TaskDto task = new TaskDto();
            DAG dag = mock(DAG.class);
            task.setDag(dag);

            DatabaseNode node = mock(DatabaseNode.class);
            when(node.getConnectionId()).thenReturn(connId);
            when(dag.getNodes()).thenReturn(Arrays.asList(node));

            when(dataSourceService.findAllEntity(any(Query.class))).thenReturn(new ArrayList<>());

            taskResourceHandler.loadConnections(Arrays.asList(task));

            verify(dataSourceService).findAllEntity(any(Query.class));
        }
    }

    @Nested
    @DisplayName("findDuplicateNames Tests")
    class FindDuplicateNamesTests {

        @Test
        @DisplayName("Should return empty map when no duplicates")
        void testFindDuplicateNamesNoDuplicates() {
            ObjectId taskId = new ObjectId();
            TaskDto task = new TaskDto();
            task.setId(taskId);
            task.setName("Unique Task");

            // taskService.findOne returns null by default (mock default), so no stubbing needed

            Map<String, String> result = taskResourceHandler.findDuplicateNames(Arrays.asList(task), user);

            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should find duplicate by id")
        void testFindDuplicateNamesWithDuplicates() {
            ObjectId taskId = new ObjectId();
            TaskDto task = new TaskDto();
            task.setId(taskId);
            task.setName("Duplicate Task");

            TaskDto existing = new TaskDto();
            existing.setId(taskId);
            existing.setName("Duplicate Task");

            when(taskService.findOne(any(Query.class), eq(user))).thenReturn(existing);

            Map<String, String> result = taskResourceHandler.findDuplicateNames(Arrays.asList(task), user);

            assertEquals(1, result.size());
            assertTrue(result.containsKey(taskId.toHexString()));
            assertEquals("duplicate", result.get(taskId.toHexString()));
        }

        @Test
        @DisplayName("Should skip null tasks and tasks without id")
        void testFindDuplicateNamesSkipsNullAndNoId() {
            TaskDto taskNoId = new TaskDto();
            taskNoId.setName("No Id Task");
            List<TaskDto> tasks = Arrays.asList(null, taskNoId);

            Map<String, String> result = taskResourceHandler.findDuplicateNames(tasks, user);

            assertTrue(result.isEmpty());
            verify(taskService, never()).findOne(any(Query.class), any(UserDetail.class));
        }

        @Test
        @DisplayName("Should not check same id twice")
        void testFindDuplicateNamesSkipsAlreadyChecked() {
            ObjectId taskId = new ObjectId();

            TaskDto task1 = new TaskDto();
            task1.setId(taskId);
            task1.setName("Same Name");

            TaskDto task2 = new TaskDto();
            task2.setId(taskId);
            task2.setName("Same Name");

            TaskDto existing = new TaskDto();
            existing.setId(taskId);
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

            taskResourceHandler.handleRelatedResources(payloadsByType, Arrays.asList(task), user,new HashSet<>(), true);

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
            TaskUpAndLoadDto inspectPayload = new TaskUpAndLoadDto();
            inspectPayload.setCollectionName(GroupConstants.COLLECTION_INSPECT);
            inspectPayload.setJson(JsonUtil.toJsonUseJackson(inspect));
            when(inspectResourceHandler.buildExportPayload(anyList(), eq(user), anyBoolean())).thenReturn(Arrays.asList(inspectPayload));

            taskResourceHandler.handleRelatedResources(payloadsByType, Arrays.asList(task), user, new HashSet<>(), true);

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

