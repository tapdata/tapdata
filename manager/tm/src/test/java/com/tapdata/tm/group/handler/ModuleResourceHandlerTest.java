package com.tapdata.tm.group.handler;

import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.group.constant.GroupConstants;
import com.tapdata.tm.group.dto.ResourceType;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.task.bean.TaskUpAndLoadDto;
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

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class ModuleResourceHandlerTest {

    @Mock
    private ModulesService modulesService;

    @Mock
    private DataSourceService dataSourceService;

    @InjectMocks
    private ModuleResourceHandler moduleResourceHandler;

    private UserDetail user;

    @BeforeEach
    void setUp() {
        user = new UserDetail("userId123", "customerId", "testuser", "password", "customerType",
                "accessCode", false, false, false, false,
                Arrays.asList(new SimpleGrantedAuthority("role")));
    }

    @Nested
    @DisplayName("getResourceType Tests")
    class GetResourceTypeTests {

        @Test
        @DisplayName("Should return MODULE resource type")
        void testGetResourceType() {
            assertEquals(ResourceType.MODULE, moduleResourceHandler.getResourceType());
        }
    }

    @Nested
    @DisplayName("loadResources Tests")
    class LoadResourcesTests {

        @Test
        @DisplayName("Should return empty list when ids is null")
        void testLoadResourcesNullIds() {
            List<ModulesDto> result = moduleResourceHandler.loadResources(null, user);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should return empty list when ids is empty")
        void testLoadResourcesEmptyIds() {
            List<ModulesDto> result = moduleResourceHandler.loadResources(new ArrayList<>(), user);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should load modules by ids")
        void testLoadResourcesWithIds() {
            String id1 = new ObjectId().toHexString();
            String id2 = new ObjectId().toHexString();
            List<String> ids = Arrays.asList(id1, id2);

            ModulesDto module1 = new ModulesDto();
            module1.setId(new ObjectId(id1));
            module1.setName("Module 1");

            ModulesDto module2 = new ModulesDto();
            module2.setId(new ObjectId(id2));
            module2.setName("Module 2");

            when(modulesService.findAllDto(any(Query.class), eq(user))).thenReturn(Arrays.asList(module1, module2));

            List<ModulesDto> result = moduleResourceHandler.loadResources(ids, user);

            assertEquals(2, result.size());
            verify(modulesService).findAllDto(any(Query.class), eq(user));
        }

        @Test
        @DisplayName("Should filter null ids")
        void testLoadResourcesFilterNullIds() {
            String id1 = new ObjectId().toHexString();
            List<String> ids = Arrays.asList(id1, null);

            when(modulesService.findAllDto(any(Query.class), eq(user))).thenReturn(new ArrayList<>());

            moduleResourceHandler.loadResources(ids, user);

            verify(modulesService).findAllDto(any(Query.class), eq(user));
        }
    }

    @Nested
    @DisplayName("buildExportPayload Tests")
    class BuildExportPayloadTests {

        @Test
        @DisplayName("Should return empty list when resources is null")
        void testBuildExportPayloadNullResources() {
            List<TaskUpAndLoadDto> result = moduleResourceHandler.buildExportPayload(null, user);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should return empty list when resources is empty")
        void testBuildExportPayloadEmptyResources() {
            List<TaskUpAndLoadDto> result = moduleResourceHandler.buildExportPayload(new ArrayList<>(), user);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should build payload and clear sensitive fields")
        void testBuildExportPayloadClearsSensitiveFields() {
            ModulesDto module = new ModulesDto();
            module.setId(new ObjectId());
            module.setName("Test Module");
            module.setCustomId("customId");
            module.setLastUpdBy("lastUpdBy");
            module.setStatus("active");

            List<TaskUpAndLoadDto> result = moduleResourceHandler.buildExportPayload(Arrays.asList(module), user);

            assertEquals(1, result.size());
            assertEquals(GroupConstants.COLLECTION_MODULES, result.get(0).getCollectionName());

            // Verify fields cleared by implementation: customId, lastUpdBy, status
            assertNull(module.getCustomId());
            assertNull(module.getLastUpdBy());
            assertNull(module.getStatus());
        }

        @Test
        @DisplayName("Should set correct collection name")
        void testBuildExportPayloadCollectionName() {
            ModulesDto module = new ModulesDto();
            module.setId(new ObjectId());
            module.setName("Test Module");

            List<TaskUpAndLoadDto> result = moduleResourceHandler.buildExportPayload(Arrays.asList(module), user);

            assertEquals(GroupConstants.COLLECTION_MODULES, result.get(0).getCollectionName());
        }
    }

    @Nested
    @DisplayName("collectPayload Tests")
    class CollectPayloadTests {

        @Test
        @DisplayName("Should do nothing when payload is null")
        void testCollectPayloadNullPayload() {
            Map<String, ModulesDto> resourceMap = new HashMap<>();
            List<MetadataInstancesDto> metadataList = new ArrayList<>();

            moduleResourceHandler.collectPayload(null, resourceMap, metadataList);

            assertTrue(resourceMap.isEmpty());
            assertTrue(metadataList.isEmpty());
        }

        @Test
        @DisplayName("Should do nothing when payload is empty")
        void testCollectPayloadEmptyPayload() {
            Map<String, ModulesDto> resourceMap = new HashMap<>();
            List<MetadataInstancesDto> metadataList = new ArrayList<>();

            moduleResourceHandler.collectPayload(new ArrayList<>(), resourceMap, metadataList);

            assertTrue(resourceMap.isEmpty());
            assertTrue(metadataList.isEmpty());
        }

        @Test
        @DisplayName("Should collect module from payload")
        void testCollectPayloadModule() {
            ModulesDto module = new ModulesDto();
            module.setId(new ObjectId());
            module.setName("Test Module");

            TaskUpAndLoadDto payload = new TaskUpAndLoadDto();
            payload.setCollectionName(GroupConstants.COLLECTION_MODULES);
            payload.setJson(JsonUtil.toJsonUseJackson(module));

            Map<String, ModulesDto> resourceMap = new HashMap<>();
            List<MetadataInstancesDto> metadataList = new ArrayList<>();

            moduleResourceHandler.collectPayload(Arrays.asList(payload), resourceMap, metadataList);

            assertEquals(1, resourceMap.size());
            assertTrue(resourceMap.containsKey(module.getId().toHexString()));
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

            Map<String, ModulesDto> resourceMap = new HashMap<>();
            List<MetadataInstancesDto> metadataList = new ArrayList<>();

            moduleResourceHandler.collectPayload(Arrays.asList(payload), resourceMap, metadataList);

            assertEquals(1, metadataList.size());
        }

        @Test
        @DisplayName("Should skip items with blank json")
        void testCollectPayloadSkipsBlankJson() {
            TaskUpAndLoadDto payload = new TaskUpAndLoadDto();
            payload.setCollectionName(GroupConstants.COLLECTION_MODULES);
            payload.setJson("");

            Map<String, ModulesDto> resourceMap = new HashMap<>();
            List<MetadataInstancesDto> metadataList = new ArrayList<>();

            moduleResourceHandler.collectPayload(Arrays.asList(payload), resourceMap, metadataList);

            assertTrue(resourceMap.isEmpty());
        }

        @Test
        @DisplayName("Should skip module when id is null")
        void testCollectPayloadSkipsModuleWithNullId() {
            ModulesDto module = new ModulesDto();
            module.setId(null);
            module.setName("Test Module");

            TaskUpAndLoadDto payload = new TaskUpAndLoadDto();
            payload.setCollectionName(GroupConstants.COLLECTION_MODULES);
            payload.setJson(JsonUtil.toJsonUseJackson(module));

            Map<String, ModulesDto> resourceMap = new HashMap<>();
            List<MetadataInstancesDto> metadataList = new ArrayList<>();

            moduleResourceHandler.collectPayload(Arrays.asList(payload), resourceMap, metadataList);

            assertTrue(resourceMap.isEmpty());
        }
    }

    @Nested
    @DisplayName("loadConnections Tests")
    class LoadConnectionsTests {

        @Test
        @DisplayName("Should return empty list when resources is null")
        void testLoadConnectionsNullResources() {
            List<DataSourceEntity> result = moduleResourceHandler.loadConnections(null);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should return empty list when resources is empty")
        void testLoadConnectionsEmptyResources() {
            List<DataSourceEntity> result = moduleResourceHandler.loadConnections(new ArrayList<>());
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should load connections by connectionId")
        void testLoadConnectionsByConnectionId() {
            String connId = "aaaaaaaaaaaaaaaaaaaaaaaa";
            ModulesDto module = new ModulesDto();
            module.setConnectionId(connId);

            when(dataSourceService.findAllEntity(any(Query.class))).thenReturn(new ArrayList<>());

            moduleResourceHandler.loadConnections(Arrays.asList(module));

            verify(dataSourceService).findAllEntity(any(Query.class));
        }

        @Test
        @DisplayName("Should load connections by connection ObjectId")
        void testLoadConnectionsByConnectionObjectId() {
            ObjectId connId = new ObjectId();
            ModulesDto module = new ModulesDto();
            module.setConnectionId(null);
            module.setConnection(connId);

            when(dataSourceService.findAllEntity(any(Query.class))).thenReturn(new ArrayList<>());

            moduleResourceHandler.loadConnections(Arrays.asList(module));

            verify(dataSourceService).findAllEntity(any(Query.class));
        }

        @Test
        @DisplayName("Should skip modules without connection")
        void testLoadConnectionsSkipsModulesWithoutConnection() {
            ModulesDto module = new ModulesDto();
            module.setConnectionId(null);
            module.setConnection(null);

            List<DataSourceEntity> result = moduleResourceHandler.loadConnections(Arrays.asList(module));

            assertTrue(result.isEmpty());
            verify(dataSourceService, never()).findAllEntity(any(Query.class));
        }
    }

    @Nested
    @DisplayName("findDuplicateNames Tests")
    class FindDuplicateNamesTests {

        @Test
        @DisplayName("Should return empty map when no duplicates")
        void testFindDuplicateNamesNoDuplicates() {
            ObjectId moduleId = new ObjectId();
            ModulesDto module = new ModulesDto();
            module.setId(moduleId);
            module.setName("Unique Module");

            Map<String, String> result = moduleResourceHandler.findDuplicateNames(Arrays.asList(module), user);

            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should find duplicates by id")
        void testFindDuplicateNamesWithDuplicates() {
            ObjectId moduleId = new ObjectId();
            ModulesDto module = new ModulesDto();
            module.setId(moduleId);
            module.setName("Duplicate Module");

            ModulesDto existing = new ModulesDto();
            existing.setId(moduleId);
            existing.setName("Duplicate Module");

            when(modulesService.findOne(any(Query.class), eq(user))).thenReturn(existing);

            Map<String, String> result = moduleResourceHandler.findDuplicateNames(Arrays.asList(module), user);

            assertEquals(1, result.size());
            assertTrue(result.containsKey(moduleId.toHexString()));
            assertEquals("duplicate", result.get(moduleId.toHexString()));
        }

        @Test
        @DisplayName("Should skip null modules and modules with null id")
        void testFindDuplicateNamesSkipsNull() {
            ModulesDto moduleNoId = new ModulesDto();
            moduleNoId.setName("No Id");
            List<ModulesDto> modules = Arrays.asList(null, moduleNoId);

            Map<String, String> result = moduleResourceHandler.findDuplicateNames(modules, user);

            assertTrue(result.isEmpty());
            verify(modulesService, never()).findOne(any(Query.class), any(UserDetail.class));
        }

        @Test
        @DisplayName("Should skip modules with null id")
        void testFindDuplicateNamesSkipsNullId() {
            ModulesDto module = new ModulesDto();
            module.setName("Some Name");

            Map<String, String> result = moduleResourceHandler.findDuplicateNames(Arrays.asList(module), user);

            assertTrue(result.isEmpty());
            verify(modulesService, never()).findOne(any(Query.class), any(UserDetail.class));
        }

        @Test
        @DisplayName("Should not check same id twice")
        void testFindDuplicateNamesSkipsAlreadyChecked() {
            ObjectId moduleId = new ObjectId();
            ModulesDto module1 = new ModulesDto();
            module1.setId(moduleId);
            module1.setName("Same Name");

            ModulesDto module2 = new ModulesDto();
            module2.setId(moduleId);
            module2.setName("Same Name");

            ModulesDto existing = new ModulesDto();
            existing.setId(moduleId);
            existing.setName("Same Name");

            when(modulesService.findOne(any(Query.class), eq(user))).thenReturn(existing);

            Map<String, String> result = moduleResourceHandler.findDuplicateNames(Arrays.asList(module1, module2), user);

            assertEquals(1, result.size());
            verify(modulesService, times(1)).findOne(any(Query.class), eq(user));
        }
    }

    @Nested
    @DisplayName("resolveResourceName Tests")
    class ResolveResourceNameTests {

        @Test
        @DisplayName("Should return null when resourceMap is null")
        void testResolveResourceNameNullMap() {
            String result = moduleResourceHandler.resolveResourceName("id123", null);
            assertNull(result);
        }

        @Test
        @DisplayName("Should return null when resourceId is null")
        void testResolveResourceNameNullId() {
            Map<String, ModulesDto> resourceMap = new HashMap<>();
            String result = moduleResourceHandler.resolveResourceName(null, resourceMap);
            assertNull(result);
        }

        @Test
        @DisplayName("Should return null when resource not found")
        void testResolveResourceNameNotFound() {
            Map<String, ModulesDto> resourceMap = new HashMap<>();
            String result = moduleResourceHandler.resolveResourceName("nonexistent", resourceMap);
            assertNull(result);
        }

        @Test
        @DisplayName("Should return resource name when found")
        void testResolveResourceNameFound() {
            ModulesDto module = new ModulesDto();
            module.setName("Test Module");

            Map<String, ModulesDto> resourceMap = new HashMap<>();
            resourceMap.put("id123", module);

            String result = moduleResourceHandler.resolveResourceName("id123", resourceMap);

            assertEquals("Test Module", result);
        }
    }
}

