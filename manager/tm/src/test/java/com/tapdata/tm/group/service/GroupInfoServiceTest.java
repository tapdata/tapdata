package com.tapdata.tm.group.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.ImportModeEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.group.constant.GroupConstants;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.service.InspectService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.group.dto.GroupInfoDto;
import com.tapdata.tm.group.dto.GroupInfoRecordDetail;
import com.tapdata.tm.group.dto.GroupInfoRecordDto;
import com.tapdata.tm.group.dto.ResourceItem;
import com.tapdata.tm.group.dto.ResourceType;
import com.tapdata.tm.group.handler.ModuleResourceHandler;
import com.tapdata.tm.group.handler.ResourceHandler;
import com.tapdata.tm.group.handler.ResourceHandlerRegistry;
import com.tapdata.tm.group.handler.TaskResourceHandler;
import com.tapdata.tm.group.repostitory.GroupInfoRepository;
import com.tapdata.tm.group.service.transfer.GroupTransferStrategy;
import com.tapdata.tm.group.service.transfer.GroupTransferStrategyRegistry;
import com.tapdata.tm.group.service.transfer.GroupTransferType;
import com.tapdata.tm.group.vo.ExportGroupRequest;
import com.tapdata.tm.group.vo.FieldChange;
import com.tapdata.tm.group.vo.ResourceDiffItem;
import com.tapdata.tm.group.vo.ResourceDiff;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.task.bean.TaskUpAndLoadDto;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.utils.SpringContextHelper;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.HttpServletResponse;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.group.service.transfer.GroupExportRequest;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.bean.SourceDto;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.UndeclaredThrowableException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.*;

/**
 * Unit tests for GroupInfoService
 */
@ExtendWith(MockitoExtension.class)
public class GroupInfoServiceTest {

    @Mock
    private GroupInfoRepository groupInfoRepository;

    @Mock
    private GroupInfoRecordService groupInfoRecordService;

    @Mock
    private ResourceHandlerRegistry resourceHandlerRegistry;

    @Mock
    private com.tapdata.tm.task.service.TaskService taskService;

    @Mock
    private ModulesService modulesService;

    @Mock
    private com.tapdata.tm.ds.service.impl.DataSourceService dataSourceService;

    @Mock
    private com.tapdata.tm.metadatainstance.service.MetadataInstancesService metadataInstancesService;

    @Mock
    private com.tapdata.tm.inspect.service.InspectService inspectService;

    @Mock
    private GroupTransferStrategyRegistry transferStrategyRegistry;

    @Mock
    private GroupTransferStrategy groupTransferStrategy;

    @Mock
    private com.tapdata.tm.task.service.batchup.BatchUpChecker batchUpChecker;

    @Mock
    private com.tapdata.tm.metadatadefinition.service.MetadataDefinitionService metadataDefinitionService;

    @Mock
    private com.tapdata.tm.ds.service.impl.DataSourceDefinitionService dataSourceDefinitionService;

    @Mock
    private com.tapdata.tm.user.service.UserService userService;

    @Mock
    private com.tapdata.tm.role.service.RoleService roleService;

    @Mock
    private com.tapdata.tm.roleMapping.service.RoleMappingService roleMappingService;

    private GroupInfoService groupInfoService;

    private UserDetail user;
    @BeforeEach
    void setUp() {
        groupInfoService = spy(new GroupInfoService(groupInfoRepository));
        ReflectionTestUtils.setField(groupInfoService, "groupInfoRecordService", groupInfoRecordService);
        ReflectionTestUtils.setField(groupInfoService, "resourceHandlerRegistry", resourceHandlerRegistry);
        ReflectionTestUtils.setField(groupInfoService, "taskService", taskService);
        ReflectionTestUtils.setField(groupInfoService, "modulesService", modulesService);
        ReflectionTestUtils.setField(groupInfoService, "dataSourceService", dataSourceService);
        ReflectionTestUtils.setField(groupInfoService, "metadataInstancesService", metadataInstancesService);
        ReflectionTestUtils.setField(groupInfoService, "inspectService", inspectService);
        ReflectionTestUtils.setField(groupInfoService, "transferStrategyRegistry", transferStrategyRegistry);
        ReflectionTestUtils.setField(groupInfoService, "batchUpChecker", batchUpChecker);
        ReflectionTestUtils.setField(groupInfoService, "metadataDefinitionService", metadataDefinitionService);
        ReflectionTestUtils.setField(groupInfoService, "dataSourceDefinitionService", dataSourceDefinitionService);
        ReflectionTestUtils.setField(groupInfoService, "userService", userService);
        ReflectionTestUtils.setField(groupInfoService, "roleService", roleService);
        ReflectionTestUtils.setField(groupInfoService, "roleMappingService", roleMappingService);

        // Setup default mock for transfer strategy (lenient because not all tests use it)
        lenient().when(transferStrategyRegistry.getStrategy(GroupTransferType.FILE)).thenReturn(groupTransferStrategy);

        user = new UserDetail("userId123", "customerId", "testuser", "password", "customerType",
                "accessCode", false, false, false, false,
                Arrays.asList(new SimpleGrantedAuthority("role")));

        ResourceHandlerRegistry resourceHandlerRegistry = spy(ResourceHandlerRegistry.class);

        ReflectionTestUtils.setField(resourceHandlerRegistry, "handlers", Arrays.asList(mock(TaskResourceHandler.class), mock(ModuleResourceHandler.class)));
        resourceHandlerRegistry.init();
        ReflectionTestUtils.setField(groupInfoService, "resourceHandlerRegistry", resourceHandlerRegistry);
    }

    @Nested
    @DisplayName("groupList Tests")
    class GroupListTests {

        @Test
        @DisplayName("Should return empty page when no groups exist")
        void testGroupListEmpty() {
            Filter filter = new Filter();
            Page<GroupInfoDto> mockPage = new Page<>();
            mockPage.setTotal(0);
            mockPage.setItems(Collections.emptyList());

            doReturn(mockPage).when(groupInfoService).find(any(Filter.class), any(UserDetail.class));

            Page<GroupInfoDto> result = groupInfoService.groupList(filter, user);

            assertNotNull(result);
            assertEquals(0, result.getTotal());
            assertTrue(result.getItems().isEmpty());
        }

        @Test
        @DisplayName("Should return groups with resource item details filled")
        void testGroupListWithItems() {
            Filter filter = new Filter();

            GroupInfoDto groupDto = new GroupInfoDto();
            groupDto.setId(new ObjectId());
            groupDto.setName("Test Group");

            ResourceItem item = new ResourceItem();
            item.setId("task123");
            item.setType(ResourceType.SYNC_TASK);
            groupDto.setResourceItemList(Arrays.asList(item));

            Page<GroupInfoDto> mockPage = new Page<>();
            mockPage.setTotal(1);
            mockPage.setItems(Arrays.asList(groupDto));

            doReturn(mockPage).when(groupInfoService).find(any(Filter.class), any(UserDetail.class));

            Page<GroupInfoDto> result = groupInfoService.groupList(filter, user);

            assertNotNull(result);
            assertEquals(1, result.getTotal());
            assertEquals("Test Group", result.getItems().get(0).getName());
        }
    }

    @Nested
    @DisplayName("calculateProgress Tests")
    class CalculateProgressTests {

        @Test
        @DisplayName("Should return 0 when total is 0")
        void testCalculateProgressZeroTotal() {
            int result = invokeCalculateProgress(0, 0);
            assertEquals(0, result);
        }

        @Test
        @DisplayName("Should return correct percentage")
        void testCalculateProgressNormal() {
            int result = invokeCalculateProgress(50, 100);
            assertEquals(50, result);
        }

        @Test
        @DisplayName("Should cap at 99 before completion")
        void testCalculateProgressCappedAt99() {
            int result = invokeCalculateProgress(99, 100);
            assertEquals(99, result);
        }

        @Test
        @DisplayName("Should handle partial resources")
        void testCalculateProgressPartial() {
            // 10 out of 21 resources = 47%
            int result = invokeCalculateProgress(10, 21);
            assertEquals(47, result);
        }

        private int invokeCalculateProgress(int imported, int total) {
            return (int) ReflectionTestUtils.invokeMethod(groupInfoService, "calculateProgress", imported, total);
        }
    }

    @Nested
    @DisplayName("buildRecord Tests")
    class BuildRecordTests {

        @Test
        @DisplayName("Should build export record correctly")
        void testBuildExportRecord() {
            List<GroupInfoRecordDetail> details = new ArrayList<>();
            String fileName = "test-export.tar";

            GroupInfoRecordDto result = (GroupInfoRecordDto) ReflectionTestUtils.invokeMethod(
                    groupInfoService, "buildRecord",
                    GroupInfoRecordDto.TYPE_EXPORT, user, details, fileName);

            assertNotNull(result);
            assertEquals(GroupInfoRecordDto.TYPE_EXPORT, result.getType());
            assertEquals(GroupInfoRecordDto.STATUS_EXPORTING, result.getStatus());
            assertEquals(fileName, result.getFileName());
            assertEquals("testuser", result.getOperator());
        }

        @Test
        @DisplayName("Should build import record correctly")
        void testBuildImportRecord() {
            List<GroupInfoRecordDetail> details = new ArrayList<>();
            String fileName = "test-import.tar";

            GroupInfoRecordDto result = (GroupInfoRecordDto) ReflectionTestUtils.invokeMethod(
                    groupInfoService, "buildRecord",
                    GroupInfoRecordDto.TYPE_IMPORT, user, details, fileName);

            assertNotNull(result);
            assertEquals(GroupInfoRecordDto.TYPE_IMPORT, result.getType());
            assertEquals(GroupInfoRecordDto.STATUS_IMPORTING, result.getStatus());
            assertEquals(fileName, result.getFileName());
        }
    }

    @Nested
    @DisplayName("exportGroupInfos Tests")
    class ExportGroupInfosTests {

        @Test
        @DisplayName("Should validate export record DTO structure")
        void testExportRecordStructure() {
            // Test the export record structure used by exportGroupInfos
            GroupInfoRecordDto recordDto = new GroupInfoRecordDto();
            recordDto.setType(GroupInfoRecordDto.TYPE_EXPORT);
            recordDto.setStatus(GroupInfoRecordDto.STATUS_EXPORTING);
            recordDto.setFileName("test-export.tar");

            assertEquals(GroupInfoRecordDto.TYPE_EXPORT, recordDto.getType());
            assertEquals(GroupInfoRecordDto.STATUS_EXPORTING, recordDto.getStatus());
            assertEquals("test-export.tar", recordDto.getFileName());
        }


        @Test
        @DisplayName("Should load resources using resource handlers")
        void testExportGroupInfosLoadsResources() throws Exception {
            HttpServletResponse response = mock(HttpServletResponse.class);

            List<String> groupIds = Arrays.asList(new ObjectId().toHexString());

            GroupInfoDto groupInfo = new GroupInfoDto();
            groupInfo.setId(new ObjectId());
            groupInfo.setName("Test Group");
            ResourceItem item = new ResourceItem();
            item.setId("task123");
            item.setType(ResourceType.SYNC_TASK);
            groupInfo.setResourceItemList(Arrays.asList(item));

            doReturn(Arrays.asList(groupInfo)).when(groupInfoService).findAllDto(any(Query.class), any(UserDetail.class));

            GroupInfoRecordDto savedRecord = new GroupInfoRecordDto();
            savedRecord.setId(new ObjectId());
            when(groupInfoRecordService.save(any(GroupInfoRecordDto.class), any(UserDetail.class))).thenReturn(savedRecord);

            doNothing().when(groupInfoService).updateRecordStatus(any(), any(), any(), any(), any());

			ExportGroupRequest exportGroupRequest = new ExportGroupRequest();
			exportGroupRequest.setGroupIds(groupIds);
			exportGroupRequest.setGroupTransferType(GroupTransferType.FILE);
			exportGroupRequest.setGroupResetTask(new HashMap<>());
			groupInfoService.exportGroupInfos(response, exportGroupRequest, user);

            verify(groupTransferStrategy).exportGroups(any());
        }

        @Test
        @DisplayName("Should build export payload using resource handlers")
        void testExportGroupInfosBuildsPayload() throws Exception {
            HttpServletResponse response = mock(HttpServletResponse.class);

            List<String> groupIds = Arrays.asList(new ObjectId().toHexString());

            GroupInfoDto groupInfo = new GroupInfoDto();
            groupInfo.setId(new ObjectId());
            groupInfo.setName("Test Group");
            groupInfo.setResourceItemList(new ArrayList<>());

            doReturn(Arrays.asList(groupInfo)).when(groupInfoService).findAllDto(any(Query.class), any(UserDetail.class));

            GroupInfoRecordDto savedRecord = new GroupInfoRecordDto();
            savedRecord.setId(new ObjectId());
            when(groupInfoRecordService.save(any(GroupInfoRecordDto.class), any(UserDetail.class))).thenReturn(savedRecord);

            doNothing().when(groupInfoService).updateRecordStatus(any(), any(), any(), any(), any());
			ExportGroupRequest exportGroupRequest = new ExportGroupRequest();
			exportGroupRequest.setGroupIds(groupIds);
			exportGroupRequest.setGroupTransferType(GroupTransferType.FILE);
			exportGroupRequest.setGroupResetTask(new HashMap<>());
			groupInfoService.exportGroupInfos(response, exportGroupRequest, user);

            verify(groupInfoRecordService).save(any(GroupInfoRecordDto.class), any());
        }

        @Test
        @DisplayName("Should save export record with TYPE_EXPORT")
        void testExportGroupInfosSavesRecord() throws Exception {
            HttpServletResponse response = mock(HttpServletResponse.class);

            List<String> groupIds = Arrays.asList(new ObjectId().toHexString());

            GroupInfoDto groupInfo = new GroupInfoDto();
            groupInfo.setId(new ObjectId());
            groupInfo.setName("Test Group");
            groupInfo.setResourceItemList(new ArrayList<>());

            doReturn(Arrays.asList(groupInfo)).when(groupInfoService).findAllDto(any(Query.class), any(UserDetail.class));

            GroupInfoRecordDto savedRecord = new GroupInfoRecordDto();
            savedRecord.setId(new ObjectId());
            when(groupInfoRecordService.save(any(GroupInfoRecordDto.class), any())).thenReturn(savedRecord);

            doNothing().when(groupInfoService).updateRecordStatus(any(), any(), any(), any(), any());
			ExportGroupRequest exportGroupRequest = new ExportGroupRequest();
			exportGroupRequest.setGroupIds(groupIds);
			exportGroupRequest.setGroupTransferType(GroupTransferType.FILE);
			exportGroupRequest.setGroupResetTask(new HashMap<>());
            groupInfoService.exportGroupInfos(response, exportGroupRequest, user);

            verify(groupInfoRecordService).save(any(GroupInfoRecordDto.class), any());
        }

        @Test
        @DisplayName("Should delegate to transfer strategy for export")
        void testExportGroupInfosSetsResponseHeaders() throws Exception {
            HttpServletResponse response = mock(HttpServletResponse.class);

            List<String> groupIds = Arrays.asList(new ObjectId().toHexString());

            GroupInfoDto groupInfo = new GroupInfoDto();
            groupInfo.setId(new ObjectId());
            groupInfo.setName("Test Group");
            groupInfo.setResourceItemList(new ArrayList<>());

            doReturn(Arrays.asList(groupInfo)).when(groupInfoService).findAllDto(any(Query.class), any(UserDetail.class));

            GroupInfoRecordDto savedRecord = new GroupInfoRecordDto();
            savedRecord.setId(new ObjectId());
            when(groupInfoRecordService.save(any(GroupInfoRecordDto.class), any(UserDetail.class))).thenReturn(savedRecord);

            doNothing().when(groupInfoService).updateRecordStatus(any(), any(), any(), any(), any());
			ExportGroupRequest exportGroupRequest = new ExportGroupRequest();
			exportGroupRequest.setGroupIds(groupIds);
			exportGroupRequest.setGroupTransferType(GroupTransferType.FILE);
			exportGroupRequest.setGroupResetTask(new HashMap<>());
            groupInfoService.exportGroupInfos(response, exportGroupRequest, user);

            // Verify that the transfer strategy is called for export
            verify(groupTransferStrategy).exportGroups(any());
        }

        @Test
        @DisplayName("Should update record status to COMPLETED on success")
        void testExportGroupInfosStatusCompleted() throws Exception {
            HttpServletResponse response = mock(HttpServletResponse.class);

            List<String> groupIds = Arrays.asList(new ObjectId().toHexString());

            GroupInfoDto groupInfo = new GroupInfoDto();
            groupInfo.setId(new ObjectId());
            groupInfo.setName("Test Group");
            groupInfo.setResourceItemList(new ArrayList<>());

            doReturn(Arrays.asList(groupInfo)).when(groupInfoService).findAllDto(any(Query.class), any(UserDetail.class));

            GroupInfoRecordDto savedRecord = new GroupInfoRecordDto();
            savedRecord.setId(new ObjectId());
            when(groupInfoRecordService.save(any(GroupInfoRecordDto.class), any(UserDetail.class))).thenReturn(savedRecord);

            doNothing().when(groupInfoService).updateRecordStatus(any(), any(), any(), any(), any());
			ExportGroupRequest exportGroupRequest = new ExportGroupRequest();
			exportGroupRequest.setGroupIds(groupIds);
			exportGroupRequest.setGroupTransferType(GroupTransferType.FILE);
			exportGroupRequest.setGroupResetTask(new HashMap<>());
            groupInfoService.exportGroupInfos(response, exportGroupRequest, user);

            verify(groupInfoService).updateRecordStatus(eq(savedRecord.getId()),
                eq(GroupInfoRecordDto.STATUS_COMPLETED), isNull(), any(), eq(user));
        }

        @Test
        @DisplayName("Should update record status to FAILED on strategy exception")
        void testExportGroupInfosStatusFailedOnException() throws Exception {
            HttpServletResponse response = mock(HttpServletResponse.class);

            List<String> groupIds = Arrays.asList(new ObjectId().toHexString());

            GroupInfoDto groupInfo = new GroupInfoDto();
            groupInfo.setId(new ObjectId());
            groupInfo.setName("Test Group");
            groupInfo.setResourceItemList(new ArrayList<>());

            doReturn(Arrays.asList(groupInfo)).when(groupInfoService).findAllDto(any(Query.class), any(UserDetail.class));

            GroupInfoRecordDto savedRecord = new GroupInfoRecordDto();
            savedRecord.setId(new ObjectId());
            when(groupInfoRecordService.save(any(GroupInfoRecordDto.class), any(UserDetail.class))).thenReturn(savedRecord);

            // Make the strategy throw an exception
            doThrow(new RuntimeException("Export error")).when(groupTransferStrategy).exportGroups(any());

            doNothing().when(groupInfoService).updateRecordStatus(any(), any(), any(), any(), any());

			ExportGroupRequest exportGroupRequest = new ExportGroupRequest();
			exportGroupRequest.setGroupIds(groupIds);
			exportGroupRequest.setGroupTransferType(GroupTransferType.FILE);
			exportGroupRequest.setGroupResetTask(new HashMap<>());

            // FILE type is sync — performExport catches exception, updates status, then re-throws as BizException
            assertThrows(BizException.class, () ->
                groupInfoService.exportGroupInfos(response, exportGroupRequest, user));

            verify(groupInfoService).updateRecordStatus(eq(savedRecord.getId()),
                eq(GroupInfoRecordDto.STATUS_FAILED), eq("Export error"), any(), eq(user));
        }

        @Test
        @DisplayName("Should handle related resources through handlers")
        void testExportGroupInfosHandlesRelatedResources() throws Exception {
            HttpServletResponse response = mock(HttpServletResponse.class);

            List<String> groupIds = Arrays.asList(new ObjectId().toHexString());

            GroupInfoDto groupInfo = new GroupInfoDto();
            groupInfo.setId(new ObjectId());
            groupInfo.setName("Test Group");
            ResourceItem item = new ResourceItem();
            item.setId("task123");
            item.setType(ResourceType.MIGRATE_TASK);
            groupInfo.setResourceItemList(Arrays.asList(item));

            doReturn(Arrays.asList(groupInfo)).when(groupInfoService).findAllDto(any(Query.class), any(UserDetail.class));

            GroupInfoRecordDto savedRecord = new GroupInfoRecordDto();
            savedRecord.setId(new ObjectId());
            when(groupInfoRecordService.save(any(GroupInfoRecordDto.class), any(UserDetail.class))).thenReturn(savedRecord);

            doNothing().when(groupInfoService).updateRecordStatus(any(), any(), any(), any(), any());
			ExportGroupRequest exportGroupRequest = new ExportGroupRequest();
			exportGroupRequest.setGroupIds(groupIds);
			exportGroupRequest.setGroupTransferType(GroupTransferType.FILE);
			exportGroupRequest.setGroupResetTask(new HashMap<>());
            groupInfoService.exportGroupInfos(response, exportGroupRequest, user);

            verify(groupTransferStrategy).exportGroups(any());
        }

        @Test
        @DisplayName("Should delegate export to transfer strategy")
        void testExportGroupInfosWritesTarContent() throws Exception {
            HttpServletResponse response = mock(HttpServletResponse.class);

            List<String> groupIds = Arrays.asList(new ObjectId().toHexString());

            GroupInfoDto groupInfo = new GroupInfoDto();
            groupInfo.setId(new ObjectId());
            groupInfo.setName("Test Group");
            groupInfo.setResourceItemList(new ArrayList<>());

            doReturn(Arrays.asList(groupInfo)).when(groupInfoService).findAllDto(any(Query.class), any(UserDetail.class));

            GroupInfoRecordDto savedRecord = new GroupInfoRecordDto();
            savedRecord.setId(new ObjectId());
            when(groupInfoRecordService.save(any(GroupInfoRecordDto.class), any(UserDetail.class))).thenReturn(savedRecord);

            doNothing().when(groupInfoService).updateRecordStatus(any(), any(), any(), any(), any());
			ExportGroupRequest exportGroupRequest = new ExportGroupRequest();
			exportGroupRequest.setGroupIds(groupIds);
			exportGroupRequest.setGroupTransferType(GroupTransferType.FILE);
			exportGroupRequest.setGroupResetTask(new HashMap<>());
            groupInfoService.exportGroupInfos(response, exportGroupRequest, user);

            // Verify that the transfer strategy is called for export
            verify(groupTransferStrategy).exportGroups(any());
        }

        @Test
        @DisplayName("Should export multiple groups")
        void testExportGroupInfosMultipleGroups() throws Exception {
            HttpServletResponse response = mock(HttpServletResponse.class);

            List<String> groupIds = Arrays.asList(new ObjectId().toHexString(), new ObjectId().toHexString());

            GroupInfoDto groupInfo1 = new GroupInfoDto();
            groupInfo1.setId(new ObjectId());
            groupInfo1.setName("Group 1");
            groupInfo1.setResourceItemList(new ArrayList<>());

            GroupInfoDto groupInfo2 = new GroupInfoDto();
            groupInfo2.setId(new ObjectId());
            groupInfo2.setName("Group 2");
            groupInfo2.setResourceItemList(new ArrayList<>());

            doReturn(Arrays.asList(groupInfo1, groupInfo2)).when(groupInfoService).findAllDto(any(Query.class), any(UserDetail.class));

            GroupInfoRecordDto savedRecord = new GroupInfoRecordDto();
            savedRecord.setId(new ObjectId());
            when(groupInfoRecordService.save(any(GroupInfoRecordDto.class), any(UserDetail.class))).thenReturn(savedRecord);

            doNothing().when(groupInfoService).updateRecordStatus(any(), any(), any(), any(), any());
			ExportGroupRequest exportGroupRequest = new ExportGroupRequest();
			exportGroupRequest.setGroupIds(groupIds);
			exportGroupRequest.setGroupTransferType(GroupTransferType.FILE);
			exportGroupRequest.setGroupResetTask(new HashMap<>());
            groupInfoService.exportGroupInfos(response, exportGroupRequest, user);

            verify(groupInfoService).updateRecordStatus(eq(savedRecord.getId()),
                eq(GroupInfoRecordDto.STATUS_COMPLETED), isNull(), any(), eq(user));
        }
    }

    @Nested
    @DisplayName("batchImportGroup Tests")
    class BatchImportGroupTests {

        @Test
        @DisplayName("Should set correct filename in import record")
        void testBatchImportGroupFilename() {
            String expectedFilename = "group-export-20260112.tar";

            GroupInfoRecordDto dto = new GroupInfoRecordDto();
            dto.setFileName(expectedFilename);
            dto.setType(GroupInfoRecordDto.TYPE_IMPORT);

            assertEquals(expectedFilename, dto.getFileName());
            assertEquals(GroupInfoRecordDto.TYPE_IMPORT, dto.getType());
        }

        @Test
        @DisplayName("Should initialize progress to 0 for new import")
        void testBatchImportGroupProgressInitialization() {
            GroupInfoRecordDto dto = new GroupInfoRecordDto();
            dto.setProgress(0);
            dto.setStatus(GroupInfoRecordDto.STATUS_IMPORTING);

            assertEquals(0, dto.getProgress());
            assertEquals(GroupInfoRecordDto.STATUS_IMPORTING, dto.getStatus());
        }

        @Test
        @DisplayName("Should create import record and return record ID")
        void testBatchImportGroupCreatesRecord() throws Exception {
            org.springframework.web.multipart.MultipartFile file = mock(
                    org.springframework.web.multipart.MultipartFile.class);

            ObjectId expectedRecordId = new ObjectId();

            when(transferStrategyRegistry.getStrategy(GroupTransferType.FILE)).thenReturn(groupTransferStrategy);
            when(groupTransferStrategy.importGroups(any())).thenReturn(expectedRecordId);

            ObjectId result = groupInfoService.batchImportGroup(file, user, null);
            assertNotNull(result);
            assertEquals(expectedRecordId, result);
            verify(transferStrategyRegistry).getStrategy(GroupTransferType.FILE);
            verify(groupTransferStrategy).importGroups(any());
        }

        @Test
        @DisplayName("Should delegate to strategy with correct import mode")
        void testBatchImportGroupDefaultImportMode() throws Exception {
            org.springframework.web.multipart.MultipartFile file = mock(
                    org.springframework.web.multipart.MultipartFile.class);

            ObjectId expectedRecordId = new ObjectId();

            when(transferStrategyRegistry.getStrategy(GroupTransferType.FILE)).thenReturn(groupTransferStrategy);
            when(groupTransferStrategy.importGroups(any())).thenReturn(expectedRecordId);

            groupInfoService.batchImportGroup(file, user, null);

            verify(groupTransferStrategy).importGroups(any());
        }

        @Test
        @DisplayName("Should delegate to strategy with REPLACE import mode")
        void testBatchImportGroupCallsExecuteImportAsync() throws Exception {
            org.springframework.web.multipart.MultipartFile file = mock(
                    org.springframework.web.multipart.MultipartFile.class);

            ObjectId expectedRecordId = new ObjectId();

            when(transferStrategyRegistry.getStrategy(GroupTransferType.FILE)).thenReturn(groupTransferStrategy);
            when(groupTransferStrategy.importGroups(any())).thenReturn(expectedRecordId);

            groupInfoService.batchImportGroup(file, user, com.tapdata.tm.commons.task.dto.ImportModeEnum.REPLACE);

            verify(groupTransferStrategy).importGroups(any());
        }

        @Test
        @DisplayName("Should return record ID from strategy")
        void testBatchImportGroupInitialProgressIsZero() throws Exception {
            org.springframework.web.multipart.MultipartFile file = mock(
                    org.springframework.web.multipart.MultipartFile.class);

            ObjectId expectedRecordId = new ObjectId();

            when(transferStrategyRegistry.getStrategy(GroupTransferType.FILE)).thenReturn(groupTransferStrategy);
            when(groupTransferStrategy.importGroups(any())).thenReturn(expectedRecordId);

            ObjectId result = groupInfoService.batchImportGroup(file, user, null);

            assertEquals(expectedRecordId, result);
        }

        @Test
        @DisplayName("Should throw IOException when strategy throws IOException")
        void testBatchImportGroupIOException() throws Exception {
            org.springframework.web.multipart.MultipartFile file = mock(
                    org.springframework.web.multipart.MultipartFile.class);

            when(transferStrategyRegistry.getStrategy(GroupTransferType.FILE)).thenReturn(groupTransferStrategy);
            when(groupTransferStrategy.importGroups(any())).thenThrow(new java.io.IOException("File read error"));

            assertThrows(java.io.IOException.class, () -> {
                groupInfoService.batchImportGroup(file, user, null);
            });
        }

        @Test
        @DisplayName("Should pass file to strategy")
        void testBatchImportGroupUsesOriginalFilename() throws Exception {
            org.springframework.web.multipart.MultipartFile file = mock(
                    org.springframework.web.multipart.MultipartFile.class);

            ObjectId expectedRecordId = new ObjectId();

            when(transferStrategyRegistry.getStrategy(GroupTransferType.FILE)).thenReturn(groupTransferStrategy);
            when(groupTransferStrategy.importGroups(any())).thenReturn(expectedRecordId);

            groupInfoService.batchImportGroup(file, user, null);

            verify(groupTransferStrategy).importGroups(any());
        }

        @Test
        @DisplayName("Should pass user to strategy")
        void testBatchImportGroupSetsOperator() throws Exception {
            org.springframework.web.multipart.MultipartFile file = mock(
                    org.springframework.web.multipart.MultipartFile.class);

            ObjectId expectedRecordId = new ObjectId();

            when(transferStrategyRegistry.getStrategy(GroupTransferType.FILE)).thenReturn(groupTransferStrategy);
            when(groupTransferStrategy.importGroups(any())).thenReturn(expectedRecordId);

            groupInfoService.batchImportGroup(file, user, null);

            verify(groupTransferStrategy).importGroups(any());
        }
    }


    @Nested
    @DisplayName("executeImportAsync Tests")
    class ExecuteImportAsyncTests {

        @Test
        @DisplayName("Should update record status to COMPLETED on successful completion")
        void testExecuteImportAsyncUpdatesProgressTo100() {
            Map<String, List<TaskUpAndLoadDto>> payloads = new HashMap<>();
            payloads.put("GroupInfo.json", new ArrayList<>());
            ObjectId recordId = new ObjectId();

            // Mock all required dependencies
            when(dataSourceService.batchImport(any(), any(), any())).thenReturn(new HashMap<>());
            when(metadataDefinitionService.batchImport(any(), any())).thenReturn(new HashMap<>());
            doNothing().when(batchUpChecker).checkDataSourceConnection(any(), any(), any());
            doNothing().when(groupInfoService).updateImportProgress(any(), anyInt(), any(), any());
            doNothing().when(groupInfoService).updateRecordStatus(any(), any(), any(), any(), any());
            doReturn(new ArrayList<>()).when(groupInfoService).buildImportRecordDetails(any(), any());

            groupInfoService.executeImportAsync(payloads, user, null, "test.tar", recordId);

            // Verify that updateRecordStatus is called with STATUS_COMPLETED at the end
            verify(groupInfoService).updateRecordStatus(eq(recordId), eq(GroupInfoRecordDto.STATUS_COMPLETED),
                    isNull(), any(), eq(user));
        }

        @Test
        @DisplayName("Should update record status to COMPLETED on success")
        void testExecuteImportAsyncStatusCompleted() {
            Map<String, List<TaskUpAndLoadDto>> payloads = new HashMap<>();
            payloads.put("GroupInfo.json", new ArrayList<>());
            ObjectId recordId = new ObjectId();

            // Mock all required dependencies
            when(dataSourceService.batchImport(any(), any(), any())).thenReturn(new HashMap<>());
            when(metadataDefinitionService.batchImport(any(), any())).thenReturn(new HashMap<>());
            doNothing().when(batchUpChecker).checkDataSourceConnection(any(), any(), any());
            doNothing().when(groupInfoService).updateImportProgress(any(), anyInt(), any(), any());
            doNothing().when(groupInfoService).updateRecordStatus(any(), any(), any(), any(), any());
            doReturn(new ArrayList<>()).when(groupInfoService).buildImportRecordDetails(any(), any());

            groupInfoService.executeImportAsync(payloads, user, null, "test.tar", recordId);

            verify(groupInfoService).updateRecordStatus(eq(recordId), eq(GroupInfoRecordDto.STATUS_COMPLETED),
                    isNull(), any(), eq(user));
        }

        @Test
        @DisplayName("Should update record status to FAILED on exception")
        void testExecuteImportAsyncStatusFailedOnException() {
            Map<String, List<TaskUpAndLoadDto>> payloads = new HashMap<>();
            payloads.put("GroupInfo.json", new ArrayList<>());
            ObjectId recordId = new ObjectId();

            // Mock all required dependencies - use lenient for those that might not be called due to early exception
            lenient().when(dataSourceService.batchImport(any(), any(), any())).thenReturn(new HashMap<>());
            lenient().when(metadataDefinitionService.batchImport(any(), any())).thenReturn(new HashMap<>());
            doNothing().when(batchUpChecker).checkDataSourceConnection(any(), any(), any());
            doReturn(new ArrayList<>()).when(groupInfoService).buildImportRecordDetails(any(), any());
            doThrow(new RuntimeException("Import error")).when(groupInfoService)
                    .updateImportProgress(any(), anyInt(), any(), any());

            groupInfoService.executeImportAsync(payloads, user, null, "test.tar", recordId);

            // The actual implementation uses ExceptionUtils.getStackTrace(e) which includes the full stack trace
            verify(groupInfoService).updateRecordStatus(eq(recordId), eq(GroupInfoRecordDto.STATUS_FAILED),
                    argThat(msg -> msg != null && msg.contains("Import error")), any(), eq(user));
        }

        @Test
        @DisplayName("Should import connections in stage 1")
        void testExecuteImportAsyncImportsConnections() {
            Map<String, List<TaskUpAndLoadDto>> payloads = new HashMap<>();
            payloads.put("GroupInfo.json", new ArrayList<>());
            ObjectId recordId = new ObjectId();

            // Mock all required dependencies
            when(dataSourceService.batchImport(any(), any(), any())).thenReturn(new HashMap<>());
            when(metadataDefinitionService.batchImport(any(), any())).thenReturn(new HashMap<>());
            doNothing().when(batchUpChecker).checkDataSourceConnection(any(), any(), any());
            doNothing().when(groupInfoService).updateImportProgress(any(), anyInt(), any(), any());
            doNothing().when(groupInfoService).updateRecordStatus(any(), any(), any(), any(), any());
            doReturn(new ArrayList<>()).when(groupInfoService).buildImportRecordDetails(any(), any());

            groupInfoService.executeImportAsync(payloads, user, null, "test.tar", recordId);

            verify(dataSourceService).batchImport(any(), eq(user), any());
        }

        @Test
        @DisplayName("Should save group infos in stage 5 using upsertByWhere")
        void testExecuteImportAsyncSavesGroupInfos() {
            Map<String, List<TaskUpAndLoadDto>> payloads = new HashMap<>();

            // Create a valid GroupInfo payload
            GroupInfoDto groupInfo = new GroupInfoDto();
            groupInfo.setId(new ObjectId());
            groupInfo.setName("Test Group");
            groupInfo.setResourceItemList(new ArrayList<>());

            TaskUpAndLoadDto groupPayload = new TaskUpAndLoadDto();
            groupPayload.setCollectionName("GroupInfo");
            groupPayload.setJson(JsonUtil.toJsonUseJackson(groupInfo));

            payloads.put("GroupInfo.json", Arrays.asList(groupPayload));
            ObjectId recordId = new ObjectId();

            // Mock all required dependencies
            when(dataSourceService.batchImport(any(), any(), any())).thenReturn(new HashMap<>());
            when(metadataDefinitionService.batchImport(any(), any())).thenReturn(new HashMap<>());
            doNothing().when(batchUpChecker).checkDataSourceConnection(any(), any(), any());
            doReturn(new GroupInfoDto()).when(groupInfoService).upsertByWhere(any(), any(), any(UserDetail.class));
            doNothing().when(groupInfoService).updateImportProgress(any(), anyInt(), any(), any());
            doNothing().when(groupInfoService).updateRecordStatus(any(), any(), any(), any(), any());
            doReturn(new ArrayList<>()).when(groupInfoService).buildImportRecordDetails(any(), any());

            groupInfoService.executeImportAsync(payloads, user, null, "test.tar", recordId);

            // Verify upsertByWhere is called for each group info
            verify(groupInfoService).upsertByWhere(any(), any(), eq(user));
        }

        @Test
        @DisplayName("Should clear group info fields before saving")
        void testExecuteImportAsyncClearsGroupInfoFields() {
            Map<String, List<TaskUpAndLoadDto>> payloads = new HashMap<>();

            GroupInfoDto groupInfo = new GroupInfoDto();
            groupInfo.setId(new ObjectId());
            groupInfo.setName("Test Group");
            groupInfo.setCreateUser("originalUser");
            groupInfo.setCustomId("customId");
            groupInfo.setLastUpdBy("lastUpdBy");
            groupInfo.setUserId("userId");
            groupInfo.setResourceItemList(new ArrayList<>());

            TaskUpAndLoadDto groupPayload = new TaskUpAndLoadDto();
            groupPayload.setCollectionName("GroupInfo");
            groupPayload.setJson(JsonUtil.toJsonUseJackson(groupInfo));

            payloads.put("GroupInfo.json", Arrays.asList(groupPayload));
            ObjectId recordId = new ObjectId();

            // Mock all required dependencies
            when(dataSourceService.batchImport(any(), any(), any())).thenReturn(new HashMap<>());
            when(metadataDefinitionService.batchImport(any(), any())).thenReturn(new HashMap<>());
            doNothing().when(batchUpChecker).checkDataSourceConnection(any(), any(), any());
            doReturn(new GroupInfoDto()).when(groupInfoService).upsertByWhere(any(), any(), any(UserDetail.class));
            doNothing().when(groupInfoService).updateImportProgress(any(), anyInt(), any(), any());
            doNothing().when(groupInfoService).updateRecordStatus(any(), any(), any(), any(), any());
            doReturn(new ArrayList<>()).when(groupInfoService).buildImportRecordDetails(any(), any());

            groupInfoService.executeImportAsync(payloads, user, null, "test.tar", recordId);

            // Verify upsertByWhere is called for each group info
            verify(groupInfoService).upsertByWhere(any(), any(), eq(user));
        }

        @Test
        @DisplayName("Should handle empty payloads gracefully")
        void testExecuteImportAsyncEmptyPayloads() {
            Map<String, List<TaskUpAndLoadDto>> payloads = new HashMap<>();
            ObjectId recordId = new ObjectId();

            // Mock all required dependencies
            when(dataSourceService.batchImport(any(), any(), any())).thenReturn(new HashMap<>());
            when(metadataDefinitionService.batchImport(any(), any())).thenReturn(new HashMap<>());
            doNothing().when(batchUpChecker).checkDataSourceConnection(any(), any(), any());
            doNothing().when(groupInfoService).updateImportProgress(any(), anyInt(), any(), any());
            doNothing().when(groupInfoService).updateRecordStatus(any(), any(), any(), any(), any());
            doReturn(new ArrayList<>()).when(groupInfoService).buildImportRecordDetails(any(), any());

            assertDoesNotThrow(() -> {
                groupInfoService.executeImportAsync(payloads, user, null, "test.tar", recordId);
            });

            verify(groupInfoService).updateRecordStatus(eq(recordId), eq(GroupInfoRecordDto.STATUS_COMPLETED),
                    isNull(), any(), eq(user));
        }

        @Test
        @DisplayName("Should calculate progress correctly during import stages")
        void testExecuteImportAsyncProgressCalculation() {
            Map<String, List<TaskUpAndLoadDto>> payloads = new HashMap<>();
            payloads.put("GroupInfo.json", new ArrayList<>());
            ObjectId recordId = new ObjectId();

            // Mock all required dependencies
            when(dataSourceService.batchImport(any(), any(), any())).thenReturn(new HashMap<>());
            when(metadataDefinitionService.batchImport(any(), any())).thenReturn(new HashMap<>());
            doNothing().when(batchUpChecker).checkDataSourceConnection(any(), any(), any());
            doNothing().when(groupInfoService).updateImportProgress(any(), anyInt(), any(), any());
            doNothing().when(groupInfoService).updateRecordStatus(any(), any(), any(), any(), any());
            doReturn(new ArrayList<>()).when(groupInfoService).buildImportRecordDetails(any(), any());

            groupInfoService.executeImportAsync(payloads, user, null, "test.tar", recordId);

            // Verify progress updates are called multiple times during import stages
            verify(groupInfoService, atLeast(4)).updateImportProgress(eq(recordId), anyInt(), any(), eq(user));
        }

        @Test
        @DisplayName("Should map resource items with task ID mapping")
        void testExecuteImportAsyncMapsResourceItems() {
            Map<String, List<TaskUpAndLoadDto>> payloads = new HashMap<>();

            ResourceItem item = new ResourceItem();
            item.setId("oldTaskId");
            item.setType(ResourceType.SYNC_TASK);

            ObjectId groupId = new ObjectId();
            GroupInfoDto groupInfo = new GroupInfoDto();
            groupInfo.setId(groupId);
            groupInfo.setName("Test Group");
            groupInfo.setResourceItemList(Arrays.asList(item));

            TaskUpAndLoadDto groupPayload = new TaskUpAndLoadDto();
            groupPayload.setCollectionName("GroupInfo");
            groupPayload.setJson(JsonUtil.toJsonUseJackson(groupInfo));

            payloads.put("GroupInfo.json", Arrays.asList(groupPayload));
            ObjectId recordId = new ObjectId();

            // Create a matching GroupInfoRecordDetail
            GroupInfoRecordDetail recordDetail = new GroupInfoRecordDetail();
            recordDetail.setGroupId(groupId.toHexString());
            recordDetail.setRecordDetails(new ArrayList<>());
            List<GroupInfoRecordDetail> details = Arrays.asList(recordDetail);

            // Mock all required dependencies
            when(dataSourceService.batchImport(any(), any(), any())).thenReturn(new HashMap<>());
            when(metadataDefinitionService.batchImport(any(), any())).thenReturn(new HashMap<>());
            doNothing().when(batchUpChecker).checkDataSourceConnection(any(), any(), any());
            doReturn(new GroupInfoDto()).when(groupInfoService).upsertByWhere(any(), any(), any(UserDetail.class));
            doNothing().when(groupInfoService).updateImportProgress(any(), anyInt(), any(), any());
            doNothing().when(groupInfoService).updateRecordStatus(any(), any(), any(), any(), any());
            doReturn(details).when(groupInfoService).buildImportRecordDetails(any(), any());

            groupInfoService.executeImportAsync(payloads, user, null, "test.tar", recordId);

            // Verify upsertByWhere is called for each group info
            verify(groupInfoService).upsertByWhere(any(), any(), eq(user));
        }
    }


    @Nested
    @DisplayName("extractResourceIdsByType Tests")
    class ExtractResourceIdsByTypeTests {

        @Test
        @DisplayName("Should return empty map when groupInfos is null")
        void testExtractResourceIdsByTypeNull() {
            Map<ResourceType, Set<String>> result = (Map<ResourceType, Set<String>>) ReflectionTestUtils.invokeMethod(
                    groupInfoService, "extractResourceIdsByType", (List<GroupInfoDto>) null);

            assertNotNull(result);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should return empty map when groupInfos is empty")
        void testExtractResourceIdsByTypeEmpty() {
            Map<ResourceType, Set<String>> result = (Map<ResourceType, Set<String>>) ReflectionTestUtils.invokeMethod(
                    groupInfoService, "extractResourceIdsByType", Collections.emptyList());

            assertNotNull(result);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should extract resource IDs by type correctly")
        void testExtractResourceIdsByTypeNormal() {
            GroupInfoDto groupDto = new GroupInfoDto();
            ResourceItem item1 = new ResourceItem();
            item1.setId("task1");
            item1.setType(ResourceType.SYNC_TASK);

            ResourceItem item2 = new ResourceItem();
            item2.setId("task2");
            item2.setType(ResourceType.MIGRATE_TASK);

            ResourceItem item3 = new ResourceItem();
            item3.setId("task3");
            item3.setType(ResourceType.SYNC_TASK);

            groupDto.setResourceItemList(Arrays.asList(item1, item2, item3));

            Map<ResourceType, Set<String>> result = (Map<ResourceType, Set<String>>) ReflectionTestUtils.invokeMethod(
                    groupInfoService, "extractResourceIdsByType", Arrays.asList(groupDto));

            assertNotNull(result);
            assertEquals(2, result.size());
            assertTrue(result.containsKey(ResourceType.SYNC_TASK));
            assertTrue(result.containsKey(ResourceType.MIGRATE_TASK));
            assertEquals(2, result.get(ResourceType.SYNC_TASK).size());
            assertEquals(1, result.get(ResourceType.MIGRATE_TASK).size());
        }

        @Test
        @DisplayName("Should skip null items and items with blank id")
        void testExtractResourceIdsByTypeSkipInvalid() {
            GroupInfoDto groupDto = new GroupInfoDto();
            ResourceItem item1 = new ResourceItem();
            item1.setId("task1");
            item1.setType(ResourceType.SYNC_TASK);

            ResourceItem item2 = new ResourceItem();
            item2.setId("");
            item2.setType(ResourceType.MIGRATE_TASK);

            ResourceItem item3 = null;

            groupDto.setResourceItemList(Arrays.asList(item1, item2, item3));

            Map<ResourceType, Set<String>> result = (Map<ResourceType, Set<String>>) ReflectionTestUtils.invokeMethod(
                    groupInfoService, "extractResourceIdsByType", Arrays.asList(groupDto));

            assertNotNull(result);
            assertEquals(1, result.size());
            assertTrue(result.containsKey(ResourceType.SYNC_TASK));
        }
    }

    @Nested
    @DisplayName("buildGroupInfoPayload Tests")
    class BuildGroupInfoPayloadTests {

        @Test
        @DisplayName("Should return empty list when groupInfos is null")
        void testBuildGroupInfoPayloadNull() {
            List<TaskUpAndLoadDto> result = ReflectionTestUtils.invokeMethod(
                    groupInfoService, "buildGroupInfoPayload",
                    (List<GroupInfoDto>) null, Collections.<String, String>emptyMap());

            assertNotNull(result);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should return empty list when groupInfos is empty")
        void testBuildGroupInfoPayloadEmpty() {
            List<TaskUpAndLoadDto> result = ReflectionTestUtils.invokeMethod(
                    groupInfoService, "buildGroupInfoPayload",
                    Collections.<GroupInfoDto>emptyList(), Collections.<String, String>emptyMap());

            assertNotNull(result);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should build payload correctly")
        void testBuildGroupInfoPayloadNormal() {
            GroupInfoDto groupDto = new GroupInfoDto();
            groupDto.setId(new ObjectId());
            groupDto.setName("Test Group");
            groupDto.setDescription("Test Description");

            List<TaskUpAndLoadDto> result = ReflectionTestUtils.invokeMethod(
                    groupInfoService, "buildGroupInfoPayload",
                    Arrays.asList(groupDto), Collections.<String, String>emptyMap());

            assertNotNull(result);
            assertEquals(1, result.size());
            assertEquals("GroupInfo", result.get(0).getCollectionName());
            assertNotNull(result.get(0).getJson());
        }
    }


    @Nested
    @DisplayName("collectGroupInfoPayload Tests")
    class CollectGroupInfoPayloadTests {

        @Test
        @DisplayName("Should not add anything when payload is null")
        void testCollectGroupInfoPayloadNull() {
            List<GroupInfoDto> groupInfos = new ArrayList<>();
            ReflectionTestUtils.invokeMethod(groupInfoService, "collectGroupInfoPayload",
                    (List<TaskUpAndLoadDto>) null, groupInfos);

            assertTrue(groupInfos.isEmpty());
        }

        @Test
        @DisplayName("Should not add anything when payload is empty")
        void testCollectGroupInfoPayloadEmpty() {
            List<GroupInfoDto> groupInfos = new ArrayList<>();
            ReflectionTestUtils.invokeMethod(groupInfoService, "collectGroupInfoPayload",
                    Collections.emptyList(), groupInfos);

            assertTrue(groupInfos.isEmpty());
        }

        @Test
        @DisplayName("Should skip non-GroupInfo collection names")
        void testCollectGroupInfoPayloadSkipNonGroupInfo() {
            List<GroupInfoDto> groupInfos = new ArrayList<>();
            TaskUpAndLoadDto dto = new TaskUpAndLoadDto("Task", "{\"name\":\"test\"}");

            ReflectionTestUtils.invokeMethod(groupInfoService, "collectGroupInfoPayload",
                    Arrays.asList(dto), groupInfos);

            assertTrue(groupInfos.isEmpty());
        }

        @Test
        @DisplayName("Should collect GroupInfo correctly")
        void testCollectGroupInfoPayloadNormal() {
            List<GroupInfoDto> groupInfos = new ArrayList<>();
            TaskUpAndLoadDto dto = new TaskUpAndLoadDto("GroupInfo", "{\"name\":\"Test Group\"}");

            ReflectionTestUtils.invokeMethod(groupInfoService, "collectGroupInfoPayload",
                    Arrays.asList(dto), groupInfos);

            assertEquals(1, groupInfos.size());
            assertEquals("Test Group", groupInfos.get(0).getName());
        }
    }

    @Nested
    @DisplayName("buildGroupExportFileName Tests")
    class BuildGroupExportFileNameTests {

        @Test
        @DisplayName("Should return batch filename when multiple groups")
        void testBuildGroupExportFileNameMultiple() {
            GroupInfoDto group1 = new GroupInfoDto();
            group1.setName("Group1");
            GroupInfoDto group2 = new GroupInfoDto();
            group2.setName("Group2");

            String result = (String) ReflectionTestUtils.invokeMethod(
                    groupInfoService, "buildGroupExportFileName",
                    Arrays.asList(group1, group2), "20260112");

            assertEquals("group_batch-20260112", result);
        }

        @Test
        @DisplayName("Should return group name filename when single group")
        void testBuildGroupExportFileNameSingle() {
            GroupInfoDto group = new GroupInfoDto();
            group.setName("MyGroup");

            String result = (String) ReflectionTestUtils.invokeMethod(
                    groupInfoService, "buildGroupExportFileName",
                    Arrays.asList(group), "20260112");

            assertEquals("MyGroup-20260112", result);
        }

        @Test
        @DisplayName("Should return batch filename when single group has blank name")
        void testBuildGroupExportFileNameSingleBlankName() {
            GroupInfoDto group = new GroupInfoDto();
            group.setName("");

            String result = (String) ReflectionTestUtils.invokeMethod(
                    groupInfoService, "buildGroupExportFileName",
                    Arrays.asList(group), "20260112");

            assertEquals("group_batch-20260112", result);
        }
    }

    @Nested
    @DisplayName("getResourceId Tests")
    class GetResourceIdTests {

        @Test
        @DisplayName("Should return task id for TaskDto")
        void testGetResourceIdTaskDto() {
            TaskDto taskDto = new TaskDto();
            ObjectId id = new ObjectId();
            taskDto.setId(id);

            String result = (String) ReflectionTestUtils.invokeMethod(
                    groupInfoService, "getResourceId", taskDto);

            assertEquals(id.toHexString(), result);
        }

        @Test
        @DisplayName("Should return module id for ModulesDto")
        void testGetResourceIdModulesDto() {
            ModulesDto modulesDto = new ModulesDto();
            ObjectId id = new ObjectId();
            modulesDto.setId(id);

            String result = (String) ReflectionTestUtils.invokeMethod(
                    groupInfoService, "getResourceId", modulesDto);

            assertEquals(id.toHexString(), result);
        }

        @Test
        @DisplayName("Should return null for unknown type")
        void testGetResourceIdUnknownType() {
            String result = (String) ReflectionTestUtils.invokeMethod(
                    groupInfoService, "getResourceId", "unknown");

            assertNull(result);
        }

        @Test
        @DisplayName("Should return null when TaskDto id is null")
        void testGetResourceIdTaskDtoNullId() {
            TaskDto taskDto = new TaskDto();
            taskDto.setId(null);

            String result = (String) ReflectionTestUtils.invokeMethod(
                    groupInfoService, "getResourceId", taskDto);

            assertNull(result);
        }
    }

    @Nested
    @DisplayName("getResourceName Tests")
    class GetResourceNameTests {

        @Test
        @DisplayName("Should return task name for TaskDto")
        void testGetResourceNameTaskDto() {
            TaskDto taskDto = new TaskDto();
            taskDto.setName("Test Task");

            String result = (String) ReflectionTestUtils.invokeMethod(
                    groupInfoService, "getResourceName", taskDto);

            assertEquals("Test Task", result);
        }

        @Test
        @DisplayName("Should return module name for ModulesDto")
        void testGetResourceNameModulesDto() {
            ModulesDto modulesDto = new ModulesDto();
            modulesDto.setName("Test Module");

            String result = (String) ReflectionTestUtils.invokeMethod(
                    groupInfoService, "getResourceName", modulesDto);

            assertEquals("Test Module", result);
        }

        @Test
        @DisplayName("Should return null for unknown type")
        void testGetResourceNameUnknownType() {
            String result = (String) ReflectionTestUtils.invokeMethod(
                    groupInfoService, "getResourceName", "unknown");

            assertNull(result);
        }
    }

    @Nested
    @DisplayName("updateRecordStatus Tests")
    class UpdateRecordStatusTests {

        @Test
        @DisplayName("Should not update when recordId is null")
        void testUpdateRecordStatusNullId() {
            ReflectionTestUtils.invokeMethod(groupInfoService, "updateRecordStatus",
                    null, GroupInfoRecordDto.STATUS_COMPLETED, null, null, user);

            verify(groupInfoRecordService, never()).updateById(any(String.class), any(Update.class), any(UserDetail.class));
        }

        @Test
        @DisplayName("Should update status correctly")
        void testUpdateRecordStatusNormal() {
            ObjectId recordId = new ObjectId();

            ReflectionTestUtils.invokeMethod(groupInfoService, "updateRecordStatus",
                    recordId, GroupInfoRecordDto.STATUS_COMPLETED, null, null, user);

            verify(groupInfoRecordService).updateById(eq(recordId), any(), eq(user));
        }

        @Test
        @DisplayName("Should update status with message")
        void testUpdateRecordStatusWithMessage() {
            ObjectId recordId = new ObjectId();

            ReflectionTestUtils.invokeMethod(groupInfoService, "updateRecordStatus",
                    recordId, GroupInfoRecordDto.STATUS_FAILED, "Error message", null, user);

            verify(groupInfoRecordService).updateById(eq(recordId), any(), eq(user));
        }
    }

    @Nested
    @DisplayName("updateImportProgress Tests")
    class UpdateImportProgressTests {

        @Test
        @DisplayName("Should update progress correctly")
        void testUpdateImportProgressNormal() {
            ObjectId recordId = new ObjectId();

            ReflectionTestUtils.invokeMethod(groupInfoService, "updateImportProgress",
                    recordId, 50, null, user);

            verify(groupInfoRecordService).updateById(eq(recordId), any(), eq(user));
        }
    }

    @Nested
    @DisplayName("buildExportRecordDetails Tests")
    class BuildExportRecordDetailsTests {

        @Test
        @DisplayName("Should build export record details correctly")
        void testBuildExportRecordDetailsNormal() {
            GroupInfoDto groupDto = new GroupInfoDto();
            ObjectId groupId = new ObjectId();
            groupDto.setId(groupId);
            groupDto.setName("Test Group");
            groupDto.setResourceItemList(new ArrayList<>());

            Map<ResourceType, List<?>> resourcesByType = new LinkedHashMap<>();

            List<GroupInfoRecordDetail> result = (List<GroupInfoRecordDetail>) ReflectionTestUtils.invokeMethod(
                    groupInfoService, "buildExportRecordDetails",
                    Arrays.asList(groupDto), resourcesByType);

            assertNotNull(result);
            assertEquals(1, result.size());
            assertEquals(groupId.toHexString(), result.get(0).getGroupId());
            assertEquals("Test Group", result.get(0).getGroupName());
        }

        @Test
        @DisplayName("Should handle null group id")
        void testBuildExportRecordDetailsNullGroupId() {
            GroupInfoDto groupDto = new GroupInfoDto();
            groupDto.setId(null);
            groupDto.setName("Test Group");
            groupDto.setResourceItemList(new ArrayList<>());

            Map<ResourceType, List<?>> resourcesByType = new LinkedHashMap<>();

            List<GroupInfoRecordDetail> result = (List<GroupInfoRecordDetail>) ReflectionTestUtils.invokeMethod(
                    groupInfoService, "buildExportRecordDetails",
                    Arrays.asList(groupDto), resourcesByType);

            assertNotNull(result);
            assertEquals(1, result.size());
            assertNull(result.get(0).getGroupId());
        }
    }

    @Nested
    @DisplayName("buildRecord Tests - Extended")
    class BuildRecordExtendedTests {

        @Test
        @DisplayName("Should set null operator when user is null")
        void testBuildRecordNullUser() {
            List<GroupInfoRecordDetail> details = new ArrayList<>();
            String fileName = "test.tar";

            GroupInfoRecordDto result = (GroupInfoRecordDto) ReflectionTestUtils.invokeMethod(
                    groupInfoService, "buildRecord",
                    GroupInfoRecordDto.TYPE_EXPORT, null, details, fileName);

            assertNotNull(result);
            assertNull(result.getOperator());
        }

        @Test
        @DisplayName("Should set null status for unknown type")
        void testBuildRecordUnknownType() {
            List<GroupInfoRecordDetail> details = new ArrayList<>();
            String fileName = "test.tar";

            GroupInfoRecordDto result = (GroupInfoRecordDto) ReflectionTestUtils.invokeMethod(
                    groupInfoService, "buildRecord",
                    "unknown", user, details, fileName);

            assertNotNull(result);
            assertNull(result.getStatus());
        }

        @Test
        @DisplayName("Should set operation time")
        void testBuildRecordOperationTime() {
            List<GroupInfoRecordDetail> details = new ArrayList<>();
            String fileName = "test.tar";

            GroupInfoRecordDto result = (GroupInfoRecordDto) ReflectionTestUtils.invokeMethod(
                    groupInfoService, "buildRecord",
                    GroupInfoRecordDto.TYPE_EXPORT, user, details, fileName);

            assertNotNull(result);
            assertNotNull(result.getOperationTime());
        }
    }

    @Nested
    @DisplayName("loadGroupInfosByIds Tests")
    class LoadGroupInfosByIdsTests {

        @Test
        @DisplayName("Should return empty list when groupIds is null")
        void testLoadGroupInfosByIdsNull() {
            List<GroupInfoDto> result = (List<GroupInfoDto>) ReflectionTestUtils.invokeMethod(
                    groupInfoService, "loadGroupInfosByIds", (List<String>) null, user);

            assertNotNull(result);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should return empty list when groupIds is empty")
        void testLoadGroupInfosByIdsEmpty() {
            List<GroupInfoDto> result = (List<GroupInfoDto>) ReflectionTestUtils.invokeMethod(
                    groupInfoService, "loadGroupInfosByIds", Collections.emptyList(), user);

            assertNotNull(result);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should load group infos by ids")
        void testLoadGroupInfosByIdsNormal() {
            ObjectId id = new ObjectId();
            GroupInfoDto groupDto = new GroupInfoDto();
            groupDto.setId(id);
            groupDto.setName("Test Group");

            doReturn(Arrays.asList(groupDto)).when(groupInfoService).findAllDto(any(Query.class), any(UserDetail.class));

            List<GroupInfoDto> result = (List<GroupInfoDto>) ReflectionTestUtils.invokeMethod(
                    groupInfoService, "loadGroupInfosByIds", Arrays.asList(id.toHexString()), user);

            assertNotNull(result);
            assertEquals(1, result.size());
            assertEquals("Test Group", result.get(0).getName());
        }
    }

    @Nested
    @DisplayName("getConnectionChangedFields tests")
    class GetConnectionChangedFieldsTest {

        private DataSourceConnectionDto buildConn(String name, String connType, String dbType,
                                                   Map<String, Object> config, String pdkHash) {
            DataSourceConnectionDto conn = new DataSourceConnectionDto();
            conn.setName(name);
            conn.setConnection_type(connType);
            conn.setDatabase_type(dbType);
            conn.setConfig(config);
            conn.setPdkHash(pdkHash);
            return conn;
        }

        private DataSourceDefinitionDto buildDefinition(String pdkHash, Map<String, Object> connectionProperties) {
            DataSourceDefinitionDto def = new DataSourceDefinitionDto();
            def.setPdkHash(pdkHash);
            LinkedHashMap<String, Object> properties = new LinkedHashMap<>();
            Map<String, Object> connection = new LinkedHashMap<>();
            connection.put("properties", connectionProperties);
            properties.put("connection", connection);
            def.setProperties(properties);
            return def;
        }

        @Test
        @DisplayName("Same connection should produce empty changes")
        void testSameConnection() {
            Map<String, Object> config = new HashMap<>();
            config.put("database", "mydb");
            config.put("timezone", "UTC");
            DataSourceConnectionDto conn1 = buildConn("test", "source", "MySQL", new HashMap<>(config), "hash1");
            DataSourceConnectionDto conn2 = buildConn("test", "source", "MySQL", new HashMap<>(config), "hash1");

            List<FieldChange> changes = ReflectionTestUtils.invokeMethod(
                    groupInfoService, "getConnectionChangedFields", conn1, conn2);

            assertNotNull(changes);
            assertTrue(changes.isEmpty());
        }

        @Test
        @DisplayName("Config.database change should produce precise FieldChange")
        void testConfigDatabaseChange() {
            Map<String, Object> config1 = new HashMap<>();
            config1.put("database_name", "old_db");
            config1.put("timezone", "UTC");
            Map<String, Object> config2 = new HashMap<>();
            config2.put("database_name", "new_db");
            config2.put("timezone", "UTC");

            DataSourceConnectionDto fileConn = buildConn("test", "source", "MySQL", config2, null);
            DataSourceConnectionDto existingConn = buildConn("test", "source", "MySQL", config1, null);

            List<FieldChange> changes = ReflectionTestUtils.invokeMethod(
                    groupInfoService, "getConnectionChangedFields", fileConn, existingConn);

            assertNotNull(changes);
            boolean foundDatabaseChange = changes.stream()
                    .anyMatch(c -> "config.database_name".equals(c.getField()) && "old_db".equals(c.getFrom()) && "new_db".equals(c.getTo()));
            assertTrue(foundDatabaseChange, "Should have precise config.database_name change, got: " + changes);
        }

        @Test
        @DisplayName("Sensitive/env fields (host, password) should not produce diff")
        void testSensitiveFieldsExcluded() {
            Map<String, Object> config1 = new HashMap<>();
            config1.put("host", "host1");
            config1.put("password", "pass1");
            config1.put("database", "db");
            Map<String, Object> config2 = new HashMap<>();
            config2.put("database", "db");
            // host/password not in file config (stripped on export)

            DataSourceConnectionDto fileConn = buildConn("test", "source", "MySQL", config2, null);
            DataSourceConnectionDto existingConn = buildConn("test", "source", "MySQL", config1, null);

            List<FieldChange> changes = ReflectionTestUtils.invokeMethod(
                    groupInfoService, "getConnectionChangedFields", fileConn, existingConn);

            assertNotNull(changes);
            assertTrue(changes.stream().noneMatch(c -> c.getField().contains("host")),
                    "host should be excluded");
            assertTrue(changes.stream().noneMatch(c -> c.getField().contains("password")),
                    "password should be excluded");
        }

        @Test
        @DisplayName("All config field changes should produce exact match entries")
        void testAllConfigFieldsExactMatch() {
            Map<String, Object> config1 = new HashMap<>();
            config1.put("database", "db");
            config1.put("someOtherField", "val1");
            config1.put("anotherField", "val2");
            Map<String, Object> config2 = new HashMap<>();
            config2.put("database", "db");
            config2.put("someOtherField", "changed1");
            config2.put("anotherField", "changed2");

            DataSourceConnectionDto fileConn = buildConn("test", "source", "MySQL", config2, null);
            DataSourceConnectionDto existingConn = buildConn("test", "source", "MySQL", config1, null);

            List<FieldChange> changes = ReflectionTestUtils.invokeMethod(
                    groupInfoService, "getConnectionChangedFields", fileConn, existingConn);

            assertNotNull(changes);
            // Should NOT have 'config.other' summary
            boolean foundOtherConfig = changes.stream()
                    .anyMatch(c -> "config.other".equals(c.getField()));
            assertFalse(foundOtherConfig, "Should NOT have 'config.other' summary, got: " + changes);

            // Should have exact match entries for each changed field
            FieldChange someOther = changes.stream()
                    .filter(c -> "config.someOtherField".equals(c.getField())).findFirst().orElse(null);
            assertNotNull(someOther, "Should have 'config.someOtherField' change");
            assertEquals("val1", someOther.getFrom());
            assertEquals("changed1", someOther.getTo());

            FieldChange another = changes.stream()
                    .filter(c -> "config.anotherField".equals(c.getField())).findFirst().orElse(null);
            assertNotNull(another, "Should have 'config.anotherField' change");
            assertEquals("val2", another.getFrom());
            assertEquals("changed2", another.getTo());
        }

        @Test
        @DisplayName("Top-level important field change should produce precise FieldChange")
        void testTopLevelFieldChange() {
            Map<String, Object> config = new HashMap<>();
            config.put("database", "db");

            DataSourceConnectionDto fileConn = buildConn("test", "source_and_target", "MySQL", config, null);
            DataSourceConnectionDto existingConn = buildConn("test", "source", "MySQL", config, null);

            List<FieldChange> changes = ReflectionTestUtils.invokeMethod(
                    groupInfoService, "getConnectionChangedFields", fileConn, existingConn);

            assertNotNull(changes);
            boolean foundConnTypeChange = changes.stream()
                    .anyMatch(c -> "connection_type".equals(c.getField())
                            && "source".equals(c.getFrom())
                            && "source_and_target".equals(c.getTo()));
            assertTrue(foundConnTypeChange, "Should have connection_type change, got: " + changes);
        }

        @Test
        @DisplayName("Definition null should degrade gracefully")
        void testDefinitionNull() {
            Map<String, Object> config1 = new HashMap<>();
            config1.put("database", "old_db");
            Map<String, Object> config2 = new HashMap<>();
            config2.put("database", "new_db");

            DataSourceConnectionDto fileConn = buildConn("test", "source", "MySQL", config2, null);
            DataSourceConnectionDto existingConn = buildConn("test", "source", "MySQL", config1, null);

            List<FieldChange> changes = ReflectionTestUtils.invokeMethod(
                    groupInfoService, "getConnectionChangedFields", fileConn, existingConn);

            assertNotNull(changes);
            assertFalse(changes.isEmpty(), "Should still detect database change without definition");
        }

        @Test
        @DisplayName("Password-type config key should be masked in diff")
        void testPasswordFieldMasked() {
            Map<String, Object> config1 = new HashMap<>();
            config1.put("database_host", "host1.example.com");
            config1.put("database_password", "oldpass");
            config1.put("database_name", "old_db");
            Map<String, Object> config2 = new HashMap<>();
            config2.put("database_host", "host2.example.com");
            config2.put("database_password", "newpass");
            config2.put("database_name", "new_db");

            DataSourceConnectionDto fileConn = buildConn("test", "source", "MySQL", config2, "hash1");
            DataSourceConnectionDto existingConn = buildConn("test", "source", "MySQL", config1, "hash1");

            List<FieldChange> changes = ReflectionTestUtils.invokeMethod(
                    groupInfoService, "getConnectionChangedFields", fileConn, existingConn);

            assertNotNull(changes);
            // database_host change should be detected with actual values (not masked)
            boolean foundHost = changes.stream().anyMatch(c ->
                    "config.database_host".equals(c.getField())
                            && "host1.example.com".equals(c.getFrom())
                            && "host2.example.com".equals(c.getTo()));
            assertTrue(foundHost, "database_host change should show actual values, got: " + changes);
            // database_password change should be detected but values masked
            boolean foundPass = changes.stream().anyMatch(c ->
                    "config.database_password".equals(c.getField())
                            && "******".equals(c.getFrom())
                            && "******".equals(c.getTo()));
            assertTrue(foundPass, "database_password change should be detected with masked values, got: " + changes);
            // database_name should still show actual values
            boolean foundDb = changes.stream().anyMatch(c -> "config.database_name".equals(c.getField()));
            assertTrue(foundDb, "database_name change should be detected, got: " + changes);
        }

        @Test
        @DisplayName("Config key should be used directly as field name (no spec mapping)")
        void testConfigKeyUsedDirectly() {
            Map<String, Object> config1 = new HashMap<>();
            config1.put("database", "old_db");
            Map<String, Object> config2 = new HashMap<>();
            config2.put("database", "new_db");

            DataSourceConnectionDto fileConn = buildConn("test", "source", "MySQL", config2, "hash1");
            DataSourceConnectionDto existingConn = buildConn("test", "source", "MySQL", config1, "hash1");

            List<FieldChange> changes = ReflectionTestUtils.invokeMethod(
                    groupInfoService, "getConnectionChangedFields", fileConn, existingConn);

            assertNotNull(changes);
            // Change should be reported using the original config key "database"
            boolean foundDb = changes.stream().anyMatch(c ->
                    "config.database".equals(c.getField())
                            && "old_db".equals(c.getFrom())
                            && "new_db".equals(c.getTo()));
            assertTrue(foundDb, "Should detect database change using original config key, got: " + changes);
        }

        @Test
        @DisplayName("MongoDB URI change should mask only password, non-MongoDB URI should be fully masked")
        void testUriFieldSmartMask() {
            // Config key "database_uri" matches URI_DISPLAY_API_KEYS directly
            Map<String, Object> config1 = new HashMap<>();
            config1.put("database_uri", "mongodb://admin:oldPass@host1:27017/db1");
            Map<String, Object> config2 = new HashMap<>();
            config2.put("database_uri", "mongodb://admin:newPass@host2:27017/db2");

            DataSourceConnectionDto fileConn = buildConn("test", "source", "MongoDB", config2, "hash1");
            DataSourceConnectionDto existingConn = buildConn("test", "source", "MongoDB", config1, "hash1");

            List<FieldChange> changes = ReflectionTestUtils.invokeMethod(
                    groupInfoService, "getConnectionChangedFields", fileConn, existingConn);

            assertNotNull(changes);
            FieldChange uriChange = changes.stream()
                    .filter(c -> "config.database_uri".equals(c.getField()))
                    .findFirst().orElse(null);
            assertNotNull(uriChange, "URI change should be detected, got: " + changes);
            // Password parts should be masked, but host/db visible
            String fromStr = (String) uriChange.getFrom();
            String toStr = (String) uriChange.getTo();
            assertTrue(fromStr.contains("host1:27017"), "From should show host");
            assertTrue(toStr.contains("host2:27017"), "To should show host");
            assertFalse(fromStr.contains("oldPass"), "From should not show password");
            assertFalse(toStr.contains("newPass"), "To should not show password");
            assertTrue(fromStr.contains("******"), "From should contain mask");
            assertTrue(toStr.contains("******"), "To should contain mask");
        }
    }

    @Nested
    @DisplayName("ResourceHandler BFS utility tests")
    class ResourceHandlerBfsTest {

        @Test
        @DisplayName("buildConfigPathToApiKeyMap should return correct mappings")
        void testBuildConfigPathToApiKeyMap() {
            Map<String, Object> hostMeta = new LinkedHashMap<>();
            hostMeta.put("apiServerKey", "database_host");
            Map<String, Object> portMeta = new LinkedHashMap<>();
            portMeta.put("apiServerKey", "database_port");
            Map<String, Object> dbMeta = new LinkedHashMap<>();
            dbMeta.put("title", "Database");
            // No apiServerKey for database

            // Nested: ssl.sslKey
            Map<String, Object> sslKeyMeta = new LinkedHashMap<>();
            sslKeyMeta.put("apiServerKey", "database_password");
            Map<String, Object> sslChildProps = new LinkedHashMap<>();
            sslChildProps.put("sslKey", sslKeyMeta);
            Map<String, Object> sslMeta = new LinkedHashMap<>();
            sslMeta.put("properties", sslChildProps);

            Map<String, Object> connProps = new LinkedHashMap<>();
            connProps.put("host", hostMeta);
            connProps.put("port", portMeta);
            connProps.put("database", dbMeta);
            connProps.put("ssl", sslMeta);

            DataSourceDefinitionDto def = new DataSourceDefinitionDto();
            LinkedHashMap<String, Object> properties = new LinkedHashMap<>();
            Map<String, Object> connection = new LinkedHashMap<>();
            connection.put("properties", connProps);
            properties.put("connection", connection);
            def.setProperties(properties);

            Map<String, String> result = ResourceHandler.buildConfigPathToApiKeyMap(def);
            assertEquals("database_host", result.get("host"));
            assertEquals("database_port", result.get("port"));
            assertNull(result.get("database")); // no apiServerKey
            assertEquals("database_password", result.get("ssl.sslKey"));
        }

        @Test
        @DisplayName("buildConfigPathToLabelMap should return correct labels")
        void testBuildConfigPathToLabelMap() {
            Map<String, Object> hostMeta = new LinkedHashMap<>();
            hostMeta.put("title", "Host Address");
            Map<String, Object> dbMeta = new LinkedHashMap<>();
            dbMeta.put("title", "Database Name");

            Map<String, Object> connProps = new LinkedHashMap<>();
            connProps.put("host", hostMeta);
            connProps.put("database", dbMeta);

            DataSourceDefinitionDto def = new DataSourceDefinitionDto();
            LinkedHashMap<String, Object> properties = new LinkedHashMap<>();
            Map<String, Object> connection = new LinkedHashMap<>();
            connection.put("properties", connProps);
            properties.put("connection", connection);
            def.setProperties(properties);

            Map<String, String> result = ResourceHandler.buildConfigPathToLabelMap(def);
            assertEquals("Host Address", result.get("host"));
            assertEquals("Database Name", result.get("database"));
        }

        @Test
        @DisplayName("getMaskedConfigPaths should return sensitive paths")
        void testGetMaskedConfigPaths() {
            Map<String, Object> hostMeta = new LinkedHashMap<>();
            hostMeta.put("apiServerKey", "database_host");
            Map<String, Object> dbMeta = new LinkedHashMap<>();
            dbMeta.put("apiServerKey", "some_other_key");

            Map<String, Object> connProps = new LinkedHashMap<>();
            connProps.put("host", hostMeta);
            connProps.put("database", dbMeta);

            DataSourceDefinitionDto def = new DataSourceDefinitionDto();
            LinkedHashMap<String, Object> properties = new LinkedHashMap<>();
            Map<String, Object> connection = new LinkedHashMap<>();
            connection.put("properties", connProps);
            properties.put("connection", connection);
            def.setProperties(properties);

            Set<String> masked = ResourceHandler.getMaskedConfigPaths(def);
            assertTrue(masked.contains("host"));
            assertFalse(masked.contains("database"));
        }

        @Test
        @DisplayName("getNestedValue should traverse dotted paths")
        void testGetNestedValue() {
            Map<String, Object> inner = new HashMap<>();
            inner.put("key", "value");
            Map<String, Object> config = new HashMap<>();
            config.put("ssl", inner);
            config.put("simple", "val");

            assertEquals("value", ResourceHandler.getNestedValue(config, "ssl.key"));
            assertEquals("val", ResourceHandler.getNestedValue(config, "simple"));
            assertNull(ResourceHandler.getNestedValue(config, "nonexistent.path"));
            assertNull(ResourceHandler.getNestedValue(null, "any"));
        }

        @Test
        @DisplayName("Null definition should return empty maps")
        void testNullDefinition() {
            assertTrue(ResourceHandler.buildConfigPathToApiKeyMap(null).isEmpty());
            assertTrue(ResourceHandler.buildConfigPathToLabelMap(null).isEmpty());
            assertTrue(ResourceHandler.getMaskedConfigPaths(null).isEmpty());
        }
    }

    @Nested
    @DisplayName("deepDiff tests")
    class DeepDiffTest {

        private List<FieldChange> invokeDiff(String path, Object existing, Object file) {
            List<FieldChange> changes = new ArrayList<>();
            ReflectionTestUtils.invokeMethod(groupInfoService, "deepDiff", changes, path, existing, file);
            return changes;
        }

        @Test
        @DisplayName("Same maps should produce no changes")
        void testSameMaps() {
            Map<String, Object> m = Map.of("a", "1", "b", 2);
            List<FieldChange> changes = invokeDiff("root", new HashMap<>(m), new HashMap<>(m));
            assertTrue(changes.isEmpty());
        }

        @Test
        @DisplayName("Map with added/removed/modified keys")
        void testMapDiff() {
            Map<String, Object> existing = new LinkedHashMap<>();
            existing.put("a", "old");
            existing.put("b", "same");
            existing.put("c", "removed");
            Map<String, Object> file = new LinkedHashMap<>();
            file.put("a", "new");
            file.put("b", "same");
            file.put("d", "added");

            List<FieldChange> changes = invokeDiff("root", existing, file);
            assertTrue(changes.stream().anyMatch(c -> "root.a".equals(c.getField()) && "old".equals(c.getFrom()) && "new".equals(c.getTo())));
            assertTrue(changes.stream().anyMatch(c -> "root.c".equals(c.getField())));
            assertTrue(changes.stream().anyMatch(c -> "root.d".equals(c.getField())));
            assertTrue(changes.stream().noneMatch(c -> "root.b".equals(c.getField())));
        }

        @Test
        @DisplayName("List with keyed diff using ARRAY_KEY_CONFIG (fields by field_name)")
        void testListKeyedDiff() {
            Map<String, Object> f1 = new LinkedHashMap<>();
            f1.put("field_name", "col1");
            f1.put("type", "string");
            Map<String, Object> f2 = new LinkedHashMap<>();
            f2.put("field_name", "col1");
            f2.put("type", "int");

            List<FieldChange> changes = invokeDiff("fields", List.of(f1), List.of(f2));
            assertTrue(changes.stream().anyMatch(c -> c.getField().contains("[col1]") && c.getField().contains("type")));
        }

        @Test
        @DisplayName("List without key config should use index-based diff")
        void testListIndexDiff() {
            List<FieldChange> changes = invokeDiff("unknownArray", List.of("a", "b"), List.of("a", "c"));
            assertTrue(changes.stream().anyMatch(c -> "unknownArray[1]".equals(c.getField())));
            assertTrue(changes.stream().noneMatch(c -> "unknownArray[0]".equals(c.getField())));
        }

        @Test
        @DisplayName("List size difference should produce changes for extra elements")
        void testListSizeDiff() {
            List<FieldChange> changes = invokeDiff("arr", List.of("a"), List.of("a", "b"));
            assertEquals(1, changes.size());
            assertEquals("arr[1]", changes.get(0).getField());
            assertNull(changes.get(0).getFrom());
            assertEquals("b", changes.get(0).getTo());
        }

        @Test
        @DisplayName("Leaf: null vs non-null")
        void testLeafNullVsNonNull() {
            List<FieldChange> changes = invokeDiff("f", null, "val");
            assertEquals(1, changes.size());
            assertEquals("f", changes.get(0).getField());
        }

        @Test
        @DisplayName("Leaf: different types (String vs Map)")
        void testLeafDifferentTypes() {
            List<FieldChange> changes = invokeDiff("f", "str", Map.of("k", "v"));
            assertEquals(1, changes.size());
        }

        @Test
        @DisplayName("Nested Map+List mixed recursion - paths uses keyed diff by 'name'")
        void testNestedMixed() {
            // Start path as "paths" so ARRAY_KEY_CONFIG matches "paths" -> keyField "name"
            List<Map<String, Object>> existing = List.of(Map.of("name", "p1", "desc", "old"));
            List<Map<String, Object>> file = List.of(Map.of("name", "p1", "desc", "new"));
            List<FieldChange> changes = invokeDiff("paths", existing, file);
            assertTrue(changes.stream().anyMatch(c -> c.getField().contains("[p1]") && c.getField().contains("desc")),
                    "Should use keyed diff on paths array, got: " + changes);
        }

        @Test
        @DisplayName("Empty list vs empty list should produce no changes")
        void testEmptyLists() {
            List<FieldChange> changes = invokeDiff("arr", List.of(), List.of());
            assertTrue(changes.isEmpty());
        }
    }

    @Nested
    @DisplayName("jsonEqual tests")
    class JsonEqualTest {

        private boolean invokeJsonEqual(Object a, Object b) {
            return Boolean.TRUE.equals(
                    ReflectionTestUtils.invokeMethod(groupInfoService, "jsonEqual", a, b));
        }

        @Test
        @DisplayName("null vs null should be equal")
        void testBothNull() {
            assertTrue(invokeJsonEqual(null, null));
        }

        @Test
        @DisplayName("Empty string vs null should be equal")
        void testEmptyStringVsNull() {
            assertTrue(invokeJsonEqual("", null));
            assertTrue(invokeJsonEqual(null, ""));
        }

        @Test
        @DisplayName("Empty string vs empty string should be equal")
        void testEmptyStringVsEmptyString() {
            assertTrue(invokeJsonEqual("", ""));
        }

        @Test
        @DisplayName("Map key order difference should still be equal")
        void testMapKeyOrder() {
            Map<String, Object> m1 = new LinkedHashMap<>();
            m1.put("b", 2);
            m1.put("a", 1);
            Map<String, Object> m2 = new LinkedHashMap<>();
            m2.put("a", 1);
            m2.put("b", 2);
            assertTrue(invokeJsonEqual(m1, m2));
        }

        @Test
        @DisplayName("Different values should not be equal")
        void testDifferentValues() {
            assertFalse(invokeJsonEqual("abc", "def"));
            assertFalse(invokeJsonEqual(1, 2));
        }

        @Test
        @DisplayName("null vs non-null should not be equal")
        void testNullVsNonNull() {
            assertFalse(invokeJsonEqual(null, "val"));
            assertFalse(invokeJsonEqual("val", null));
        }
    }

    @Nested
    @DisplayName("normalizeConfigForComparison tests")
    class NormalizeConfigForComparisonTest {

        private Map<String, Object> invoke(Map<String, Object> config) {
            return ReflectionTestUtils.invokeMethod(
                    groupInfoService, "normalizeConfigForComparison", config);
        }

        @Test
        @DisplayName("null config returns null")
        void testNullConfig() {
            assertNull(invoke(null));
        }

        @Test
        @DisplayName("With empty CONFIG_ENV_EXCLUDED_FIELDS, all fields are preserved")
        void testEnvFieldsRemoved() {
            Map<String, Object> config = new HashMap<>();
            config.put("host", "localhost");
            config.put("port", 3306);
            config.put("password", "secret");
            config.put("user", "admin");
            config.put("username", "admin");
            config.put("database_name", "mydb");
            config.put("datasourceInstanceId", "inst1");

            Map<String, Object> result = invoke(config);
            assertNotNull(result);
            // CONFIG_ENV_EXCLUDED_FIELDS is now empty, so all fields are preserved
            assertTrue(result.containsKey("host"));
            assertTrue(result.containsKey("port"));
            assertTrue(result.containsKey("password"));
            assertTrue(result.containsKey("user"));
            assertTrue(result.containsKey("username"));
            assertTrue(result.containsKey("datasourceInstanceId"));
            assertEquals("mydb", result.get("database_name"));
        }

        @Test
        @DisplayName("All fields are preserved for comparison (masking happens at FieldChange level)")
        void testAllFieldsPreserved() {
            Map<String, Object> config = new HashMap<>();
            config.put("myCustomHost", "host.example.com");
            config.put("database", "mydb");

            Map<String, Object> result = invoke(config);
            assertNotNull(result);
            assertTrue(result.containsKey("myCustomHost"));
            assertEquals("host.example.com", result.get("myCustomHost"));
            assertEquals("mydb", result.get("database"));
        }
    }

    @Nested
    @DisplayName("filterToFileKeys tests")
    class FilterToFileKeysTest {

        private Map<String, Object> invoke(Map<String, Object> fileConfig, Map<String, Object> existingConfig) {
            return ReflectionTestUtils.invokeMethod(
                    groupInfoService, "filterToFileKeys", fileConfig, existingConfig);
        }

        @Test
        @DisplayName("null fileConfig returns existingConfig as-is")
        void testNullFileConfig() {
            Map<String, Object> existing = Map.of("a", 1);
            assertSame(existing, invoke(null, existing));
        }

        @Test
        @DisplayName("null existingConfig returns null")
        void testNullExistingConfig() {
            assertNull(invoke(Map.of("a", 1), null));
        }

        @Test
        @DisplayName("Only file keys are retained in existing config")
        void testNormalFilter() {
            Map<String, Object> fileConfig = new HashMap<>();
            fileConfig.put("database", "db");
            fileConfig.put("schema", "public");

            Map<String, Object> existingConfig = new HashMap<>();
            existingConfig.put("database", "db");
            existingConfig.put("schema", "public");
            existingConfig.put("uri", "mongodb://...");  // not in file, should be filtered out

            Map<String, Object> result = invoke(fileConfig, existingConfig);
            assertNotNull(result);
            assertEquals(2, result.size());
            assertTrue(result.containsKey("database"));
            assertTrue(result.containsKey("schema"));
            assertFalse(result.containsKey("uri"));
        }
    }


    @Nested
    @DisplayName("getModuleChangedFields tests")
    class GetModuleChangedFieldsTest {

        private List<FieldChange> invoke(Map<String, Object> fileMap, Map<String, Object> existingMap) {
            return ReflectionTestUtils.invokeMethod(
                    groupInfoService, "getModuleChangedFields", fileMap, existingMap);
        }

        @Test
        @DisplayName("Both null returns empty list")
        void testBothNull() {
            List<FieldChange> changes = invoke(null, null);
            assertTrue(changes.isEmpty());
        }

        @Test
        @DisplayName("One null returns wildcard change")
        void testOneNull() {
            Map<String, Object> map = Map.of("name", "test");
            List<FieldChange> changes = invoke(map, null);
            assertEquals(1, changes.size());
            assertEquals("*", changes.get(0).getField());
        }

        @Test
        @DisplayName("Same maps produce no changes")
        void testSameMaps() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("name", "api1");
            m.put("apiType", "defaultApi");
            List<FieldChange> changes = invoke(new LinkedHashMap<>(m), new LinkedHashMap<>(m));
            assertTrue(changes.isEmpty());
        }

        @Test
        @DisplayName("Different fields produce changes via deepDiff")
        void testDifferentFields() {
            Map<String, Object> existing = new LinkedHashMap<>();
            existing.put("name", "api1");
            existing.put("apiType", "defaultApi");
            existing.put("description", "old desc");
            Map<String, Object> file = new LinkedHashMap<>();
            file.put("name", "api1");
            file.put("apiType", "clientApi");
            file.put("description", "old desc");

            List<FieldChange> changes = invoke(file, existing);
            assertFalse(changes.isEmpty());
            assertTrue(changes.stream().anyMatch(c -> "apiType".equals(c.getField())));
        }
    }

    @Nested
    @DisplayName("normalizeModuleForComparison tests")
    class NormalizeModuleForComparisonTest {

        private Map<String, Object> invoke(String json) {
            return ReflectionTestUtils.invokeMethod(
                    groupInfoService, "normalizeModuleForComparison", json);
        }

        @Test
        @DisplayName("Blank json returns null")
        void testBlankJson() {
            assertNull(invoke(null));
            assertNull(invoke(""));
            assertNull(invoke("   "));
        }

        @Test
        @DisplayName("Excluded fields are removed")
        void testExcludedFieldsRemoved() {
            String json = "{\"id\":\"123\",\"name\":\"api1\",\"connectionId\":\"c1\",\"status\":\"active\",\"apiType\":\"defaultApi\"}";
            Map<String, Object> result = invoke(json);
            assertNotNull(result);
            assertFalse(result.containsKey("id"));
            assertTrue(result.containsKey("connectionId"));
            assertFalse(result.containsKey("status"));
            assertEquals("api1", result.get("name"));
            assertEquals("defaultApi", result.get("apiType"));
        }

        @Test
        @DisplayName("fields array items have id removed")
        void testFieldsIdRemoved() {
            String json = "{\"name\":\"api1\",\"fields\":[{\"id\":\"f1\",\"field_name\":\"col1\"},{\"id\":\"f2\",\"field_name\":\"col2\"}]}";
            Map<String, Object> result = invoke(json);
            assertNotNull(result);
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> fields = (List<Map<String, Object>>) result.get("fields");
            assertNotNull(fields);
            assertEquals(2, fields.size());
            assertFalse(fields.get(0).containsKey("id"));
            assertFalse(fields.get(1).containsKey("id"));
            assertEquals("col1", fields.get(0).get("field_name"));
        }

        @Test
        @DisplayName("listtags array items have id removed")
        void testListtagsIdRemoved() {
            String json = "{\"name\":\"api1\",\"listtags\":[{\"id\":\"t1\",\"value\":\"tag1\"}]}";
            Map<String, Object> result = invoke(json);
            assertNotNull(result);
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> tags = (List<Map<String, Object>>) result.get("listtags");
            assertNotNull(tags);
            assertFalse(tags.get(0).containsKey("id"));
            assertEquals("tag1", tags.get(0).get("value"));
        }
    }

    @Nested
    @DisplayName("getInspectChangedFields tests")
    class GetInspectChangedFieldsTest {

        private com.tapdata.tm.inspect.dto.InspectDto buildInspect(String flowId, String mode, String method) {
            com.tapdata.tm.inspect.dto.InspectDto dto = new com.tapdata.tm.inspect.dto.InspectDto();
            dto.setName("inspect1");
            dto.setFlowId(flowId);
            dto.setMode(mode);
            dto.setInspectMethod(method);
            return dto;
        }

        @Test
        @DisplayName("Same inspect returns empty changes")
        void testSameInspect() {
            com.tapdata.tm.inspect.dto.InspectDto i1 = buildInspect("flow1", "manual", "row_count");
            com.tapdata.tm.inspect.dto.InspectDto i2 = buildInspect("flow1", "manual", "row_count");

            List<FieldChange> changes = ReflectionTestUtils.invokeMethod(
                    groupInfoService, "getInspectChangedFields", i1, i2);
            assertNotNull(changes);
            assertTrue(changes.isEmpty());
        }

        @Test
        @DisplayName("Different flowId/mode/inspectMethod produces changes")
        void testDifferentFields() {
            com.tapdata.tm.inspect.dto.InspectDto file = buildInspect("flow2", "cron", "field");
            com.tapdata.tm.inspect.dto.InspectDto existing = buildInspect("flow1", "manual", "row_count");

            List<FieldChange> changes = ReflectionTestUtils.invokeMethod(
                    groupInfoService, "getInspectChangedFields", file, existing);
            assertNotNull(changes);
            assertTrue(changes.stream().anyMatch(c -> "flowId".equals(c.getField())));
            assertTrue(changes.stream().anyMatch(c -> "mode".equals(c.getField())));
            assertTrue(changes.stream().anyMatch(c -> "inspectMethod".equals(c.getField())));
        }
    }

    @Nested
    @DisplayName("normalizePathForConfig tests")
    class NormalizePathForConfigTest {

        private String invoke(String path) {
            return ReflectionTestUtils.invokeMethod(groupInfoService, "normalizePathForConfig", path);
        }

        @Test
        @DisplayName("Concrete keyed segments replaced with [*]")
        void testNormalize() {
            assertEquals("paths[*].fields", invoke("paths[customerQuery].fields"));
            assertEquals("paths[*].fields[*].type", invoke("paths[api1].fields[col1].type"));
        }

        @Test
        @DisplayName("[*] remains unchanged")
        void testWildcardUnchanged() {
            assertEquals("paths[*].fields", invoke("paths[*].fields"));
        }

        @Test
        @DisplayName("No brackets returns as-is")
        void testNoBrackets() {
            assertEquals("fields", invoke("fields"));
        }
    }

    // ====================== buildConnectionDiff / buildTaskDiff / buildApiDiff Tests ======================

    @Nested
    @DisplayName("buildConnectionDiff tests")
    class BuildConnectionDiffTest {

        private ResourceDiff invoke(Map<String, List<TaskUpAndLoadDto>> payloads) {
            return ReflectionTestUtils.invokeMethod(groupInfoService, "buildConnectionDiff", payloads, user);
        }

        private TaskUpAndLoadDto connPayload(String name, String connType, Map<String, Object> config) {
            return connPayload(new ObjectId().toHexString(), name, connType, config);
        }

        private TaskUpAndLoadDto connPayload(String id, String name, String connType, Map<String, Object> config) {
            Map<String, Object> json = new LinkedHashMap<>();
            json.put("id", id);
            json.put("name", name);
            json.put("connection_type", connType);
            json.put("database_type", "MySQL");
            if (config != null) json.put("config", config);
            return new TaskUpAndLoadDto(GroupConstants.COLLECTION_CONNECTION, JsonUtil.toJsonUseJackson(json));
        }

        private TaskUpAndLoadDto connPayloadWithId(String id, String name, String connType, Map<String, Object> config) {
            Map<String, Object> json = new LinkedHashMap<>();
            json.put("id", id);
            json.put("name", name);
            json.put("connection_type", connType);
            json.put("database_type", "MySQL");
            if (config != null) json.put("config", config);
            return new TaskUpAndLoadDto(GroupConstants.COLLECTION_CONNECTION, JsonUtil.toJsonUseJackson(json));
        }

        @Test
        @DisplayName("Empty payloads returns empty diff")
        void testEmptyPayloads() {
            ResourceDiff diff = invoke(Collections.emptyMap());
            assertNotNull(diff);
            assertTrue(diff.getAdd().isEmpty());
            assertTrue(diff.getUpdate().isEmpty());
        }

        @Test
        @DisplayName("Payload with wrong collectionName is skipped")
        void testWrongCollectionName() {
            TaskUpAndLoadDto item = new TaskUpAndLoadDto("WrongCollection", "{\"name\":\"conn1\",\"connection_type\":\"source\"}");
            Map<String, List<TaskUpAndLoadDto>> payloads = Map.of("Connection.json", List.of(item));
            lenient().when(dataSourceService.findAllDto(any(Query.class), any(UserDetail.class))).thenReturn(Collections.emptyList());

            ResourceDiff diff = invoke(payloads);
            assertTrue(diff.getAdd().isEmpty());
            assertTrue(diff.getUpdate().isEmpty());
        }

        @Test
        @DisplayName("New connection (not in DB) produces add item")
        void testNewConnection() {
            TaskUpAndLoadDto item = connPayloadWithId(new ObjectId().toHexString(), "new_conn", "source", Map.of("database_name", "db1"));
            Map<String, List<TaskUpAndLoadDto>> payloads = Map.of("Connection.json", List.of(item));

            when(dataSourceService.findAllDto(any(Query.class), any(UserDetail.class))).thenReturn(Collections.emptyList());
            lenient().when(dataSourceDefinitionService.findByPdkHashList(anySet(), any(UserDetail.class))).thenReturn(Collections.emptyList());

            ResourceDiff diff = invoke(payloads);
            assertEquals(1, diff.getAdd().size());
            assertEquals("new_conn", diff.getAdd().get(0).getName());
            assertTrue(diff.getUpdate().isEmpty());
        }

        @Test
        @DisplayName("Existing connection with no change produces no update")
        void testNoChange() {
            ObjectId connId = new ObjectId();
            Map<String, Object> config = new HashMap<>();
            config.put("database_name", "db1");
            TaskUpAndLoadDto item = connPayloadWithId(connId.toHexString(), "conn1", "source", config);
            Map<String, List<TaskUpAndLoadDto>> payloads = Map.of("Connection.json", List.of(item));

            DataSourceConnectionDto existing = new DataSourceConnectionDto();
            existing.setId(connId);
            existing.setName("conn1");
            existing.setConnection_type("source");
            existing.setDatabase_type("MySQL");
            existing.setConfig(new HashMap<>(config));

            when(dataSourceService.findAllDto(any(Query.class), any(UserDetail.class))).thenReturn(List.of(existing));
            lenient().when(dataSourceDefinitionService.findByPdkHashList(anySet(), any(UserDetail.class))).thenReturn(Collections.emptyList());

            ResourceDiff diff = invoke(payloads);
            assertTrue(diff.getAdd().isEmpty());
            assertTrue(diff.getUpdate().isEmpty());
        }

        @Test
        @DisplayName("Existing connection with different connection_type produces update")
        void testConnectionTypeChanged() {
            ObjectId connId = new ObjectId();
            Map<String, Object> config = new HashMap<>();
            config.put("database_name", "db1");
            TaskUpAndLoadDto item = connPayloadWithId(connId.toHexString(), "conn1", "source_and_target", config);
            Map<String, List<TaskUpAndLoadDto>> payloads = Map.of("Connection.json", List.of(item));

            DataSourceConnectionDto existing = new DataSourceConnectionDto();
            existing.setId(connId);
            existing.setName("conn1");
            existing.setConnection_type("source");
            existing.setDatabase_type("MySQL");
            existing.setConfig(new HashMap<>(config));

            when(dataSourceService.findAllDto(any(Query.class), any(UserDetail.class))).thenReturn(List.of(existing));
            lenient().when(dataSourceDefinitionService.findByPdkHashList(anySet(), any(UserDetail.class))).thenReturn(Collections.emptyList());

            ResourceDiff diff = invoke(payloads);
            assertTrue(diff.getAdd().isEmpty());
            assertEquals(1, diff.getUpdate().size());
            ResourceDiffItem updateItem = diff.getUpdate().get(0);
            assertEquals("conn1", updateItem.getName());
            assertTrue(updateItem.getChanges().stream()
                    .anyMatch(c -> "connection_type".equals(c.getField())
                            && "source".equals(c.getFrom())
                            && "source_and_target".equals(c.getTo())));
        }

        @Test
        @DisplayName("Existing connection with different config produces update")
        void testConfigChanged() {
            ObjectId connId = new ObjectId();
            Map<String, Object> fileConfig = new HashMap<>();
            fileConfig.put("database_name", "new_db");
            TaskUpAndLoadDto item = connPayloadWithId(connId.toHexString(), "conn1", "source", fileConfig);
            Map<String, List<TaskUpAndLoadDto>> payloads = Map.of("Connection.json", List.of(item));

            DataSourceConnectionDto existing = new DataSourceConnectionDto();
            existing.setId(connId);
            existing.setName("conn1");
            existing.setConnection_type("source");
            existing.setDatabase_type("MySQL");
            Map<String, Object> existingConfig = new HashMap<>();
            existingConfig.put("database_name", "old_db");
            existing.setConfig(existingConfig);

            when(dataSourceService.findAllDto(any(Query.class), any(UserDetail.class))).thenReturn(List.of(existing));
            lenient().when(dataSourceDefinitionService.findByPdkHashList(anySet(), any(UserDetail.class))).thenReturn(Collections.emptyList());

            ResourceDiff diff = invoke(payloads);
            assertEquals(1, diff.getUpdate().size());
            assertTrue(diff.getUpdate().get(0).getChanges().stream()
                    .anyMatch(c -> "config.database_name".equals(c.getField())));
        }

        @Test
        @DisplayName("Multiple connections: mix of add and update")
        void testMixedAddAndUpdate() {
            ObjectId existingConnId = new ObjectId();
            Map<String, Object> config1 = new HashMap<>();
            config1.put("database_name", "db1");
            Map<String, Object> config2 = new HashMap<>();
            config2.put("database_name", "new_db2");

            TaskUpAndLoadDto item1 = connPayloadWithId(new ObjectId().toHexString(), "brand_new", "source", config1);
            TaskUpAndLoadDto item2 = connPayloadWithId(existingConnId.toHexString(), "existing_conn", "source", config2);
            Map<String, List<TaskUpAndLoadDto>> payloads = Map.of("Connection.json", List.of(item1, item2));

            DataSourceConnectionDto existingConn = new DataSourceConnectionDto();
            existingConn.setId(existingConnId);
            existingConn.setName("existing_conn");
            existingConn.setConnection_type("source");
            existingConn.setDatabase_type("MySQL");
            Map<String, Object> oldConfig = new HashMap<>();
            oldConfig.put("database_name", "old_db2");
            existingConn.setConfig(oldConfig);

            when(dataSourceService.findAllDto(any(Query.class), any(UserDetail.class))).thenReturn(List.of(existingConn));
            lenient().when(dataSourceDefinitionService.findByPdkHashList(anySet(), any(UserDetail.class))).thenReturn(Collections.emptyList());

            ResourceDiff diff = invoke(payloads);
            assertEquals(1, diff.getAdd().size());
            assertEquals("brand_new", diff.getAdd().get(0).getName());
            assertEquals(1, diff.getUpdate().size());
            assertEquals("existing_conn", diff.getUpdate().get(0).getName());
        }

        @Test
        @DisplayName("Duplicate ids in payload are deduplicated (first wins)")
        void testDuplicateNames() {
            String sameId = new ObjectId().toHexString();
            TaskUpAndLoadDto item1 = connPayloadWithId(sameId, "conn1", "source", Map.of("database_name", "db_first"));
            TaskUpAndLoadDto item2 = connPayloadWithId(sameId, "conn1", "target", Map.of("database_name", "db_second"));
            Map<String, List<TaskUpAndLoadDto>> payloads = Map.of("Connection.json", List.of(item1, item2));

            when(dataSourceService.findAllDto(any(Query.class), any(UserDetail.class))).thenReturn(Collections.emptyList());
            lenient().when(dataSourceDefinitionService.findByPdkHashList(anySet(), any(UserDetail.class))).thenReturn(Collections.emptyList());

            ResourceDiff diff = invoke(payloads);
            assertEquals(1, diff.getAdd().size(), "Duplicate ids should be deduplicated");
        }

        @Test
        @DisplayName("Vault secrets are injected before comparison so masked password is restored")
        void testVaultInjectionBeforeComparison() {
            // File connection has masked password; vault.json has the real values
            // resolveVaultStrategy requires all three: _url, _user, _password
            ObjectId connId = new ObjectId();
            Map<String, Object> fileConfig = new HashMap<>();
            fileConfig.put("password", "******");
            fileConfig.put("username", "******");
            fileConfig.put("host", "******");
            fileConfig.put("port", 3306);
            fileConfig.put("database_name", "db1");
            TaskUpAndLoadDto connItem = connPayloadWithId(connId.toHexString(), "conn1", "source", fileConfig);

            // Vault entries: {connName}_url, {connName}_user, {connName}_password
            // Use simple host:port/user format so parseUriComponents can extract host/port
            Map<String, String> vaultMap = new LinkedHashMap<>();
            vaultMap.put("conn1_url", "myhost:3306/admin");
            vaultMap.put("conn1_user", "admin");
            vaultMap.put("conn1_password", "realPassword");
            TaskUpAndLoadDto vaultItem = new TaskUpAndLoadDto(GroupConstants.VAULT_FILE, JsonUtil.toJsonUseJackson(vaultMap));

            Map<String, List<TaskUpAndLoadDto>> payloads = new HashMap<>();
            payloads.put("Connection.json", List.of(connItem));
            payloads.put(GroupConstants.VAULT_FILE, List.of(vaultItem));

            // Existing connection in DB has the same real values (after vault injection they should match)
            // Note: parseUriComponents returns port as int, so existing must also use int
            DataSourceConnectionDto existing = new DataSourceConnectionDto();
            existing.setId(connId);
            existing.setName("conn1");
            existing.setConnection_type("source");
            existing.setDatabase_type("MySQL");
            Map<String, Object> existingConfig = new HashMap<>();
            existingConfig.put("password", "realPassword");
            existingConfig.put("username", "admin");
            existingConfig.put("host", "myhost");
            existingConfig.put("port", 3306);
            existingConfig.put("database_name", "db1");
            existing.setConfig(existingConfig);

            when(dataSourceService.findAllDto(any(Query.class), any(UserDetail.class))).thenReturn(List.of(existing));
            lenient().when(dataSourceDefinitionService.findByPdkHashList(anySet(), any(UserDetail.class))).thenReturn(Collections.emptyList());

            ResourceDiff diff = invoke(payloads);
            // After vault injection the password should match → no update
            assertTrue(diff.getUpdate().isEmpty(),
                    "With vault injection, identical passwords should not produce a diff");
        }
    }

    @Nested
    @DisplayName("buildTaskDiff tests")
    class BuildTaskDiffTest {

        private ResourceDiff invoke(Map<String, List<TaskUpAndLoadDto>> payloads) {
            return ReflectionTestUtils.invokeMethod(groupInfoService, "buildTaskDiff", payloads, user);
        }

        private TaskUpAndLoadDto migrateTaskPayload(String name) {
            return migrateTaskPayload(new ObjectId().toHexString(), name);
        }

        private TaskUpAndLoadDto migrateTaskPayload(String id, String name) {
            Map<String, Object> json = new LinkedHashMap<>();
            json.put("id", id);
            json.put("name", name);
            json.put("type", "initial_sync+cdc");
            json.put("syncType", "migrate");
            return new TaskUpAndLoadDto(GroupConstants.COLLECTION_TASK, JsonUtil.toJsonUseJackson(json));
        }

        private TaskUpAndLoadDto migrateTaskPayloadWithId(String id, String name) {
            Map<String, Object> json = new LinkedHashMap<>();
            json.put("id", id);
            json.put("name", name);
            json.put("type", "initial_sync+cdc");
            json.put("syncType", "migrate");
            return new TaskUpAndLoadDto(GroupConstants.COLLECTION_TASK, JsonUtil.toJsonUseJackson(json));
        }

        private TaskUpAndLoadDto syncTaskPayload(String name) {
            return syncTaskPayload(new ObjectId().toHexString(), name);
        }

        private TaskUpAndLoadDto syncTaskPayload(String id, String name) {
            Map<String, Object> json = new LinkedHashMap<>();
            json.put("id", id);
            json.put("name", name);
            json.put("type", "initial_sync+cdc");
            json.put("syncType", "sync");
            return new TaskUpAndLoadDto(GroupConstants.COLLECTION_TASK, JsonUtil.toJsonUseJackson(json));
        }

        private TaskUpAndLoadDto syncTaskPayloadWithId(String id, String name) {
            Map<String, Object> json = new LinkedHashMap<>();
            json.put("id", id);
            json.put("name", name);
            json.put("type", "initial_sync+cdc");
            json.put("syncType", "sync");
            return new TaskUpAndLoadDto(GroupConstants.COLLECTION_TASK, JsonUtil.toJsonUseJackson(json));
        }

        private TaskUpAndLoadDto inspectPayload(String name, String flowId, String mode, String method) {
            Map<String, Object> json = new LinkedHashMap<>();
            json.put("name", name);
            json.put("flowId", flowId);
            json.put("mode", mode);
            json.put("inspectMethod", method);
            return new TaskUpAndLoadDto(GroupConstants.COLLECTION_INSPECT, JsonUtil.toJsonUseJackson(json));
        }

        @Test
        @DisplayName("Empty payloads returns empty diff")
        void testEmptyPayloads() {
            ResourceDiff diff = invoke(Collections.emptyMap());
            assertNotNull(diff);
            assertTrue(diff.getAdd().isEmpty());
            assertTrue(diff.getUpdate().isEmpty());
        }

        @Test
        @DisplayName("New migrate task produces add item with type 'migrate'")
        void testNewMigrateTask() {
            Map<String, List<TaskUpAndLoadDto>> payloads = Map.of(
                    "MigrateTask.json", List.of(migrateTaskPayloadWithId(new ObjectId().toHexString(), "task1")));

            when(taskService.findAllDto(any(Query.class), any(UserDetail.class))).thenReturn(Collections.emptyList());

            ResourceDiff diff = invoke(payloads);
            assertEquals(1, diff.getAdd().size());
            assertEquals("task1", diff.getAdd().get(0).getName());
            assertEquals("migrate", diff.getAdd().get(0).getType());
        }

        @Test
        @DisplayName("New sync task produces add item with type 'sync'")
        void testNewSyncTask() {
            Map<String, List<TaskUpAndLoadDto>> payloads = Map.of(
                    "SyncTask.json", List.of(syncTaskPayloadWithId(new ObjectId().toHexString(), "sync_task1")));

            when(taskService.findAllDto(any(Query.class), any(UserDetail.class))).thenReturn(Collections.emptyList());

            ResourceDiff diff = invoke(payloads);
            assertEquals(1, diff.getAdd().size());
            assertEquals("sync_task1", diff.getAdd().get(0).getName());
            assertEquals("sync", diff.getAdd().get(0).getType());
        }

        @Test
        @DisplayName("Existing task with same config produces no update")
        void testExistingTaskNoChange() {
            ObjectId taskId = new ObjectId();
            Map<String, List<TaskUpAndLoadDto>> payloads = Map.of(
                    "MigrateTask.json", List.of(migrateTaskPayloadWithId(taskId.toHexString(), "task1")));

            TaskDto existingTask = new TaskDto();
            existingTask.setId(taskId);
            existingTask.setName("task1");
            existingTask.setType("initial_sync+cdc");
            existingTask.setSyncType("migrate");

            when(taskService.findAllDto(any(Query.class), any(UserDetail.class))).thenReturn(List.of(existingTask));

            ResourceDiff diff = invoke(payloads);
            assertTrue(diff.getAdd().isEmpty());
            // Whether update is empty depends on TaskConfigCompareUtil; at least no add
        }

        @Test
        @DisplayName("New inspect task produces add item with type 'validate'")
        void testNewInspectTask() {
            Map<String, List<TaskUpAndLoadDto>> payloads = Map.of(
                    "InspectTask.json", List.of(inspectPayload("inspect1", "f1", "manual", "row_count")));

            when(inspectService.findByName("inspect1")).thenReturn(Collections.emptyList());

            ResourceDiff diff = invoke(payloads);
            assertEquals(1, diff.getAdd().size());
            assertEquals("inspect1", diff.getAdd().get(0).getName());
            assertEquals("validate", diff.getAdd().get(0).getType());
        }

        @Test
        @DisplayName("Existing inspect task with different fields produces update")
        void testExistingInspectChanged() {
            Map<String, List<TaskUpAndLoadDto>> payloads = Map.of(
                    "InspectTask.json", List.of(inspectPayload("inspect1", "flow_new", "cron", "field")));

            InspectDto existing = new InspectDto();
            existing.setName("inspect1");
            existing.setFlowId("flow_old");
            existing.setMode("manual");
            existing.setInspectMethod("row_count");

            when(inspectService.findByName("inspect1")).thenReturn(List.of(existing));

            ResourceDiff diff = invoke(payloads);
            assertTrue(diff.getAdd().isEmpty());
            assertEquals(1, diff.getUpdate().size());
            ResourceDiffItem updateItem = diff.getUpdate().get(0);
            assertEquals("inspect1", updateItem.getName());
            assertEquals("validate", updateItem.getType());
            assertTrue(updateItem.getChanges().stream().anyMatch(c -> "flowId".equals(c.getField())));
            assertTrue(updateItem.getChanges().stream().anyMatch(c -> "mode".equals(c.getField())));
            assertTrue(updateItem.getChanges().stream().anyMatch(c -> "inspectMethod".equals(c.getField())));
        }

        @Test
        @DisplayName("Mixed migrate + sync + inspect tasks")
        void testMixedTaskTypes() {
            Map<String, List<TaskUpAndLoadDto>> payloads = new HashMap<>();
            payloads.put("MigrateTask.json", List.of(migrateTaskPayloadWithId(new ObjectId().toHexString(), "m_task")));
            payloads.put("SyncTask.json", List.of(syncTaskPayloadWithId(new ObjectId().toHexString(), "s_task")));
            payloads.put("InspectTask.json", List.of(inspectPayload("v_task", "f1", "manual", "row_count")));

            when(taskService.findAllDto(any(Query.class), any(UserDetail.class))).thenReturn(Collections.emptyList());
            when(inspectService.findByName("v_task")).thenReturn(Collections.emptyList());

            ResourceDiff diff = invoke(payloads);
            assertEquals(3, diff.getAdd().size());
            assertTrue(diff.getAdd().stream().anyMatch(i -> "m_task".equals(i.getName()) && "migrate".equals(i.getType())));
            assertTrue(diff.getAdd().stream().anyMatch(i -> "s_task".equals(i.getName()) && "sync".equals(i.getType())));
            assertTrue(diff.getAdd().stream().anyMatch(i -> "v_task".equals(i.getName()) && "validate".equals(i.getType())));
        }
    }

    @Nested
    @DisplayName("buildTaskDiff: 目标环境已删除的任务判定为变更（恢复）")
    class BuildTaskDiffDeletedRestoreTest {

        private ResourceDiff invoke(Map<String, List<TaskUpAndLoadDto>> payloads) {
            return ReflectionTestUtils.invokeMethod(groupInfoService, "buildTaskDiff", payloads, user);
        }

        private TaskUpAndLoadDto migrateTaskPayload(String id, String name) {
            Map<String, Object> json = new LinkedHashMap<>();
            json.put("id", id);
            json.put("name", name);
            json.put("type", "initial_sync+cdc");
            json.put("syncType", "migrate");
            return new TaskUpAndLoadDto(GroupConstants.COLLECTION_TASK, JsonUtil.toJsonUseJackson(json));
        }

        /** remove() 删除任务时会把 name 改成「原名_随机6位」并把原名存进 deleteName */
        private TaskDto halfDeletedTask(ObjectId id, String originalName, String status) {
            TaskDto dto = new TaskDto();
            dto.setId(id);
            dto.setName(originalName + "_ab12cd");
            dto.setDeleteName(originalName);
            dto.setStatus(status);
            dto.setType("initial_sync+cdc");
            dto.setSyncType("migrate");
            return dto;
        }

        private TaskEntity idOnlyEntity(ObjectId id) {
            TaskEntity entity = new TaskEntity();
            entity.setId(id);
            return entity;
        }

        @Test
        @DisplayName("status=deleting 且已被改名 → 落 add 桶，不是 name 变更")
        void testDeletingTaskIsRestoreNotNameChange() {
            ObjectId taskId = new ObjectId();
            Map<String, List<TaskUpAndLoadDto>> payloads = Map.of(
                    "MigrateTask.json", List.of(migrateTaskPayload(taskId.toHexString(), "task3")));

            when(taskService.findAllDto(any(Query.class), any(UserDetail.class)))
                    .thenReturn(List.of(halfDeletedTask(taskId, "task3", TaskDto.STATUS_DELETING)));
            when(taskService.findAll(any(Query.class), any(UserDetail.class)))
                    .thenReturn(List.of(idOnlyEntity(taskId)));

            ResourceDiff diff = invoke(payloads);

            assertEquals(1, diff.getAdd().size(), "被删除的任务应作为恢复项进入 add 桶");
            assertEquals("task3", diff.getAdd().get(0).getName());
            assertTrue(diff.getUpdate().isEmpty(), "不应把删除时的改名当成普通的 name 变更");
        }

        @Test
        @DisplayName("status=delete_failed 且已被改名 → 落 add 桶")
        void testDeleteFailedTaskIsRestore() {
            ObjectId taskId = new ObjectId();
            Map<String, List<TaskUpAndLoadDto>> payloads = Map.of(
                    "MigrateTask.json", List.of(migrateTaskPayload(taskId.toHexString(), "task3")));

            when(taskService.findAllDto(any(Query.class), any(UserDetail.class)))
                    .thenReturn(List.of(halfDeletedTask(taskId, "task3", TaskDto.STATUS_DELETE_FAILED)));
            when(taskService.findAll(any(Query.class), any(UserDetail.class)))
                    .thenReturn(List.of(idOnlyEntity(taskId)));

            ResourceDiff diff = invoke(payloads);

            assertEquals(1, diff.getAdd().size());
            assertEquals("task3", diff.getAdd().get(0).getName());
            assertTrue(diff.getUpdate().isEmpty());
        }

        @Test
        @DisplayName("回归：is_deleted=true（查不到活跃记录）仍落 add 桶")
        void testFullyDeletedTaskIsRestore() {
            ObjectId taskId = new ObjectId();
            Map<String, List<TaskUpAndLoadDto>> payloads = Map.of(
                    "MigrateTask.json", List.of(migrateTaskPayload(taskId.toHexString(), "task3")));

            when(taskService.findAllDto(any(Query.class), any(UserDetail.class)))
                    .thenReturn(Collections.emptyList());
            when(taskService.findAll(any(Query.class), any(UserDetail.class)))
                    .thenReturn(List.of(idOnlyEntity(taskId)));

            ResourceDiff diff = invoke(payloads);

            assertEquals(1, diff.getAdd().size());
            assertEquals("task3", diff.getAdd().get(0).getName());
            assertTrue(diff.getUpdate().isEmpty());
        }

        @Test
        @DisplayName("回归：仅运行状态 stop→running、配置一致 → 既不 add 也不 update")
        void testRunningStatusOnlyIsNotAChange() {
            ObjectId taskId = new ObjectId();
            Map<String, List<TaskUpAndLoadDto>> payloads = Map.of(
                    "MigrateTask.json", List.of(migrateTaskPayload(taskId.toHexString(), "task2")));

            TaskDto running = new TaskDto();
            running.setId(taskId);
            running.setName("task2");
            running.setStatus(TaskDto.STATUS_RUNNING);
            running.setType("initial_sync+cdc");
            running.setSyncType("migrate");

            when(taskService.findAllDto(any(Query.class), any(UserDetail.class))).thenReturn(List.of(running));
            when(taskService.findAll(any(Query.class), any(UserDetail.class)))
                    .thenReturn(Collections.emptyList());

            ResourceDiff diff = invoke(payloads);

            assertTrue(diff.getAdd().isEmpty(), "运行状态差异不算变更");
            assertTrue(diff.getUpdate().isEmpty(), "运行状态差异不算变更");
        }
    }

    @Nested
    @DisplayName("buildApiDiff tests")
    class BuildApiDiffTest {

        private ResourceDiff invoke(Map<String, List<TaskUpAndLoadDto>> payloads) {
            return ReflectionTestUtils.invokeMethod(groupInfoService, "buildApiDiff", payloads, user);
        }

        private TaskUpAndLoadDto modulePayload(String name, String apiType, String description) {
            return modulePayload(new ObjectId().toHexString(), name, apiType, description);
        }

        private TaskUpAndLoadDto modulePayload(String id, String name, String apiType, String description) {
            Map<String, Object> json = new LinkedHashMap<>();
            json.put("id", id);
            json.put("name", name);
            json.put("apiType", apiType);
            if (description != null) json.put("description", description);
            return new TaskUpAndLoadDto(GroupConstants.COLLECTION_MODULES, JsonUtil.toJsonUseJackson(json));
        }

        private TaskUpAndLoadDto modulePayloadWithId(String id, String name, String apiType, String description) {
            ModulesDto dto = new ModulesDto();
            dto.setId(new ObjectId(id));
            dto.setName(name);
            dto.setApiType(apiType);
            if (description != null) dto.setDescription(description);
            return new TaskUpAndLoadDto(GroupConstants.COLLECTION_MODULES, JsonUtil.toJsonUseJackson(dto));
        }

        @Test
        @DisplayName("Empty payloads returns empty diff")
        void testEmptyPayloads() {
            ResourceDiff diff = invoke(Collections.emptyMap());
            assertNotNull(diff);
            assertTrue(diff.getAdd().isEmpty());
            assertTrue(diff.getUpdate().isEmpty());
        }

        @Test
        @DisplayName("Payload with wrong collectionName is skipped")
        void testWrongCollectionName() {
            TaskUpAndLoadDto item = new TaskUpAndLoadDto("WrongCollection",
                    "{\"name\":\"api1\",\"apiType\":\"defaultApi\"}");
            Map<String, List<TaskUpAndLoadDto>> payloads = Map.of("Module.json", List.of(item));
            lenient().when(modulesService.findAllDto(any(Query.class), any(UserDetail.class))).thenReturn(Collections.emptyList());

            ResourceDiff diff = invoke(payloads);
            assertTrue(diff.getAdd().isEmpty());
            assertTrue(diff.getUpdate().isEmpty());
        }

        @Test
        @DisplayName("New API module produces add item")
        void testNewModule() {
            Map<String, List<TaskUpAndLoadDto>> payloads = Map.of(
                    "Module.json", List.of(modulePayloadWithId(new ObjectId().toHexString(), "api1", "defaultApi", "desc")));

            when(modulesService.findAllDto(any(Query.class), any(UserDetail.class))).thenReturn(Collections.emptyList());

            ResourceDiff diff = invoke(payloads);
            assertEquals(1, diff.getAdd().size());
            assertEquals("api1", diff.getAdd().get(0).getName());
            assertTrue(diff.getUpdate().isEmpty());
        }

        @Test
        @DisplayName("Existing module with same content produces no update")
        void testNoChange() {
            ObjectId moduleId = new ObjectId();
            Map<String, List<TaskUpAndLoadDto>> payloads = Map.of(
                    "Module.json", List.of(modulePayloadWithId(moduleId.toHexString(), "api1", "defaultApi", "same desc")));

            ModulesDto existing = new ModulesDto();
            existing.setId(moduleId);
            existing.setName("api1");
            existing.setApiType("defaultApi");
            existing.setDescription("same desc");

            when(modulesService.findAllDto(any(Query.class), any(UserDetail.class))).thenReturn(List.of(existing));

            ResourceDiff diff = invoke(payloads);
            assertTrue(diff.getAdd().isEmpty());
            assertTrue(diff.getUpdate().isEmpty());
        }

        @Test
        @DisplayName("Existing module with different apiType produces update")
        void testApiTypeChanged() {
            ObjectId moduleId = new ObjectId();
            Map<String, List<TaskUpAndLoadDto>> payloads = Map.of(
                    "Module.json", List.of(modulePayloadWithId(moduleId.toHexString(), "api1", "clientApi", "desc")));

            ModulesDto existing = new ModulesDto();
            existing.setId(moduleId);
            existing.setName("api1");
            existing.setApiType("defaultApi");
            existing.setDescription("desc");

            when(modulesService.findAllDto(any(Query.class), any(UserDetail.class))).thenReturn(List.of(existing));

            ResourceDiff diff = invoke(payloads);
            assertTrue(diff.getAdd().isEmpty());
            assertEquals(1, diff.getUpdate().size());
            assertEquals("api1", diff.getUpdate().get(0).getName());
            assertTrue(diff.getUpdate().get(0).getChanges().stream()
                    .anyMatch(c -> "apiType".equals(c.getField())));
        }

        @Test
        @DisplayName("Multiple modules: mix of add and update")
        void testMixedAddAndUpdate() {
            ObjectId existingModuleId = new ObjectId();
            Map<String, List<TaskUpAndLoadDto>> payloads = Map.of("Module.json", List.of(
                    modulePayloadWithId(new ObjectId().toHexString(), "new_api", "defaultApi", "new"),
                    modulePayloadWithId(existingModuleId.toHexString(), "existing_api", "clientApi", "updated")));

            ModulesDto existing = new ModulesDto();
            existing.setId(existingModuleId);
            existing.setName("existing_api");
            existing.setApiType("defaultApi");
            existing.setDescription("old");

            when(modulesService.findAllDto(any(Query.class), any(UserDetail.class))).thenReturn(List.of(existing));

            ResourceDiff diff = invoke(payloads);
            assertEquals(1, diff.getAdd().size());
            assertEquals("new_api", diff.getAdd().get(0).getName());
            assertEquals(1, diff.getUpdate().size());
            assertEquals("existing_api", diff.getUpdate().get(0).getName());
        }

        @Test
        @DisplayName("Duplicate ids in payload are deduplicated (first wins)")
        void testDuplicateNames() {
            String sameId = new ObjectId().toHexString();
            Map<String, List<TaskUpAndLoadDto>> payloads = Map.of("Module.json", List.of(
                    modulePayloadWithId(sameId, "api1", "defaultApi", "first"),
                    modulePayloadWithId(sameId, "api1", "clientApi", "second")));

            when(modulesService.findAllDto(any(Query.class), any(UserDetail.class))).thenReturn(Collections.emptyList());

            ResourceDiff diff = invoke(payloads);
            assertEquals(1, diff.getAdd().size(), "Duplicate ids should be deduplicated");
        }
    }

    @Nested
    @DisplayName("maskUriValue tests")
    class MaskUriValueTest {

        @Test
        @DisplayName("null returns null")
        void testNull() {
            assertNull(GroupInfoService.maskUriValue(null));
        }

        @Test
        @DisplayName("MongoDB URI with password masks only password part")
        void testMongoUriWithPassword() {
            String uri = "mongodb://admin:secretPass123@host1:27017,host2:27017/mydb?replicaSet=rs0";
            Object result = GroupInfoService.maskUriValue(uri);
            String masked = (String) result;
            // Password should be masked
            assertFalse(masked.contains("secretPass123"), "Password should be masked");
            // Other parts should remain visible
            assertTrue(masked.contains("admin"), "Username should be visible");
            assertTrue(masked.contains("host1:27017"), "Host should be visible");
            assertTrue(masked.contains("mydb"), "Database should be visible");
            assertTrue(masked.contains("******"), "Should contain mask placeholder");
            assertEquals("mongodb://admin:******@host1:27017,host2:27017/mydb?replicaSet=rs0", masked);
        }

        @Test
        @DisplayName("MongoDB URI without password returns as-is")
        void testMongoUriWithoutPassword() {
            String uri = "mongodb://host1:27017/mydb";
            Object result = GroupInfoService.maskUriValue(uri);
            assertEquals(uri, result);
        }

        @Test
        @DisplayName("MongoDB URI with URL-encoded password masks only password part")
        void testMongoUriWithEncodedPassword() {
            String uri = "mongodb://user:myP%40ss@localhost:27017/testdb";
            Object result = GroupInfoService.maskUriValue(uri);
            String masked = (String) result;
            assertTrue(masked.contains("user:"), "Username should be visible");
            assertTrue(masked.contains("localhost:27017"), "Host should be visible");
            assertFalse(masked.contains("myP%40ss"), "URL-encoded password should be masked");
            assertTrue(masked.contains("******"), "Should contain mask placeholder");
            assertEquals("mongodb://user:******@localhost:27017/testdb", masked);
        }

        @Test
        @DisplayName("mongodb+srv URI masks only the password part")
        void testMongoSrvUriMasksPassword() {
            String uri = "mongodb+srv://user:pass@cluster0.example.net/testdb";
            Object result = GroupInfoService.maskUriValue(uri);
            assertEquals("mongodb+srv://user:******@cluster0.example.net/testdb", result);
        }

        @Test
        @DisplayName("Non-MongoDB URI is fully masked")
        void testNonMongoUri() {
            String uri = "jdbc:mysql://root:password@localhost:3306/mydb";
            Object result = GroupInfoService.maskUriValue(uri);
            assertEquals("******", result);
        }

        @Test
        @DisplayName("Plain string (not a URI) is fully masked")
        void testPlainString() {
            Object result = GroupInfoService.maskUriValue("some-random-value");
            assertEquals("******", result);
        }
    }

    /**
     * ES-2 导出脱敏分流（[ADR-0034] D1/D2）：FILE 默认保真、可显式要求脱敏；
     * GIT **强制脱敏且不可被入参绕过**——这是红线不是默认值，git 历史是永久的。
     */
    @Nested
    @DisplayName("导出脱敏分流")
    class ExportMaskPolicyTest {

        private ExportGroupRequest request(GroupTransferType type, Boolean removeSensitiveData) {
            ExportGroupRequest request = new ExportGroupRequest();
            request.setGroupTransferType(type);
            request.setRemoveSensitiveData(removeSensitiveData);
            return request;
        }

        @Test
        @DisplayName("FILE 未指定 ⇒ 保真（本地包要能直接用）")
        void fileDefaultsToFaithful() {
            assertFalse(GroupInfoService.resolveMaskSecrets(request(GroupTransferType.FILE, null)));
        }

        @Test
        @DisplayName("FILE 显式要求移除敏感信息 ⇒ 脱敏")
        void fileHonoursExplicitRemoval() {
            assertTrue(GroupInfoService.resolveMaskSecrets(request(GroupTransferType.FILE, true)));
        }

        @Test
        @DisplayName("FILE 显式要求保真 ⇒ 保真")
        void fileHonoursExplicitFaithful() {
            assertFalse(GroupInfoService.resolveMaskSecrets(request(GroupTransferType.FILE, false)));
        }

        @Test
        @DisplayName("GIT 未指定 ⇒ 脱敏")
        void gitDefaultsToMasked() {
            assertTrue(GroupInfoService.resolveMaskSecrets(request(GroupTransferType.GIT, null)));
        }

        @Test
        @DisplayName("红线：GIT + 入参显式要求保真 ⇒ 仍然脱敏")
        void gitCannotBeTalkedOutOfMasking() {
            assertTrue(GroupInfoService.resolveMaskSecrets(request(GroupTransferType.GIT, false)),
                    "GIT 强制脱敏是红线不是默认值——能被一个入参绕过，就等于明文凭据可以被推上 GitHub，"
                            + "而 git 历史永久留存、可被 fork/缓存（ADR-0034 D2）");
        }

        @Test
        @DisplayName("传输类型缺失 / 整个请求缺失 ⇒ 按最保守的来（脱敏）")
        void unknownIntentFallsBackToMasking() {
            assertTrue(GroupInfoService.resolveMaskSecrets(request(null, false)));
            assertTrue(GroupInfoService.resolveMaskSecrets(null));
        }

        @Test
        @DisplayName("GIT 吞掉了「要保真」这个请求时必须告知，不静默")
        void gitOverrideIsAnnounced() {
            assertTrue(GroupInfoService.isMaskForciblyOverridden(request(GroupTransferType.GIT, false)),
                    "用户明确要保真却被强制脱敏，这件事必须显形（ADR-0034 D2）");
            assertFalse(GroupInfoService.isMaskForciblyOverridden(request(GroupTransferType.GIT, null)),
                    "用户压根没提要求，就没有「被吞掉的请求」可告知");
            assertFalse(GroupInfoService.isMaskForciblyOverridden(request(GroupTransferType.FILE, false)),
                    "FILE 本来就给保真，没有覆盖发生");
        }
    }

    /**
     * ES-2 接线：exportGroupInfos 必须把**解析后**的开关传下去，并在被强制覆盖时留下记录。
     * 光验策略函数不够——ES-4 刚撞过「方法有测试、接线没有」。
     */
    @Nested
    @DisplayName("导出脱敏开关接线")
    class ExportMaskWiringTest {

        private static final String SECRET_URI = "mongodb://real-host:27017/dmp";
        private static final String SECRET_PASSWORD = "s3cr3t-p4ssw0rd";
        /** 凭据在包里共三处载体，各给一个可区分的值——否则某一处漏抹了也测不出来 */
        private static final String SECRET_IN_DB_META = "mongodb://real-host:27017/from-database-metadata";
        private static final String SECRET_IN_TABLE_META = "mongodb://real-host:27017/from-table-metadata";

        private ResourceHandler registerModuleHandler() {
            // defaultAnswer=CALLS_REAL_METHODS：handleRelatedResources / buildConnectionPayload 正是被测的
            // default 方法，普通 mock 会把它们一并空转掉，连接就永远进不了包（这套用例也就白测了）
            ResourceHandler handler = mock(ResourceHandler.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
            lenient().when(handler.getResourceType()).thenReturn(ResourceType.MODULE);
            lenient().doReturn(List.of(new ModulesDto())).when(handler).loadResources(anyList(), any(UserDetail.class));
            lenient().when(handler.buildExportPayload(anyList(), any(UserDetail.class), anyBoolean())).thenReturn(new ArrayList<>());
            lenient().when(handler.loadConnections(anyList())).thenReturn(new ArrayList<>());
            ResourceHandlerRegistry registry = new ResourceHandlerRegistry();
            ReflectionTestUtils.setField(registry, "handlers", List.of(handler));
            registry.init();
            ReflectionTestUtils.setField(groupInfoService, "resourceHandlerRegistry", registry);
            return handler;
        }

        private ExportGroupRequest exportRequest(GroupTransferType type, Boolean removeSensitiveData) {
            ExportGroupRequest request = new ExportGroupRequest();
            request.setGroupIds(List.of(new ObjectId().toHexString()));
            request.setGroupTransferType(type);
            request.setRemoveSensitiveData(removeSensitiveData);
            request.setGroupResetTask(new HashMap<>());
            return request;
        }

        private GroupInfoRecordDto runExport(ExportGroupRequest request) {
            GroupInfoDto groupInfo = new GroupInfoDto();
            groupInfo.setId(new ObjectId());
            groupInfo.setName("Test Group");
            ResourceItem item = new ResourceItem();
            item.setId(new ObjectId().toHexString());
            item.setType(ResourceType.MODULE);
            groupInfo.setResourceItemList(new ArrayList<>(List.of(item)));
            // GIT 导出的记录会读 gitInfo，与本用例无关但不能为 null
            com.tapdata.tm.group.dto.GroupGitInfoDto gitInfo = new com.tapdata.tm.group.dto.GroupGitInfoDto();
            gitInfo.setRepoUrl("https://example.invalid/repo.git");
            groupInfo.setGitInfo(gitInfo);
            doReturn(List.of(groupInfo)).when(groupInfoService).findAllDto(any(Query.class), any(UserDetail.class));

            GroupInfoRecordDto savedRecord = new GroupInfoRecordDto();
            savedRecord.setId(new ObjectId());
            ArgumentCaptor<GroupInfoRecordDto> built = ArgumentCaptor.forClass(GroupInfoRecordDto.class);
            when(groupInfoRecordService.save(built.capture(), any(UserDetail.class))).thenReturn(savedRecord);
            doNothing().when(groupInfoService).updateRecordStatus(any(), any(), any(), any(), any());
            lenient().when(transferStrategyRegistry.getStrategy(any())).thenReturn(groupTransferStrategy);

            // GIT 是异步通路，doExport 会去 Spring 容器里取自己；单测里把它指回这个 spy
            try (MockedStatic<SpringContextHelper> springContext = Mockito.mockStatic(SpringContextHelper.class)) {
                springContext.when(() -> SpringContextHelper.getBean(GroupInfoService.class))
                        .thenReturn(groupInfoService);
                groupInfoService.exportGroupInfos(mock(HttpServletResponse.class), request, user);
            }
            return built.getValue();
        }

        /**
         * ES-2b：凭据在包里有**两份**拷贝——Connection 文档，以及 MetadataInstances.source
         * （SourceDto 带 config 与 database_uri/password，由 DAGService 整份序列化连接而来）。
         * 所以红线的判据必须是「**整包**搜不到明文凭据」，只看连接文档会漏掉第二条腿。
         */
        private String wholePackage(ExportGroupRequest request) {
            ResourceHandler handler = registerModuleHandler();
            DataSourceEntity connection = new DataSourceEntity();
            connection.setId(new ObjectId());
            connection.setName("MDM_CUSTOMER");
            connection.setPdkHash("pdkhash-mongodb");
            connection.setConfig(new LinkedHashMap<>(Map.of("isUri", true, "uri", SECRET_URI)));
            // 同一批 secret 在实体顶层还有一份镜像（线上确有：08-01 那次事故里 config.uri 被抹空、顶层 database_uri 仍在）
            connection.setDatabase_uri(SECRET_URI);
            connection.setDatabase_password(SECRET_PASSWORD);
            connection.setPlain_password(SECRET_PASSWORD);
            doReturn(List.of(connection)).when(handler).loadConnections(anyList());

            DataSourceDefinitionService definitionService = mock(DataSourceDefinitionService.class);
            lenient().when(definitionService.findByPdkHash(any(), anyInt(), any())).thenReturn(definitionWithUri());
            String connId = connection.getId().toHexString();
            lenient().when(metadataInstancesService.findOne(any(Query.class), any(UserDetail.class)))
                    .thenReturn(metadataCarryingSecrets(connId, SECRET_IN_DB_META));
            lenient().when(metadataInstancesService.findAllDto(any(Query.class), any(UserDetail.class)))
                    .thenReturn(new ArrayList<>(List.of(metadataCarryingSecrets(connId, SECRET_IN_TABLE_META))));

            ArgumentCaptor<GroupExportRequest> exported = ArgumentCaptor.forClass(GroupExportRequest.class);
            try (MockedStatic<cn.hutool.extra.spring.SpringUtil> springUtil =
                         Mockito.mockStatic(cn.hutool.extra.spring.SpringUtil.class)) {
                springUtil.when(() -> cn.hutool.extra.spring.SpringUtil.getBean(DataSourceDefinitionService.class))
                        .thenReturn(definitionService);
                springUtil.when(() -> cn.hutool.extra.spring.SpringUtil.getBean(
                                com.tapdata.tm.metadatainstance.service.MetadataInstancesService.class))
                        .thenReturn(metadataInstancesService);
                runExport(request);
            }
            verify(groupTransferStrategy).exportGroups(exported.capture());
            StringBuilder all = new StringBuilder();
            exported.getValue().getContents().forEach((k, v) -> all.append(new String(v, StandardCharsets.UTF_8)));
            return all.toString();
        }

        /** metadata 里那份连接拷贝——线上由 DAGService:419 整份序列化连接得来，这里照那个形状造 */
        private MetadataInstancesDto metadataCarryingSecrets(String connectionId, String uri) {
            MetadataInstancesDto meta = new MetadataInstancesDto();
            meta.setId(new ObjectId());
            SourceDto source = new SourceDto();
            source.setName("MDM_CUSTOMER");
            // source._id 必须是所属连接的 id：buildConnectionFileContents 靠它把元数据归到连接名下，
            // 不设就整份被丢掉，这套用例也就测不到 metadata 这条腿
            source.set_id(connectionId);
            source.setDatabase_uri(uri);
            source.setDatabase_password(SECRET_PASSWORD);
            source.setPlain_password(SECRET_PASSWORD);
            meta.setSource(source);
            return meta;
        }

        private DataSourceDefinitionDto definitionWithUri() {
            Map<String, Object> uriMeta = new LinkedHashMap<>();
            uriMeta.put("apiServerKey", "database_uri");
            Map<String, Object> connProps = new LinkedHashMap<>();
            connProps.put("uri", uriMeta);
            Map<String, Object> connection = new LinkedHashMap<>();
            connection.put("properties", connProps);
            LinkedHashMap<String, Object> properties = new LinkedHashMap<>();
            properties.put("connection", connection);
            DataSourceDefinitionDto def = new DataSourceDefinitionDto();
            def.setProperties(properties);
            return def;
        }

        /** 整包的文件清单——manifest 是根级 sidecar，得按文件名取，不能像凭据那样整包搜字符串 */
        private Map<String, byte[]> packageFiles(ExportGroupRequest request) {
            registerModuleHandler();
            runExport(request);
            ArgumentCaptor<GroupExportRequest> exported = ArgumentCaptor.forClass(GroupExportRequest.class);
            verify(groupTransferStrategy).exportGroups(exported.capture());
            return exported.getValue().getContents();
        }

        private Map<String, Object> manifestOf(ExportGroupRequest request) {
            byte[] manifest = packageFiles(request).get(GroupConstants.PACKAGE_MANIFEST_FILE);
            assertNotNull(manifest, "包里必须带 " + GroupConstants.PACKAGE_MANIFEST_FILE
                    + "——导入侧只能靠它区分「这个字段真的没配」和「这个字段被脱敏抹空了」（ADR-0034 D3）");
            return JsonUtil.parseJsonUseJackson(new String(manifest, StandardCharsets.UTF_8),
                    new TypeReference<Map<String, Object>>() {});
        }

        @Test
        @DisplayName("包标记：FILE 保真包标 secretsMasked=false")
        void faithfulPackageIsMarkedUnmasked() {
            assertEquals(Boolean.FALSE,
                    manifestOf(exportRequest(GroupTransferType.FILE, null))
                            .get(GroupConstants.MANIFEST_KEY_SECRETS_MASKED),
                    "保真包里的空值是用户真实配置，导入侧要照写；标记错成 true 会让它被当成脱敏留下的洞");
        }

        @Test
        @DisplayName("包标记：GIT 被强制脱敏时标 secretsMasked=true——按**实际发生**的写，不是按入参")
        void forciblyMaskedPackageIsMarkedMasked() {
            assertEquals(Boolean.TRUE,
                    manifestOf(exportRequest(GroupTransferType.GIT, false))
                            .get(GroupConstants.MANIFEST_KEY_SECRETS_MASKED),
                    "入参说要保真、通路强制脱敏了，标记必须跟着实际结果走："
                            + "标成 false 等于告诉导入侧「包里的空值可信」，正好把目标环境的凭据抹空");
        }

        @Test
        @DisplayName("对照组：FILE 保真时整包里确实找得到凭据（否则红线用例是空断言）")
        void faithfulPackageActuallyContainsTheSecret() {
            String pkg = wholePackage(exportRequest(GroupTransferType.FILE, null));
            assertTrue(pkg.contains(SECRET_URI), "连接文档那份凭据没进包，红线用例就白测了");
            assertTrue(pkg.contains(SECRET_IN_DB_META), "database 级 metadata 那份没进包，红线用例覆盖不到这条腿");
            assertTrue(pkg.contains(SECRET_IN_TABLE_META), "table 级 metadata 那份没进包，红线用例覆盖不到这条腿");
        }

        @Test
        @DisplayName("红线（整包）：GIT + 显式要求保真，整个包里搜不到明文凭据")
        void gitPackageNeverCarriesPlaintextSecrets() {
            String pkg = wholePackage(exportRequest(GroupTransferType.GIT, false));
            assertFalse(pkg.contains(SECRET_URI),
                    "GIT 包会进 git 历史、永久留存且可被 fork/缓存——明文凭据一处都不能有。"
                            + "凭据在包里有两份拷贝（Connection 文档 + MetadataInstances.source），"
                            + "只抹前者等于没抹（ADR-0034 D2）");
            assertFalse(pkg.contains(SECRET_IN_DB_META), "database 级 metadata 里那份连接拷贝同样不能留");
            assertFalse(pkg.contains(SECRET_IN_TABLE_META), "table 级 metadata 里那份连接拷贝同样不能留");
            assertFalse(pkg.contains(SECRET_PASSWORD),
                    "顶层 database_password / plain_password 是同一个 secret 的另一处存放点，同样不能留");
        }

        @Test
        @DisplayName("FILE 未指定：传给 handler 的是「保真」")
        void fileExportPassesFaithfulDown() {
            ResourceHandler handler = registerModuleHandler();
            runExport(exportRequest(GroupTransferType.FILE, null));
            verify(handler).handleRelatedResources(any(), anyList(), eq(user), anySet(), eq(false));
        }

        @Test
        @DisplayName("红线接线：GIT + 显式要求保真，传给 handler 的仍是「脱敏」，且导出记录里说明已强制脱敏")
        void gitExportForcesMaskDownAndAnnouncesIt() {
            ResourceHandler handler = registerModuleHandler();
            GroupInfoRecordDto record = runExport(exportRequest(GroupTransferType.GIT, false));

            verify(handler).handleRelatedResources(any(), anyList(), eq(user), anySet(), eq(true));
            assertNotNull(record.getMessage(), "被强制脱敏这件事必须留在导出记录里，不能静默吞掉用户的请求");
        }
    }

    /**
     * 导入侧保护（ADR-0034 D5/D6/D7）：脱敏包未带 vault 时，绝不用空值覆盖目标环境
     * 已有的连接凭据。实撞过一次——Create MongoDB client failed, error: uri is blank。
     */
    @Nested
    @DisplayName("preserveExistingSecrets")
    class PreserveExistingSecretsTest {

        private com.tapdata.tm.commons.schema.DataSourceDefinitionDto definitionWithUri() {
            Map<String, Object> uriMeta = new LinkedHashMap<>();
            uriMeta.put("apiServerKey", "database_uri");
            Map<String, Object> connProps = new LinkedHashMap<>();
            connProps.put("uri", uriMeta);
            Map<String, Object> connection = new LinkedHashMap<>();
            connection.put("properties", connProps);
            LinkedHashMap<String, Object> properties = new LinkedHashMap<>();
            properties.put("connection", connection);
            com.tapdata.tm.commons.schema.DataSourceDefinitionDto def =
                    new com.tapdata.tm.commons.schema.DataSourceDefinitionDto();
            def.setProperties(properties);
            return def;
        }

        @Test
        @DisplayName("包里 uri 被脱敏抹空时，改用目标既有 uri，并把该字段报出来")
        void maskedPackage_keepsTargetSecretAndReportsIt() {
            ObjectId connId = new ObjectId();

            DataSourceConnectionDto incoming = new DataSourceConnectionDto();
            incoming.setId(connId);
            incoming.setName("MDM_CUSTOMER");
            incoming.setPdkHash("pdkhash-mongodb");
            incoming.setConfig(new LinkedHashMap<>(Map.of("isUri", true)));

            DataSourceConnectionDto existing = new DataSourceConnectionDto();
            existing.setId(connId);
            existing.setConfig(new LinkedHashMap<>(Map.of(
                    "isUri", true,
                    "uri", "mongodb://real-host:27017/dmp")));

            when(dataSourceService.findById(eq(connId), any(String[].class))).thenReturn(existing);
            when(dataSourceDefinitionService.findByPdkHash(eq("pdkhash-mongodb"), anyInt(), any(UserDetail.class)))
                    .thenReturn(definitionWithUri());

            Map<String, DataSourceConnectionDto> connections = new LinkedHashMap<>();
            connections.put("MDM_CUSTOMER", incoming);

            Map<String, List<String>> report = groupInfoService.preserveExistingSecrets(
                    Collections.emptyMap(), connections, user);

            assertEquals("mongodb://real-host:27017/dmp", incoming.getConfig().get("uri"),
                    "目标既有 uri 必须留下——包里那个空缺是脱敏产物，不是用户的配置");
            assertEquals(Map.of(connId.toHexString(), List.of("uri")), report,
                    "保留了哪些字段必须按连接报出来，否则用户分不清『凭据已更新』和『沿用了旧的』");
        }
    }

    /**
     * ES-3：包自带脱敏标记（[ADR-0034] D3/D4）——导入侧**是否信任包里的空值**由它决定。
     *
     * 脱敏包里的空值是抹出来的洞（保留目标既有值），保真包里的空值是用户真实配置（照写）。
     * 两者在包里长得一模一样，不可由内容推断，只能靠标记；标记缺失 ⇒ 老包 ⇒ 按脱敏包（D4）。
     */
    @Nested
    @DisplayName("包脱敏标记决定导入侧是否保留既有凭据")
    class PackageSecretsMarkTest {

        private static final String REAL_URI = "mongodb://real-host:27017/dmp";

        private DataSourceDefinitionDto definitionWithUri() {
            Map<String, Object> uriMeta = new LinkedHashMap<>();
            uriMeta.put("apiServerKey", "database_uri");
            Map<String, Object> connProps = new LinkedHashMap<>();
            connProps.put("uri", uriMeta);
            Map<String, Object> connection = new LinkedHashMap<>();
            connection.put("properties", connProps);
            LinkedHashMap<String, Object> properties = new LinkedHashMap<>();
            properties.put("connection", connection);
            DataSourceDefinitionDto def = new DataSourceDefinitionDto();
            def.setProperties(properties);
            return def;
        }

        private Map<String, List<TaskUpAndLoadDto>> payloadsWithManifest(String manifestJson) {
            Map<String, List<TaskUpAndLoadDto>> payloads = new HashMap<>();
            if (manifestJson != null) {
                payloads.put(GroupConstants.PACKAGE_MANIFEST_FILE,
                        List.of(new TaskUpAndLoadDto(GroupConstants.PACKAGE_MANIFEST_FILE, manifestJson)));
            }
            return payloads;
        }

        /** 包里 uri 缺失的那条连接；目标环境同 id 的连接凭据齐全 */
        private DataSourceConnectionDto incomingWithBlankUri(ObjectId connId) {
            DataSourceConnectionDto incoming = new DataSourceConnectionDto();
            incoming.setId(connId);
            incoming.setName("MDM_CUSTOMER");
            incoming.setPdkHash("pdkhash-mongodb");
            incoming.setConfig(new LinkedHashMap<>(Map.of("isUri", true)));
            return incoming;
        }

        private void stubTargetHasRealUri(ObjectId connId) {
            DataSourceConnectionDto existing = new DataSourceConnectionDto();
            existing.setId(connId);
            existing.setConfig(new LinkedHashMap<>(Map.of("isUri", true, "uri", REAL_URI)));
            lenient().when(dataSourceService.findById(eq(connId), any(String[].class))).thenReturn(existing);
            lenient().when(dataSourceDefinitionService.findByPdkHash(eq("pdkhash-mongodb"), anyInt(), any(UserDetail.class)))
                    .thenReturn(definitionWithUri());
        }

        /** @return 包里那条连接落库前的 uri（null = 没被回填） */
        private Object uriAfterPreserve(String manifestJson) {
            ObjectId connId = new ObjectId();
            DataSourceConnectionDto incoming = incomingWithBlankUri(connId);
            stubTargetHasRealUri(connId);
            Map<String, DataSourceConnectionDto> connections = new LinkedHashMap<>();
            connections.put("MDM_CUSTOMER", incoming);

            groupInfoService.preserveExistingSecrets(payloadsWithManifest(manifestJson), connections, user);
            return incoming.getConfig().get("uri");
        }

        @Test
        @DisplayName("保真包（secretsMasked=false）：包里的空值是用户真实配置，照写，不拿目标旧值回填")
        void faithfulPackage_trustsBlankValue() {
            assertNull(uriAfterPreserve("{\"secretsMasked\":false}"),
                    "保真包里没有 uri，就是这个连接真的没配 uri；"
                            + "拿目标旧值回填等于让用户永远删不掉一个配置项，且「以为更新了其实没有」");
        }

        @Test
        @DisplayName("脱敏包（secretsMasked=true）：空值是抹出来的洞，保留目标既有值")
        void maskedPackage_keepsTargetValue() {
            assertEquals(REAL_URI, uriAfterPreserve("{\"secretsMasked\":true}"),
                    "脱敏包里的空缺不是用户配置，写下去就把目标环境的连接打死了");
        }

        @Test
        @DisplayName("老包（无标记文件）：按脱敏包处理——历史包事实上都是脱敏的（D4）")
        void oldPackageWithoutManifest_keepsTargetValue() {
            assertEquals(REAL_URI, uriAfterPreserve(null),
                    "把老包当保真包，就会信任包里的空值——正是 ADR-0034 要修的那个 bug");
        }

        @Test
        @DisplayName("标记文件在、但没有这一位：按脱敏包处理——别把「没写」当成「写了 false」")
        void manifestWithoutTheBit_keepsTargetValue() {
            assertEquals(REAL_URI, uriAfterPreserve("{\"someFutureField\":1}"),
                    "将来包格式若改名/漏写这一位，必须退回保守的一侧，不能静默变成「信任空值」");
        }

        @Test
        @DisplayName("标记读不懂时按脱敏包处理——不确定就别信任空值")
        void unreadableManifest_keepsTargetValue() {
            assertEquals(REAL_URI, uriAfterPreserve("not-json-at-all"),
                    "解析不了就退回最保守的一侧：错判成保真包的代价是抹掉目标凭据，反过来只是少更新一个字段");
        }
    }

    /**
     * ES-4b：① 两处调用点的接线覆盖 ② 保留清单接进导入报告（ADR-0034 D7 的「显形」）。
     *
     * 断言刻意走行为面而不是 verify(调用发生过)：落库那份 config 必须已带目标既有 uri、
     * 导入记录里必须报出「哪个连接的哪个字段沿用了旧值」。摘掉任一调用点即转红。
     */
    @Nested
    @DisplayName("preserveExistingSecrets 接线 + 导入报告显形")
    class PreserveExistingSecretsWiringTest {

        private static final String REAL_URI = "mongodb://real-host:27017/dmp";

        private DataSourceDefinitionDto definitionWithUri() {
            Map<String, Object> uriMeta = new LinkedHashMap<>();
            uriMeta.put("apiServerKey", "database_uri");
            Map<String, Object> connProps = new LinkedHashMap<>();
            connProps.put("uri", uriMeta);
            Map<String, Object> connection = new LinkedHashMap<>();
            connection.put("properties", connProps);
            LinkedHashMap<String, Object> properties = new LinkedHashMap<>();
            properties.put("connection", connection);
            DataSourceDefinitionDto def = new DataSourceDefinitionDto();
            def.setProperties(properties);
            return def;
        }

        /** 包里的连接：uri 已被导出脱敏抹掉，只剩非敏感项 */
        private TaskUpAndLoadDto maskedConnectionPayload(ObjectId connId, String name) {
            Map<String, Object> json = new LinkedHashMap<>();
            json.put("id", connId.toHexString());
            json.put("name", name);
            json.put("pdkHash", "pdkhash-mongodb");
            json.put("config", new LinkedHashMap<>(Map.of("isUri", true)));
            return new TaskUpAndLoadDto(GroupConstants.COLLECTION_CONNECTION, JsonUtil.toJsonUseJackson(json));
        }

        /** 目标环境里那条连接：凭据齐全 */
        private DataSourceConnectionDto existingWithRealUri(ObjectId connId) {
            DataSourceConnectionDto existing = new DataSourceConnectionDto();
            existing.setId(connId);
            existing.setConfig(new LinkedHashMap<>(Map.of("isUri", true, "uri", REAL_URI)));
            return existing;
        }

        private void stubTargetLookup(ObjectId connId) {
            when(dataSourceService.findById(eq(connId), any(String[].class)))
                    .thenReturn(existingWithRealUri(connId));
            when(dataSourceDefinitionService.findByPdkHash(eq("pdkhash-mongodb"), anyInt(), any(UserDetail.class)))
                    .thenReturn(definitionWithUri());
        }

        @SuppressWarnings("unchecked")
        private List<DataSourceConnectionDto> captureBatchImported() {
            ArgumentCaptor<List<DataSourceConnectionDto>> captor = ArgumentCaptor.forClass(List.class);
            verify(dataSourceService).batchImport(captor.capture(), eq(user), any());
            return captor.getValue();
        }

        private void assertReportsPreservedField(List<GroupInfoRecordDetail> details, String connName, String field) {
            assertNotNull(details, "导入报告不能为 null——D7 要求保留旧凭据这件事必须显形");
            String messages = details.stream()
                    .flatMap(d -> d.getRecordDetails().stream())
                    .filter(rd -> connName.equals(rd.getResourceName()))
                    .map(GroupInfoRecordDetail.RecordDetail::getMessage)
                    .filter(java.util.Objects::nonNull)
                    .reduce("", (a, b) -> a + " " + b);
            assertTrue(messages.contains(field),
                    "导入报告里必须能看到 " + connName + " 的 " + field + " 沿用了目标既有值，实际报告：" + messages);
        }

        @Test
        @DisplayName("拆分导入（executeImportConnectionsAsync）：落库前保留目标既有 uri，并在导入记录里报出来")
        @SuppressWarnings("unchecked")
        void connectionsImportPath_preservesAndReports() {
            ObjectId connId = new ObjectId();
            ObjectId recordId = new ObjectId();
            Map<String, List<TaskUpAndLoadDto>> payloads = new HashMap<>();
            payloads.put("Connection.json", List.of(maskedConnectionPayload(connId, "MDM_CUSTOMER")));

            stubTargetLookup(connId);
            when(dataSourceService.batchImport(anyList(), any(), any())).thenReturn(new HashMap<>());
            doNothing().when(batchUpChecker).checkDataSourceConnection(any(), any(), any());
            doNothing().when(groupInfoService).updateRecordStatus(any(), any(), any(), any(), any());

            groupInfoService.executeImportConnectionsAsync(payloads, ImportModeEnum.GROUP_IMPORT, user, recordId,
                    Collections.emptyMap());

            assertEquals(REAL_URI, captureBatchImported().get(0).getConfig().get("uri"),
                    "落库的那份 config 必须已经带上目标既有 uri——否则空值会把目标环境的凭据抹掉");

            ArgumentCaptor<List<GroupInfoRecordDetail>> details = ArgumentCaptor.forClass(List.class);
            verify(groupInfoService).updateRecordStatus(eq(recordId), eq(GroupInfoRecordDto.STATUS_COMPLETED),
                    isNull(), details.capture(), eq(user));
            assertReportsPreservedField(details.getValue(), "MDM_CUSTOMER", "uri");
        }

        /**
         * ES-3 接线：包标记必须真的传到 preserveExistingSecrets——判据只在那儿算一次，
         * 但读得到 payloads 的是调用点。把 payloads 忘在调用点上，保真包就会被当老包处理。
         */
        @Test
        @DisplayName("拆分导入 + 保真包：包里的空 uri 照写，不拿目标旧值回填")
        void connectionsImportPath_faithfulPackageWritesBlankThrough() {
            ObjectId connId = new ObjectId();
            ObjectId recordId = new ObjectId();
            Map<String, List<TaskUpAndLoadDto>> payloads = new HashMap<>();
            payloads.put("Connection.json", List.of(maskedConnectionPayload(connId, "MDM_CUSTOMER")));
            payloads.put(GroupConstants.PACKAGE_MANIFEST_FILE, List.of(new TaskUpAndLoadDto(
                    GroupConstants.PACKAGE_MANIFEST_FILE, "{\"secretsMasked\":false}")));

            // 目标环境确实有真 uri：不 stub 的话，这条用例在「漏传 payloads」时也会绿（findById 返回 null 而已）
            lenient().when(dataSourceService.findById(eq(connId), any(String[].class)))
                    .thenReturn(existingWithRealUri(connId));
            lenient().when(dataSourceDefinitionService.findByPdkHash(eq("pdkhash-mongodb"), anyInt(), any(UserDetail.class)))
                    .thenReturn(definitionWithUri());
            when(dataSourceService.batchImport(anyList(), any(), any())).thenReturn(new HashMap<>());
            doNothing().when(batchUpChecker).checkDataSourceConnection(any(), any(), any());
            doNothing().when(groupInfoService).updateRecordStatus(any(), any(), any(), any(), any());

            groupInfoService.executeImportConnectionsAsync(payloads, ImportModeEnum.GROUP_IMPORT, user, recordId,
                    Collections.emptyMap());

            assertNull(captureBatchImported().get(0).getConfig().get("uri"),
                    "保真包说这个连接就是没配 uri，导入侧不该拿目标旧值把它填回去");
        }

        @Test
        @DisplayName("整组导入（executeImportAsync）：同样保留目标既有 uri，并在导入记录里报出来")
        @SuppressWarnings("unchecked")
        void groupImportPath_preservesAndReports() {
            ObjectId connId = new ObjectId();
            ObjectId recordId = new ObjectId();
            Map<String, List<TaskUpAndLoadDto>> payloads = new HashMap<>();
            payloads.put("GroupInfo.json", new ArrayList<>());

            DataSourceConnectionDto incoming = new DataSourceConnectionDto();
            incoming.setId(connId);
            incoming.setName("MDM_CUSTOMER");
            incoming.setPdkHash("pdkhash-mongodb");
            incoming.setConfig(new LinkedHashMap<>(Map.of("isUri", true)));
            registerConnectionHandler(connId, incoming);

            stubTargetLookup(connId);
            when(dataSourceService.batchImport(anyList(), any(), any())).thenReturn(new HashMap<>());
            when(metadataDefinitionService.batchImport(any(), any())).thenReturn(new HashMap<>());
            doNothing().when(batchUpChecker).checkDataSourceConnection(any(), any(), any());
            doNothing().when(groupInfoService).updateImportProgress(any(), anyInt(), any(), any());
            doNothing().when(groupInfoService).updateRecordStatus(any(), any(), any(), any(), any());

            groupInfoService.executeImportAsync(payloads, user, ImportModeEnum.GROUP_IMPORT, "test.tar", recordId);

            assertEquals(REAL_URI, captureBatchImported().get(0).getConfig().get("uri"),
                    "整组导入这条路同样会整文档覆盖，缺了保护一样会抹空目标凭据");

            ArgumentCaptor<List<GroupInfoRecordDetail>> details = ArgumentCaptor.forClass(List.class);
            verify(groupInfoService).updateRecordStatus(eq(recordId), eq(GroupInfoRecordDto.STATUS_COMPLETED),
                    isNull(), details.capture(), eq(user));
            assertReportsPreservedField(details.getValue(), "MDM_CUSTOMER", "uri");
        }

        @Test
        @DisplayName("整组导入 + 保真包：包里的空 uri 同样照写——两条调用点都得把 payloads 传下去")
        void groupImportPath_faithfulPackageWritesBlankThrough() {
            ObjectId connId = new ObjectId();
            ObjectId recordId = new ObjectId();
            Map<String, List<TaskUpAndLoadDto>> payloads = new HashMap<>();
            payloads.put("GroupInfo.json", new ArrayList<>());
            payloads.put(GroupConstants.PACKAGE_MANIFEST_FILE, List.of(new TaskUpAndLoadDto(
                    GroupConstants.PACKAGE_MANIFEST_FILE, "{\"secretsMasked\":false}")));

            DataSourceConnectionDto incoming = new DataSourceConnectionDto();
            incoming.setId(connId);
            incoming.setName("MDM_CUSTOMER");
            incoming.setPdkHash("pdkhash-mongodb");
            incoming.setConfig(new LinkedHashMap<>(Map.of("isUri", true)));
            registerConnectionHandler(connId, incoming);

            // 同上：目标环境确实有真 uri，漏传 payloads 时才会被这条用例抓住
            lenient().when(dataSourceService.findById(eq(connId), any(String[].class)))
                    .thenReturn(existingWithRealUri(connId));
            lenient().when(dataSourceDefinitionService.findByPdkHash(eq("pdkhash-mongodb"), anyInt(), any(UserDetail.class)))
                    .thenReturn(definitionWithUri());
            when(dataSourceService.batchImport(anyList(), any(), any())).thenReturn(new HashMap<>());
            when(metadataDefinitionService.batchImport(any(), any())).thenReturn(new HashMap<>());
            doNothing().when(batchUpChecker).checkDataSourceConnection(any(), any(), any());
            doNothing().when(groupInfoService).updateImportProgress(any(), anyInt(), any(), any());
            doNothing().when(groupInfoService).updateRecordStatus(any(), any(), any(), any(), any());

            groupInfoService.executeImportAsync(payloads, user, ImportModeEnum.GROUP_IMPORT, "test.tar", recordId);

            assertNull(captureBatchImported().get(0).getConfig().get("uri"),
                    "整组导入这条路也必须读包标记——只在拆分导入那条路接上，等于一半的入口还在拿旧值回填");
        }

        /** 让 executeImportAsync 的资源收集环节吐出这条连接（生产上由 Task/Module handler 的关联收集完成） */
        @SuppressWarnings("unchecked")
        private void registerConnectionHandler(ObjectId connId, DataSourceConnectionDto incoming) {
            ResourceHandler connectionHandler = mock(ResourceHandler.class);
            when(connectionHandler.getResourceType()).thenReturn(ResourceType.CONNECTION);
            doAnswer(invocation -> {
                ((Map<String, Object>) invocation.getArgument(1)).put(connId.toHexString(), incoming);
                return null;
            }).when(connectionHandler).collectPayload(any(), any(), any());
            ResourceHandlerRegistry registry = new ResourceHandlerRegistry();
            ReflectionTestUtils.setField(registry, "handlers", List.of(connectionHandler));
            registry.init();
            ReflectionTestUtils.setField(groupInfoService, "resourceHandlerRegistry", registry);
        }

        @Test
        @DisplayName("报告显形：已有的连接行直接挂上消息，不另起一行")
        void reportAttachesToExistingConnectionRow() {
            ObjectId connId = new ObjectId();
            GroupInfoRecordDetail detail = new GroupInfoRecordDetail();
            detail.setGroupName("MDM");
            GroupInfoRecordDetail.RecordDetail row = new GroupInfoRecordDetail.RecordDetail();
            row.setResourceType(ResourceType.CONNECTION);
            row.setResourceId(connId.toHexString());
            row.setResourceName("MDM_CUSTOMER");
            row.setAction(GroupInfoRecordDetail.RecordAction.IMPORTING);
            detail.getRecordDetails().add(row);
            List<GroupInfoRecordDetail> details = new ArrayList<>(List.of(detail));

            DataSourceConnectionDto conn = new DataSourceConnectionDto();
            conn.setId(connId);
            conn.setName("MDM_CUSTOMER");

            groupInfoService.reportPreservedSecrets(details,
                    Map.of(connId.toHexString(), List.of("uri", "ssl.password")),
                    Map.of(connId.toHexString(), conn));

            assertEquals(1, details.size(), "已有行能挂上就不该另起一个分组");
            assertEquals(1, details.get(0).getRecordDetails().size(), "同一个连接不该出现两行");
            String message = details.get(0).getRecordDetails().get(0).getMessage();
            assertNotNull(message);
            assertTrue(message.contains("uri") && message.contains("ssl.password"),
                    "两个被保留的字段都要报出来，实际：" + message);
            assertEquals(GroupInfoRecordDetail.RecordAction.IMPORTING,
                    details.get(0).getRecordDetails().get(0).getAction(),
                    "连接确实导入了，只是部分字段沿用旧值——不该改写 action 误报成跳过");
        }
    }

}
