package com.tapdata.tm.group.service;

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
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
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
            Map<String, Object> json = new LinkedHashMap<>();
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
            Map<String, Object> json = new LinkedHashMap<>();
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
            Map<String, Object> json = new LinkedHashMap<>();
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
    @DisplayName("buildApiDiff tests")
    class BuildApiDiffTest {

        private ResourceDiff invoke(Map<String, List<TaskUpAndLoadDto>> payloads) {
            return ReflectionTestUtils.invokeMethod(groupInfoService, "buildApiDiff", payloads, user);
        }

        private TaskUpAndLoadDto modulePayload(String name, String apiType, String description) {
            Map<String, Object> json = new LinkedHashMap<>();
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

}
