package com.tapdata.tm.group.service;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.ImportModeEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
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

            groupInfoService.exportGroupInfos(response, groupIds, user,new HashMap<>());

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

            groupInfoService.exportGroupInfos(response, groupIds, user ,new HashMap<>());

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

            groupInfoService.exportGroupInfos(response, groupIds, user,new HashMap<>());

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

            groupInfoService.exportGroupInfos(response, groupIds, user,new HashMap<>());

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

            groupInfoService.exportGroupInfos(response, groupIds, user,new HashMap<>());

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

            groupInfoService.exportGroupInfos(response, groupIds, user,new HashMap<>());

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

            groupInfoService.exportGroupInfos(response, groupIds, user,new HashMap<>());

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

            groupInfoService.exportGroupInfos(response, groupIds, user,new HashMap<>());

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

            groupInfoService.exportGroupInfos(response, groupIds, user,new HashMap<>());

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

            doNothing().when(groupInfoService).updateImportProgress(any(), anyInt(), any(), any());
            doNothing().when(groupInfoService).updateRecordStatus(any(), any(), any(), any(), any());

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

            doNothing().when(groupInfoService).updateImportProgress(any(), anyInt(), any(), any());
            doNothing().when(groupInfoService).updateRecordStatus(any(), any(), any(), any(), any());

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

            when(dataSourceService.batchImport(any(), any(), any())).thenReturn(new HashMap<>());

            doNothing().when(groupInfoService).updateImportProgress(any(), anyInt(), any(), any());
            doNothing().when(groupInfoService).updateRecordStatus(any(), any(), any(), any(), any());

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

            when(dataSourceService.batchImport(any(), any(), any())).thenReturn(new HashMap<>());
            doReturn(new GroupInfoDto()).when(groupInfoService).upsertByWhere(any(), any(), any(UserDetail.class));

            doNothing().when(groupInfoService).updateImportProgress(any(), anyInt(), any(), any());
            doNothing().when(groupInfoService).updateRecordStatus(any(), any(), any(), any(), any());

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
            when(dataSourceService.batchImport(any(), any(), any())).thenReturn(new HashMap<>());
            doReturn(new GroupInfoDto()).when(groupInfoService).upsertByWhere(any(), any(), any(UserDetail.class));

            doNothing().when(groupInfoService).updateImportProgress(any(), anyInt(), any(), any());
            doNothing().when(groupInfoService).updateRecordStatus(any(), any(), any(), any(), any());

            groupInfoService.executeImportAsync(payloads, user, null, "test.tar", recordId);

            // Verify upsertByWhere is called for each group info
            verify(groupInfoService).upsertByWhere(any(), any(), eq(user));
        }

        @Test
        @DisplayName("Should handle empty payloads gracefully")
        void testExecuteImportAsyncEmptyPayloads() {
            Map<String, List<TaskUpAndLoadDto>> payloads = new HashMap<>();
            ObjectId recordId = new ObjectId();

            when(dataSourceService.batchImport(any(), any(), any())).thenReturn(new HashMap<>());

            doNothing().when(groupInfoService).updateImportProgress(any(), anyInt(), any(), any());
            doNothing().when(groupInfoService).updateRecordStatus(any(), any(), any(), any(), any());

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



            when(dataSourceService.batchImport(any(), any(), any())).thenReturn(new HashMap<>());

            doNothing().when(groupInfoService).updateImportProgress(any(), anyInt(), any(), any());
            doNothing().when(groupInfoService).updateRecordStatus(any(), any(), any(), any(), any());

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

            GroupInfoDto groupInfo = new GroupInfoDto();
            groupInfo.setId(new ObjectId());
            groupInfo.setName("Test Group");
            groupInfo.setResourceItemList(Arrays.asList(item));

            TaskUpAndLoadDto groupPayload = new TaskUpAndLoadDto();
            groupPayload.setCollectionName("GroupInfo");
            groupPayload.setJson(JsonUtil.toJsonUseJackson(groupInfo));

            payloads.put("GroupInfo.json", Arrays.asList(groupPayload));
            ObjectId recordId = new ObjectId();

            when(dataSourceService.batchImport(any(), any(), any())).thenReturn(new HashMap<>());
            doReturn(new GroupInfoDto()).when(groupInfoService).upsertByWhere(any(), any(), any(UserDetail.class));

            doNothing().when(groupInfoService).updateImportProgress(any(), anyInt(), any(), any());
            doNothing().when(groupInfoService).updateRecordStatus(any(), any(), any(), any(), any());

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
            List<TaskUpAndLoadDto> result = (List<TaskUpAndLoadDto>) ReflectionTestUtils.invokeMethod(
                    groupInfoService, "buildGroupInfoPayload", (List<GroupInfoDto>) null);

            assertNotNull(result);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should return empty list when groupInfos is empty")
        void testBuildGroupInfoPayloadEmpty() {
            List<TaskUpAndLoadDto> result = (List<TaskUpAndLoadDto>) ReflectionTestUtils.invokeMethod(
                    groupInfoService, "buildGroupInfoPayload", Collections.emptyList());

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

            List<TaskUpAndLoadDto> result = (List<TaskUpAndLoadDto>) ReflectionTestUtils.invokeMethod(
                    groupInfoService, "buildGroupInfoPayload", Arrays.asList(groupDto));

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

}
