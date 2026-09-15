package com.tapdata.tm.ds.service.impl;

import cn.hutool.core.date.DateUtil;
import com.mongodb.client.gridfs.GridFSFindIterable;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dblock.DBLockConfiguration;
import com.tapdata.tm.dblock.DBLockRepository;
import com.tapdata.tm.dblock.LockStateEnums;
import com.tapdata.tm.ds.dto.PdkSourceDto;
import com.tapdata.tm.ds.dto.PdkVersionCheckDto;
import com.tapdata.tm.ds.vo.PdkFileTypeEnum;
import com.tapdata.tm.file.service.FileService;
import com.tapdata.tm.inspect.constant.InspectStatusEnum;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.service.InspectService;
import com.tapdata.tm.tcm.service.TcmService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.MessageUtil;
import lombok.SneakyThrows;
import org.apache.commons.fileupload2.core.FileItem;
import org.apache.commons.io.FileUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class PkdSourceServiceTest {
    private PkdSourceService pkdSourceService;
    private FileService fileService;
    private DataSourceDefinitionService dataSourceDefinitionService;
    private DataSourceService dataSourceService;
    private TaskService taskService;
    private InspectService inspectService;
    private DBLockRepository dbLockRepository;
    private DBLockConfiguration dbLockConfiguration;

    @BeforeEach
    void buildMember(){
        pkdSourceService = new PkdSourceService();
        fileService = mock(FileService.class);
        dataSourceDefinitionService = mock(DataSourceDefinitionService.class);
        dataSourceService = mock(DataSourceService.class);
        taskService = mock(TaskService.class);
        inspectService = mock(InspectService.class);
        dbLockRepository = mock(DBLockRepository.class);
        dbLockConfiguration = mock(DBLockConfiguration.class);
        ReflectionTestUtils.setField(pkdSourceService,"fileService",fileService);
        ReflectionTestUtils.setField(pkdSourceService,"dataSourceDefinitionService",dataSourceDefinitionService);
        ReflectionTestUtils.setField(pkdSourceService,"dataSourceService",dataSourceService);
        ReflectionTestUtils.setField(pkdSourceService,"taskService",taskService);
        ReflectionTestUtils.setField(pkdSourceService,"inspectService",inspectService);
        ReflectionTestUtils.setField(pkdSourceService,"dbLockRepository",dbLockRepository);
        ReflectionTestUtils.setField(pkdSourceService,"dbLockConfiguration",dbLockConfiguration);
        when(dbLockConfiguration.getOwner()).thenReturn("test-owner");
        when(dbLockRepository.init(anyString())).thenReturn(true);
        when(dbLockRepository.renew(anyString(), anyString(), any(Date.class))).thenReturn(LockStateEnums.YES);
        when(dbLockRepository.release(anyString(), anyString())).thenReturn(true);
    }

	@AfterEach
	void cleanTemporaryJar() {
		FileUtils.deleteQuietly(new File("mysql.jar"));
	}
    @Nested
    class uploadPdkTest{
        @Test
        @SneakyThrows
        void testUploadPdk(){
            MultipartFile[] files = new MultipartFile[1];
            MultipartFile file = mock(MultipartFile.class);
            files[0] = file;
            List<PdkSourceDto > pdkSourceDtos = new ArrayList<>();
            PdkSourceDto pdkSourceDto = mock(PdkSourceDto.class);
            pdkSourceDtos.add(pdkSourceDto);
            boolean latest = false;
            UserDetail user = mock(UserDetail.class);
            FileItem item = mock(FileItem.class);
            when(pdkSourceDto.getVersion()).thenReturn("1.0-SNAPSHOT");
            when(file.getOriginalFilename()).thenReturn("a.jar");
            when(file.getName()).thenReturn("a.jar");
            InputStream ins = mock(InputStream.class);
            when(file.getInputStream()).thenReturn(ins);
            when(ins.read(any())).thenReturn(-1);
            when(fileService.storeFile(any(),anyString(),any(),anyMap())).thenReturn(mock(ObjectId.class));
            pkdSourceService.uploadPdk(files,pdkSourceDtos,latest,user);
            verify(fileService).storeFile(any(),anyString(),any(),anyMap());
            FileUtils.deleteQuietly(new File("a.jar"));

            GridFSFindIterable result = mock(GridFSFindIterable.class);

            DataSourceDefinitionDto dto = new DataSourceDefinitionDto();
            dto.setId(new ObjectId());
            dto.setJarRid(new ObjectId().toHexString());
            dto.setIcon(new ObjectId().toHexString());
            when(dataSourceDefinitionService.findOne(any())).thenReturn(dto);
            when(fileService.find(any())).thenReturn(result);
            doAnswer(answer -> {

                Consumer consumer = answer.getArgument(0);
                GridFSFile gridFSFile = mock(GridFSFile.class);
                when(gridFSFile.getObjectId()).thenReturn(new ObjectId());
                consumer.accept(gridFSFile);

                return null;
            }).when(result).forEach(any());
            pkdSourceService.uploadPdk(files, pdkSourceDtos, true, user);
            verify(fileService).scheduledDeleteFiles(any(), anyString(), anyString(), any(), any());

            /*pkdSourceService.uploadPdk(files, pdkSourceDtos, true, user);
            verify(fileService).scheduledDeleteFiles(any(), anyString(), anyString(), any(), any());*/

        }

		@Test
		@SneakyThrows
		void testStopAndRestartAffectedTask() {
			ObjectId connectionId = new ObjectId();
			ObjectId taskId = new ObjectId();
			PdkSourceDto pdkSourceDto = mockPdkSourceDto();
			MultipartFile jarFile = mockJarFile();
			UserDetail user = mock(UserDetail.class);

			DataSourceConnectionDto connection = new DataSourceConnectionDto();
			connection.setId(connectionId);
			TaskDto affectedTask = new TaskDto();
			affectedTask.setId(taskId);
			affectedTask.setName("affected-task");
			affectedTask.setStatus(TaskDto.STATUS_RUNNING);
			affectedTask.setSyncType(TaskDto.SYNC_TYPE_SYNC);
			TaskDto stoppedTask = new TaskDto();
			stoppedTask.setId(taskId);
			stoppedTask.setStatus(TaskDto.STATUS_STOP);

			when(dataSourceService.findAllDto(any(Query.class), eq(user))).thenReturn(Collections.singletonList(connection));
			when(taskService.findAllDto(any(Query.class), eq(user))).thenReturn(Collections.singletonList(affectedTask));
			when(taskService.findOne(any(Query.class), eq(user))).thenReturn(stoppedTask);
			when(fileService.storeFile(any(), anyString(), isNull(), anyMap())).thenReturn(new ObjectId());

			pkdSourceService.uploadPdk(new MultipartFile[]{jarFile}, Collections.singletonList(pdkSourceDto), false, user);

			verify(taskService).pause(taskId, user, false);
			verify(taskService).start(taskId, user);
		}

		@Test
		@SneakyThrows
		void testRestartAffectedTaskWhenUploadFails() {
			ObjectId connectionId = new ObjectId();
			ObjectId taskId = new ObjectId();
			PdkSourceDto pdkSourceDto = mockPdkSourceDto();
			MultipartFile jarFile = mockJarFile();
			UserDetail user = mock(UserDetail.class);

			DataSourceConnectionDto connection = new DataSourceConnectionDto();
			connection.setId(connectionId);
			TaskDto affectedTask = new TaskDto();
			affectedTask.setId(taskId);
			affectedTask.setName("affected-task");
			affectedTask.setStatus(TaskDto.STATUS_RUNNING);
			affectedTask.setSyncType(TaskDto.SYNC_TYPE_SYNC);
			TaskDto stoppedTask = new TaskDto();
			stoppedTask.setId(taskId);
			stoppedTask.setStatus(TaskDto.STATUS_STOP);

			when(dataSourceService.findAllDto(any(Query.class), eq(user))).thenReturn(Collections.singletonList(connection));
			when(taskService.findAllDto(any(Query.class), eq(user))).thenReturn(Collections.singletonList(affectedTask));
			when(taskService.findOne(any(Query.class), eq(user))).thenReturn(stoppedTask);
			when(jarFile.getInputStream()).thenThrow(new IOException("upload failed"));

			assertThrows(BizException.class, () -> pkdSourceService.uploadPdk(
					new MultipartFile[]{jarFile}, Collections.singletonList(pdkSourceDto), false, user));

			verify(taskService).pause(taskId, user, false);
			verify(taskService).start(taskId, user);
		}

		@Test
		@SneakyThrows
		void testAllowRegistrationWhenAffectedTaskCompletesWhileStopping() {
			ObjectId connectionId = new ObjectId();
			ObjectId taskId = new ObjectId();
			PdkSourceDto pdkSourceDto = mockPdkSourceDto();
			MultipartFile jarFile = mockJarFile();
			UserDetail user = mock(UserDetail.class);

			DataSourceConnectionDto connection = new DataSourceConnectionDto();
			connection.setId(connectionId);
			TaskDto affectedTask = new TaskDto();
			affectedTask.setId(taskId);
			affectedTask.setName("completed-task");
			affectedTask.setStatus(TaskDto.STATUS_RUNNING);
			affectedTask.setSyncType(TaskDto.SYNC_TYPE_SYNC);
			TaskDto completedTask = new TaskDto();
			completedTask.setId(taskId);
			completedTask.setStatus(TaskDto.STATUS_COMPLETE);

			when(dataSourceService.findAllDto(any(Query.class), eq(user))).thenReturn(Collections.singletonList(connection));
			when(taskService.findAllDto(any(Query.class), eq(user))).thenReturn(Collections.singletonList(affectedTask));
			when(taskService.findOne(any(Query.class), eq(user))).thenReturn(completedTask);
			when(fileService.storeFile(any(), anyString(), isNull(), anyMap())).thenReturn(new ObjectId());

			pkdSourceService.uploadPdk(new MultipartFile[]{jarFile}, Collections.singletonList(pdkSourceDto), false, user);

			verify(taskService).pause(taskId, user, false);
			verify(taskService, never()).start(any(ObjectId.class), any(UserDetail.class));
		}

		@Test
		@SneakyThrows
		void testRestartAffectedTaskWhenItEntersErrorWhileStopping() {
			ObjectId connectionId = new ObjectId();
			ObjectId taskId = new ObjectId();
			PdkSourceDto pdkSourceDto = mockPdkSourceDto();
			MultipartFile jarFile = mockJarFile();
			UserDetail user = mock(UserDetail.class);

			DataSourceConnectionDto connection = new DataSourceConnectionDto();
			connection.setId(connectionId);
			TaskDto affectedTask = new TaskDto();
			affectedTask.setId(taskId);
			affectedTask.setName("error-task");
			affectedTask.setStatus(TaskDto.STATUS_RUNNING);
			affectedTask.setSyncType(TaskDto.SYNC_TYPE_SYNC);
			TaskDto errorTask = new TaskDto();
			errorTask.setId(taskId);
			errorTask.setStatus(TaskDto.STATUS_ERROR);

			when(dataSourceService.findAllDto(any(Query.class), eq(user))).thenReturn(Collections.singletonList(connection));
			when(taskService.findAllDto(any(Query.class), eq(user))).thenReturn(Collections.singletonList(affectedTask));
			when(taskService.findOne(any(Query.class), eq(user))).thenReturn(errorTask);
			when(fileService.storeFile(any(), anyString(), isNull(), anyMap())).thenReturn(new ObjectId());

			pkdSourceService.uploadPdk(new MultipartFile[]{jarFile}, Collections.singletonList(pdkSourceDto), false, user);

			verify(taskService).start(taskId, user);
		}

		@Test
		@SneakyThrows
		void testContinueRegistrationWhenPauseDoesNotTakeEffect() {
			ObjectId connectionId = new ObjectId();
			ObjectId taskId = new ObjectId();
			PdkSourceDto pdkSourceDto = mockPdkSourceDto();
			MultipartFile jarFile = mockJarFile();
			UserDetail user = mock(UserDetail.class);

			DataSourceConnectionDto connection = new DataSourceConnectionDto();
			connection.setId(connectionId);
			TaskDto affectedTask = new TaskDto();
			affectedTask.setId(taskId);
			affectedTask.setName("still-running-task");
			affectedTask.setStatus(TaskDto.STATUS_RUNNING);
			affectedTask.setSyncType(TaskDto.SYNC_TYPE_SYNC);

			ReflectionTestUtils.setField(pkdSourceService, "taskStopTimeoutMillis", 0L);
			when(dataSourceService.findAllDto(any(Query.class), eq(user))).thenReturn(Collections.singletonList(connection));
			when(taskService.findAllDto(any(Query.class), eq(user))).thenReturn(Collections.singletonList(affectedTask));
			when(taskService.findOne(any(Query.class), eq(user))).thenReturn(affectedTask);
			when(fileService.storeFile(any(), anyString(), isNull(), anyMap())).thenReturn(new ObjectId());

			pkdSourceService.uploadPdk(new MultipartFile[]{jarFile}, Collections.singletonList(pdkSourceDto), false, user);

			verify(taskService).pause(taskId, user, false);
			verify(fileService).storeFile(any(), anyString(), isNull(), anyMap());
			verify(taskService, never()).start(any(ObjectId.class), any(UserDetail.class));
		}

		@Test
		@SneakyThrows
		void testScheduleRestartWhenTaskIsStillStoppingAfterRegistration() {
			ObjectId connectionId = new ObjectId();
			ObjectId taskId = new ObjectId();
			PdkSourceDto pdkSourceDto = mockPdkSourceDto();
			MultipartFile jarFile = mockJarFile();
			UserDetail user = mock(UserDetail.class);

			DataSourceConnectionDto connection = new DataSourceConnectionDto();
			connection.setId(connectionId);
			TaskDto affectedTask = new TaskDto();
			affectedTask.setId(taskId);
			affectedTask.setName("stopping-task");
			affectedTask.setStatus(TaskDto.STATUS_RUNNING);
			affectedTask.setSyncType(TaskDto.SYNC_TYPE_SYNC);
			TaskDto stoppedTask = new TaskDto();
			stoppedTask.setId(taskId);
			stoppedTask.setStatus(TaskDto.STATUS_STOP);
			TaskDto stoppingTask = new TaskDto();
			stoppingTask.setId(taskId);
			stoppingTask.setStatus(TaskDto.STATUS_STOPPING);

			when(dataSourceService.findAllDto(any(Query.class), eq(user))).thenReturn(Collections.singletonList(connection));
			when(taskService.findAllDto(any(Query.class), eq(user))).thenReturn(Collections.singletonList(affectedTask));
			when(taskService.findOne(any(Query.class), eq(user))).thenReturn(stoppedTask, stoppingTask);
			when(fileService.storeFile(any(), anyString(), isNull(), anyMap())).thenReturn(new ObjectId());

			pkdSourceService.uploadPdk(new MultipartFile[]{jarFile}, Collections.singletonList(pdkSourceDto), false, user);

			verify(taskService).pause(taskId, user, false, true);
		}

		@Test
		void testRejectConcurrentRegistrationForSameConnector() {
			PdkSourceDto pdkSourceDto = mockPdkSourceDto();
			MultipartFile jarFile = mockJarFile();
			UserDetail user = mock(UserDetail.class);
			when(dbLockRepository.renew(anyString(), anyString(), any(Date.class))).thenReturn(LockStateEnums.NO);

			BizException exception = assertThrows(BizException.class, () -> pkdSourceService.uploadPdk(
					new MultipartFile[]{jarFile}, Collections.singletonList(pdkSourceDto), false, user));

			assertTrue(exception.getMessage().contains("Connector registration is already in progress"));
			verifyNoInteractions(dataSourceService, taskService, inspectService);
		}

		@Test
		@SneakyThrows
		void testRestartHeartbeatAndInspectTasks() {
			ObjectId connectionId = new ObjectId();
			ObjectId taskId = new ObjectId();
			ObjectId inspectId = new ObjectId();
			PdkSourceDto pdkSourceDto = mockPdkSourceDto();
			MultipartFile jarFile = mockJarFile();
			UserDetail user = mock(UserDetail.class);

			DataSourceConnectionDto connection = new DataSourceConnectionDto();
			connection.setId(connectionId);
			TaskDto heartbeatTask = new TaskDto();
			heartbeatTask.setId(taskId);
			heartbeatTask.setName("heartbeat-task");
			heartbeatTask.setStatus(TaskDto.STATUS_RUNNING);
			heartbeatTask.setSyncType(TaskDto.SYNC_TYPE_CONN_HEARTBEAT);
			TaskDto stoppedTask = new TaskDto();
			stoppedTask.setId(taskId);
			stoppedTask.setStatus(TaskDto.STATUS_STOP);
			InspectDto inspectTask = new InspectDto();
			inspectTask.setId(inspectId);
			inspectTask.setName("inspect-task");
			inspectTask.setStatus(InspectStatusEnum.RUNNING.getValue());
			InspectDto stoppedInspect = new InspectDto();
			stoppedInspect.setId(inspectId);
			stoppedInspect.setStatus(InspectStatusEnum.DONE.getValue());

			when(dataSourceService.findAllDto(any(Query.class), eq(user))).thenReturn(Collections.singletonList(connection));
			when(taskService.findAllDto(any(Query.class), eq(user))).thenReturn(Collections.singletonList(heartbeatTask));
			when(taskService.findOne(any(Query.class), eq(user))).thenReturn(stoppedTask);
			when(inspectService.findAllDto(any(Query.class), eq(user))).thenReturn(Collections.singletonList(inspectTask));
			when(inspectService.findById(inspectId)).thenReturn(stoppedInspect);
			when(fileService.storeFile(any(), anyString(), isNull(), anyMap())).thenReturn(new ObjectId());

			pkdSourceService.uploadPdk(new MultipartFile[]{jarFile}, Collections.singletonList(pdkSourceDto), false, user);

			verify(taskService).pause(taskId, user, false);
			verify(taskService).start(taskId, user);
			ArgumentCaptor<InspectDto> inspectStatusCaptor = ArgumentCaptor.forClass(InspectDto.class);
			verify(inspectService, times(2)).doExecuteInspect(any(), inspectStatusCaptor.capture(), eq(user));
			assertEquals(InspectStatusEnum.STOPPING.getValue(), inspectStatusCaptor.getAllValues().get(0).getStatus());
			assertEquals(InspectStatusEnum.SCHEDULING.getValue(), inspectStatusCaptor.getAllValues().get(1).getStatus());
		}

		@Test
		@SneakyThrows
		void testStopLogCollectorBeforeHeartbeatAndRestartBoth() {
			ObjectId connectionId = new ObjectId();
			ObjectId heartbeatId = new ObjectId();
			ObjectId logCollectorId = new ObjectId();
			PdkSourceDto pdkSourceDto = mockPdkSourceDto();
			MultipartFile jarFile = mockJarFile();
			UserDetail user = mock(UserDetail.class);

			DataSourceConnectionDto connection = new DataSourceConnectionDto();
			connection.setId(connectionId);
			TaskDto heartbeatTask = new TaskDto();
			heartbeatTask.setId(heartbeatId);
			heartbeatTask.setName("heartbeat-task");
			heartbeatTask.setStatus(TaskDto.STATUS_RUNNING);
			heartbeatTask.setSyncType(TaskDto.SYNC_TYPE_CONN_HEARTBEAT);
			TaskDto logCollectorTask = new TaskDto();
			logCollectorTask.setId(logCollectorId);
			logCollectorTask.setName("log-collector-task");
			logCollectorTask.setStatus(TaskDto.STATUS_RUNNING);
			logCollectorTask.setSyncType(TaskDto.SYNC_TYPE_LOG_COLLECTOR);
			TaskDto stoppedTask = new TaskDto();
			stoppedTask.setStatus(TaskDto.STATUS_STOP);

			when(dataSourceService.findAllDto(any(Query.class), eq(user))).thenReturn(Collections.singletonList(connection));
			when(taskService.findAllDto(any(Query.class), eq(user))).thenReturn(Arrays.asList(heartbeatTask, logCollectorTask));
			when(taskService.findOne(any(Query.class), eq(user))).thenReturn(stoppedTask);
			when(fileService.storeFile(any(), anyString(), isNull(), anyMap())).thenReturn(new ObjectId());

			pkdSourceService.uploadPdk(new MultipartFile[]{jarFile}, Collections.singletonList(pdkSourceDto), false, user);

			InOrder stopOrder = inOrder(taskService);
			stopOrder.verify(taskService).pause(logCollectorId, user, false);
			stopOrder.verify(taskService).pause(heartbeatId, user, false);
			verify(taskService).start(heartbeatId, user);
			verify(taskService).start(logCollectorId, user);
		}

		@Test
		@SneakyThrows
		void testReleaseRegistrationLockWhenUploadFails() {
			PdkSourceDto pdkSourceDto = mockPdkSourceDto();
			MultipartFile jarFile = mockJarFile();
			UserDetail user = mock(UserDetail.class);
			when(jarFile.getInputStream()).thenThrow(new IOException("upload failed"));

			assertThrows(BizException.class, () -> pkdSourceService.uploadPdk(
					new MultipartFile[]{jarFile}, Collections.singletonList(pdkSourceDto), false, user));

			verify(dbLockRepository).release(anyString(), anyString());
		}

		@Test
		@SneakyThrows
		void testSkipTaskLookupWhenConnectorHasNoConnection() {
			PdkSourceDto pdkSourceDto = mockPdkSourceDto();
			MultipartFile jarFile = mockJarFile();
			UserDetail user = mock(UserDetail.class);
			when(dataSourceService.findAllDto(any(Query.class), eq(user))).thenReturn(Collections.emptyList());
			when(fileService.storeFile(any(), anyString(), isNull(), anyMap())).thenReturn(new ObjectId());

			pkdSourceService.uploadPdk(new MultipartFile[]{jarFile}, Collections.singletonList(pdkSourceDto), false, user);

			verify(taskService, never()).findAllDto(any(Query.class), eq(user));
			verify(inspectService, never()).findAllDto(any(Query.class), eq(user));
		}

		private PdkSourceDto mockPdkSourceDto() {
			PdkSourceDto pdkSourceDto = mock(PdkSourceDto.class);
			when(pdkSourceDto.getId()).thenReturn("mysql");
			when(pdkSourceDto.getGroup()).thenReturn("io.tapdata");
			when(pdkSourceDto.getVersion()).thenReturn("1.0-SNAPSHOT");
			return pdkSourceDto;
		}

		@SneakyThrows
		private MultipartFile mockJarFile() {
			MultipartFile jarFile = mock(MultipartFile.class);
			InputStream inputStream = mock(InputStream.class);
			when(jarFile.getOriginalFilename()).thenReturn("mysql.jar");
			when(jarFile.getName()).thenReturn("mysql.jar");
			when(inputStream.read(any(byte[].class))).thenReturn(-1);
			when(jarFile.getInputStream()).thenReturn(inputStream);
			return jarFile;
		}
    }
    @Nested
    class checkJarMD5{
        @Test
        void testCheckJarMD5(){
            Criteria criteria = Criteria.where("metadata.pdkHash").is("111");
            Query query = new Query(criteria);
            criteria.and("metadata.pdkAPIBuildNumber").lte(14);
            query.with(Sort.by("metadata.pdkAPIBuildNumber").descending());
            GridFSFile gridFSFile = mock(GridFSFile.class);
            Document document = new Document();
            document.append("md5","123456");
            when(gridFSFile.getMetadata()).thenReturn(document);
            when(fileService.findOne(query)).thenReturn(gridFSFile);
            String actual = pkdSourceService.checkJarMD5("111", 14);
            assertEquals("123456",actual);
        }
        @Test
        void testCheckJarMd5CompatibleOldEngine(){
            Criteria criteria = Criteria.where("metadata.pdkHash").is("111").and("filename").is("a.jar");
            Query query = new Query(criteria);
            GridFSFile gridFSFile = mock(GridFSFile.class);
            Document document = new Document();
            document.append("md5","123456");
            when(gridFSFile.getMetadata()).thenReturn(document);
            when(fileService.findOne(query)).thenReturn(gridFSFile);
            String actual = pkdSourceService.checkJarMD5("111", "a.jar");
            assertEquals("123456",actual);
        }
        @Test
        void testCheckJarMD5WithFileName(){
            String fileName = "a.jar";
            Criteria criteria = Criteria.where("metadata.pdkHash").is("111");
            Query query = new Query(criteria);
            criteria.and("metadata.pdkAPIBuildNumber").lte(14);
            criteria.and("filename").is(fileName);
            query.with(Sort.by("metadata.pdkAPIBuildNumber").descending().and(Sort.by("uploadDate").descending()));
            GridFSFile gridFSFile = mock(GridFSFile.class);
            Document document = new Document();
            document.append("md5","123456");
            when(gridFSFile.getMetadata()).thenReturn(document);
            when(fileService.findOne(query)).thenReturn(gridFSFile);
            String actual = pkdSourceService.checkJarMD5("111", 14, fileName);
            assertEquals("123456",actual);
        }
    }

    @Nested
    class UploadDocsTest {
        Map<String, MultipartFile> docMap;
        LinkedHashMap<String, Object> messages;
        Map<String, Object> fileInfo;
        Map<String, Object> oemConfig;

        @BeforeEach
        void setUp() {
            docMap = new LinkedHashMap<>();
            messages = new LinkedHashMap<>();
            fileInfo = new LinkedHashMap<>();
            oemConfig = new LinkedHashMap<>();
        }

        @Test
        void testNullParams() {
            String filePath = "docs/test_en_US.md";

            // mock data
            MultipartFile file = mock(MultipartFile.class);
            docMap.put(filePath, file);
            messages.put("zh_CN", null);
            messages.put("en_US", new HashMap<String, String>() {{
                put("null", null); // test path is null
                put("not_start_doc", filePath); // test key not doc
            }});

            // docMap is null
            assertDoesNotThrow(() -> pkdSourceService.uploadDocs(null, messages, fileInfo, oemConfig));
            // docMap is empty
            assertDoesNotThrow(() -> pkdSourceService.uploadDocs(new LinkedHashMap<>(), messages, fileInfo, oemConfig));
            // messages is null
            assertDoesNotThrow(() -> pkdSourceService.uploadDocs(docMap, null, fileInfo, oemConfig));
            // messages lang is null
            assertDoesNotThrow(() -> pkdSourceService.uploadDocs(docMap, messages, fileInfo, oemConfig));

            // Verification results
            verify(fileService, times(0)).storeFile(any(), anyString(), any(), anyMap());
        }

        @Test
        void testReadmeDoc() {
            String filePath = "docs/test_en_US.md";

            // mock data
            MultipartFile file = mock(MultipartFile.class);
            when(file.getOriginalFilename()).thenReturn(filePath);
            docMap.put(filePath, file);
            messages.put("default", "en_US");
            messages.put("en_US", new HashMap<String, String>() {{
                put("doc", filePath);
            }});

            assertDoesNotThrow(() -> pkdSourceService.uploadDocs(docMap, messages, fileInfo, oemConfig));

            // Verification results
            verify(fileService, times(1)).storeFile(any(), anyString(), any(), anyMap());
        }

        @Test
        void testDocTips() {
            String filePath = "docs/test_en_US.md";

            // mock data
            MultipartFile file = mock(MultipartFile.class);
            when(file.getOriginalFilename()).thenReturn(filePath);
            docMap.put(filePath, file);
            messages = new LinkedHashMap<String, Object>(){{
                put("default", "en_US");
                put("en_US", new HashMap<String, String>() {{
                    put("doc:test", filePath);
                }});
                put("zh_CN", new HashMap<String, String>() {{
                    put("doc:test", filePath);
                }});
            }};

            assertDoesNotThrow(() -> pkdSourceService.uploadDocs(docMap, messages, fileInfo, oemConfig));

            // Verification results
            verify(fileService, times(1)).storeFile(any(), anyString(), any(), anyMap());
        }
    }

    @Nested
    class DownloadDocTest {

        String customerId = "test-customer-id";
        String pdkHash;
        Integer pdkBuildNumber;
        String filename;
        UserDetail user;
        HttpServletResponse response;

        @BeforeEach
        void setUp() {
            pdkHash = "123456";
            pdkBuildNumber = 1;
            filename = "test_en_US.md";
            user = mock(UserDetail.class);
            when(user.getCustomerId()).thenReturn(customerId);
            response = mock(HttpServletResponse.class);
        }

        @Test
        void testNotfoundDatasource() {
            pdkBuildNumber = null; // test pdkBuildNumber not add to criteria

            // mock data
            doAnswer(invocation -> {
                Query query = invocation.getArgument(0);
                assertNotNull(query);
                Document doc = query.getQueryObject();
                assertNotNull(doc);

                // Verification query prams
                assertTrue(doc.containsKey("pdkHash"));
                assertFalse(doc.containsKey("pdkAPIBuildNumber"));
                return null;
            }).when(dataSourceDefinitionService).findOne(any(Query.class));

            assertDoesNotThrow(() -> {
                pkdSourceService.downloadDoc(pdkHash, pdkBuildNumber, filename, user, response);

                // Verification results
                verify(response, times(1)).sendError(eq(404));
            });
        }

        @Test
        void testNotInScope() {
            // mock data
            DataSourceDefinitionDto sourceDefinitionDto = mock(DataSourceDefinitionDto.class);
            when(sourceDefinitionDto.getScope()).thenReturn("customer");
            when(dataSourceDefinitionService.findOne(any(Query.class))).thenReturn(sourceDefinitionDto);

            assertThrows(BizException.class, () -> pkdSourceService.downloadDoc(pdkHash, pdkBuildNumber, filename, user, response));
        }

        @Test
        void testIsCustomScope() {
            // mock data
            DataSourceDefinitionDto sourceDefinitionDto = mock(DataSourceDefinitionDto.class);
            when(sourceDefinitionDto.getScope()).thenReturn("customer");
            when(sourceDefinitionDto.getCustomId()).thenReturn(customerId);
            when(dataSourceDefinitionService.findOne(any(Query.class))).thenReturn(sourceDefinitionDto);

            assertDoesNotThrow(() -> {
                pkdSourceService.downloadDoc(pdkHash, pdkBuildNumber, filename, user, response);

                // Verification results
                verify(response, times(1)).sendError(eq(404));
            });
        }

        @Test
        void testSuccess() {
            ObjectId resourceId = ObjectId.get();

            // mock data
            LinkedHashMap<String, Object> messages = new LinkedHashMap<String, Object>(){{
                put(MessageUtil.getLanguage(), new HashMap<String, String>() {{
                    put(filename, resourceId.toHexString());
                }});
            }};
            DataSourceDefinitionDto sourceDefinitionDto = mock(DataSourceDefinitionDto.class);
            when(sourceDefinitionDto.getScope()).thenReturn("global");
            when(sourceDefinitionDto.getMessages()).thenReturn(messages);

            when(dataSourceDefinitionService.findOne(any(Query.class))).thenReturn(sourceDefinitionDto);

            assertDoesNotThrow(() -> {
                pkdSourceService.downloadDoc(pdkHash, pdkBuildNumber, filename, user, response);

                // Verification results
                verify(fileService, times(1)).viewImg(eq(resourceId), any());
                verify(response, times(0)).sendError(anyInt());
            });
        }
    }

    @Test
    public void testVersionCheck() {
        TcmService tcmService = mock(TcmService.class);
        List<DataSourceDefinitionDto> result = new ArrayList<>();
        when(dataSourceDefinitionService.findAll(any(Query.class))).thenReturn(result);
        when(tcmService.getLatestProductReleaseCreateTime()).thenReturn("2023-04-28");

        pkdSourceService.setTcmService(tcmService);

        Assertions.assertDoesNotThrow(() -> {
            List<PdkVersionCheckDto> a = pkdSourceService.versionCheck(5);
            Assertions.assertNotNull(a);
            Assertions.assertTrue(a.isEmpty());
        });

        for (int i = 0; i < 5; i++) {
            DataSourceDefinitionDto dto = new DataSourceDefinitionDto();
            dto.setPdkId("test");
            dto.setPdkAPIBuildNumber(i);
            dto.setPdkAPIVersion("api_v" + i);
            dto.setPdkHash("123" + i);
            dto.setLastUpdAt(new Date());
            result.add(dto);
        }

        List<PdkVersionCheckDto> versionCheckResult = pkdSourceService.versionCheck(2);
        Assertions.assertEquals(1, versionCheckResult.size());
        Assertions.assertEquals("1234", versionCheckResult.get(0).getPdkHash());
        Assertions.assertEquals(DateUtil.formatDateTime(result.get(4).getLastUpdAt()), versionCheckResult.get(0).getGitBuildTime());
        Assertions.assertTrue(versionCheckResult.get(0).isLatest());

        Map<String, String> manifest = new HashMap<>();
        String gitBuildTime = "2023-04-25T18:05:20+0800";
        manifest.put("Git-Build-Time", gitBuildTime);
        result.get(4).setManifest(manifest);

        versionCheckResult = pkdSourceService.versionCheck(2);

        Assertions.assertEquals(1, versionCheckResult.size());
        Assertions.assertEquals("1234", versionCheckResult.get(0).getPdkHash());
        String expectedGitBuildTime = DateUtil.formatDateTime(DateUtil.parse(gitBuildTime, "yyyy-MM-dd'T'HH:mm:ssZ"));
        Assertions.assertEquals(expectedGitBuildTime, versionCheckResult.get(0).getGitBuildTime());
        Assertions.assertTrue(versionCheckResult.get(0).isLatest());
    }

    @Test
    public void testUploadAndView() {

        String pdkHash = "test";
        int pdkBuildNumber = 1;
        UserDetail user = mock(UserDetail.class);
        PdkFileTypeEnum type = PdkFileTypeEnum.JAR;
        HttpServletResponse response = mock(HttpServletResponse.class);

        Assertions.assertDoesNotThrow(() -> {

            when(dataSourceDefinitionService.findOne(any(Query.class))).thenReturn(null);
            ArgumentCaptor<Integer> captor = ArgumentCaptor.forClass(Integer.class);
            doNothing().when(response).sendError(captor.capture());
            pkdSourceService.uploadAndView(pdkHash, pdkBuildNumber, user, type, response);

            Assertions.assertEquals(404, captor.getValue());
            DataSourceDefinitionDto dto = new DataSourceDefinitionDto();
            dto.setScope("customer");
            dto.setCustomId("customer_id");
            when(user.getCustomerId()).thenReturn("customer_id_1");
            when(dataSourceDefinitionService.findOne(any(Query.class))).thenReturn(dto);
            Assertions.assertThrows(BizException.class, () -> {
                pkdSourceService.uploadAndView(pdkHash, pdkBuildNumber, user, PdkFileTypeEnum.IMAGE, response);
            });

            when(user.getCustomerId()).thenReturn("customer_id");
            when(dataSourceDefinitionService.findOne(any(Query.class))).thenReturn(dto);
            pkdSourceService.uploadAndView(pdkHash, pdkBuildNumber, user, PdkFileTypeEnum.MARKDOWN, response);
            Assertions.assertTrue(captor.getAllValues().stream().anyMatch(p -> p == 404));

            dto.setMessages(new LinkedHashMap<>());
            dto.getMessages().put("zh_CN", new HashMap<String, String>(){{
                put("doc", new ObjectId().toHexString());
            }});
            dto.getMessages().put("en_US", new HashMap<String, String>(){{
                put("doc", new ObjectId().toHexString());
            }});
            dto.getMessages().put("zh_TW", new HashMap<String, String>(){{
                put("doc", new ObjectId().toHexString());
            }});

            pkdSourceService.uploadAndView(pdkHash, pdkBuildNumber, user, PdkFileTypeEnum.MARKDOWN, response);
            verify(fileService).viewImg(any(), any());

        });



    }
}
