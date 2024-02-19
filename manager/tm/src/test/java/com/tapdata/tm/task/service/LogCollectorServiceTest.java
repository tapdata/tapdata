package com.tapdata.tm.task.service;

import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.task.bean.ShareCdcTableInfo;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.UUIDUtil;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2024-02-19 15:56
 **/
@DisplayName("LogCollectorService Class Test")
public class LogCollectorServiceTest {
	private LogCollectorService logCollectorService;
	private UserDetail userDetail;

	@BeforeEach
	void setUp() {
		logCollectorService = spy(new LogCollectorService());
		userDetail = mock(UserDetail.class);
	}

	@Nested
	@DisplayName("GetShareCdcTableInfoPage Method Test")
	class GetShareCdcTableInfoPageTest {

		private Map<String, String> tableNameConnectionIdMap;
		private DataSourceService dataSourceService;
		private String connectionId;
		private String connectionName;
		private String taskId;
		private String nodeId;

		@BeforeEach
		void setUp() {
			taskId = new ObjectId().toHexString();
			nodeId = UUIDUtil.getUUID();
			connectionId = new ObjectId().toHexString();
			connectionName = "test connection";
			dataSourceService = mock(DataSourceService.class);
			ReflectionTestUtils.setField(logCollectorService, "dataSourceService", dataSourceService);
			tableNameConnectionIdMap = new HashMap<>();
			for (int i = 0; i < 50; i++) {
				tableNameConnectionIdMap.put("table_" + i, connectionId);
			}
			DataSourceConnectionDto dataSourceConnectionDto = new DataSourceConnectionDto();
			dataSourceConnectionDto.setName(connectionName);
			when(dataSourceService.findById(eq(MongoUtils.toObjectId(connectionId)), any(Field.class), eq(userDetail)))
					.thenReturn(dataSourceConnectionDto);
			doAnswer(invocationOnMock -> null).when(logCollectorService).setShareTableInfo(any(ShareCdcTableInfo.class), eq(userDetail), eq(taskId), eq(nodeId));
		}

		@Test
		@DisplayName("Test Different Page")
		void testDifferentPage() {
			Page<ShareCdcTableInfo> shareCdcTableInfoPage = logCollectorService.getShareCdcTableInfoPage(
					tableNameConnectionIdMap, 1, 10, userDetail, "", nodeId, taskId
			);
			verify(dataSourceService, times(10)).findById(eq(MongoUtils.toObjectId(connectionId)), any(Field.class), eq(userDetail));
			assertEquals(10, shareCdcTableInfoPage.getItems().size());
			assertEquals(50, shareCdcTableInfoPage.getTotal());
			assertEquals("table_0", shareCdcTableInfoPage.getItems().iterator().next().getName());

			shareCdcTableInfoPage = logCollectorService.getShareCdcTableInfoPage(
					tableNameConnectionIdMap, 2, 10, userDetail, "", nodeId, taskId
			);
			verify(dataSourceService, times(20)).findById(eq(MongoUtils.toObjectId(connectionId)), any(Field.class), eq(userDetail));
			assertEquals(10, shareCdcTableInfoPage.getItems().size());
			assertEquals(50, shareCdcTableInfoPage.getTotal());
			assertEquals("table_18", shareCdcTableInfoPage.getItems().iterator().next().getName());
		}

		@Test
		@DisplayName("Test Different size")
		void testDifferentSize() {
			Page<ShareCdcTableInfo> shareCdcTableInfoPage = logCollectorService.getShareCdcTableInfoPage(
					tableNameConnectionIdMap, 1, 1, userDetail, "", nodeId, taskId
			);
			verify(dataSourceService, times(1)).findById(eq(MongoUtils.toObjectId(connectionId)), any(Field.class), eq(userDetail));
			assertEquals(1, shareCdcTableInfoPage.getItems().size());
			assertEquals(50, shareCdcTableInfoPage.getTotal());
			assertEquals("table_0", shareCdcTableInfoPage.getItems().iterator().next().getName());

			shareCdcTableInfoPage = logCollectorService.getShareCdcTableInfoPage(
					tableNameConnectionIdMap, 1, 5, userDetail, "", nodeId, taskId
			);
			verify(dataSourceService, times(6)).findById(eq(MongoUtils.toObjectId(connectionId)), any(Field.class), eq(userDetail));
			assertEquals(5, shareCdcTableInfoPage.getItems().size());
			assertEquals(50, shareCdcTableInfoPage.getTotal());
			assertEquals("table_0", shareCdcTableInfoPage.getItems().iterator().next().getName());
		}

		@Test
		@DisplayName("Test KeyWord")
		void testKeyWord() {
			Page<ShareCdcTableInfo> shareCdcTableInfoPage = logCollectorService.getShareCdcTableInfoPage(
					tableNameConnectionIdMap, 1, 1, userDetail, "_30", nodeId, taskId
			);
			verify(dataSourceService, times(1)).findById(eq(MongoUtils.toObjectId(connectionId)), any(Field.class), eq(userDetail));
			assertEquals(1, shareCdcTableInfoPage.getItems().size());
			assertEquals(1, shareCdcTableInfoPage.getTotal());
			assertEquals("table_30", shareCdcTableInfoPage.getItems().iterator().next().getName());
		}
	}
}
