package com.tapdata.tm.metadatainstance.service;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.bean.SourceDto;
import com.tapdata.tm.commons.util.MetaDataBuilderUtils;
import com.tapdata.tm.commons.util.MetaType;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.ds.service.impl.DataSourceServiceImpl;
import com.tapdata.tm.metadatainstance.repository.MetadataInstancesRepository;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2024-04-11 17:55
 **/
@DisplayName("Class MetadataInstancesServiceImpl Test")
public class MetadataInstancesServiceImplTest {

	private MetadataInstancesRepository metadataInstancesRepository;
	private MetadataInstancesServiceImpl metadataInstancesService;
	private UserDetail userDetail;

	@BeforeEach
	void setUp() {
		metadataInstancesRepository = mock(MetadataInstancesRepository.class);
		metadataInstancesService = new MetadataInstancesServiceImpl(metadataInstancesRepository);
		userDetail = mock(UserDetail.class);
	}

	@Nested
	class checkSetLastUpdateTest {
		@BeforeEach
		void setUp() {
			metadataInstancesService = spy(metadataInstancesService);
		}

		@Test
		@DisplayName("Test main process")
		void testMainProcess() {
			long time1 = new Date().getTime();
			long time2 = new Date().getTime();
			String connectionId1 = new ObjectId().toHexString();
			SourceDto sourceDto1 = new SourceDto();
			sourceDto1.set_id(connectionId1);
			String connectionId2 = new ObjectId().toHexString();
			SourceDto sourceDto2 = new SourceDto();
			sourceDto2.set_id(connectionId2);
			List<MetadataInstancesDto> metadataInstancesDtoList = new ArrayList<>();
			MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
			metadataInstancesDto1.setLastUpdate(1L);
			metadataInstancesDto1.setSource(sourceDto1);
			metadataInstancesDto1.setConnectionId(connectionId1);
			metadataInstancesDtoList.add(metadataInstancesDto1);
			MetadataInstancesDto metadataInstancesDto2 = new MetadataInstancesDto();
			metadataInstancesDto2.setSource(sourceDto1);
			metadataInstancesDto2.setLastUpdate(null);
			metadataInstancesDtoList.add(metadataInstancesDto2);
			MetadataInstancesDto metadataInstancesDto3 = new MetadataInstancesDto();
			metadataInstancesDto3.setSource(sourceDto1);
			metadataInstancesDto3.setLastUpdate(null);
			metadataInstancesDtoList.add(metadataInstancesDto3);
			MetadataInstancesDto metadataInstancesDto4 = new MetadataInstancesDto();
			metadataInstancesDto4.setSource(sourceDto2);
			metadataInstancesDto4.setLastUpdate(null);
			metadataInstancesDtoList.add(metadataInstancesDto4);

			doReturn(time1).when(metadataInstancesService).findDatabaseMetadataInstanceLastUpdate(connectionId1, userDetail);
			doReturn(time2).when(metadataInstancesService).findDatabaseMetadataInstanceLastUpdate(connectionId2, userDetail);

			assertDoesNotThrow(() -> metadataInstancesService.checkSetLastUpdate(metadataInstancesDtoList, userDetail));

			verify(metadataInstancesService, times(2)).findDatabaseMetadataInstanceLastUpdate(any(), any());
			assertEquals(1L, metadataInstancesDto1.getLastUpdate());
			assertEquals(time1, metadataInstancesDto2.getLastUpdate());
			assertEquals(time1, metadataInstancesDto3.getLastUpdate());
			assertEquals(time2, metadataInstancesDto4.getLastUpdate());
		}

		@Test
		@DisplayName("Test all have last update")
		void testAllHaveLastUpdate() {
			long time = new Date().getTime();
			List<MetadataInstancesDto> metadataInstancesDtoList = new ArrayList<>();
			MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
			metadataInstancesDto1.setLastUpdate(1L);
			metadataInstancesDtoList.add(metadataInstancesDto1);
			MetadataInstancesDto metadataInstancesDto2 = new MetadataInstancesDto();
			metadataInstancesDto2.setLastUpdate(1L);
			metadataInstancesDtoList.add(metadataInstancesDto2);
			MetadataInstancesDto metadataInstancesDto3 = new MetadataInstancesDto();
			metadataInstancesDto3.setLastUpdate(1L);
			metadataInstancesDtoList.add(metadataInstancesDto3);

			doReturn(time).when(metadataInstancesService).findDatabaseMetadataInstanceLastUpdate(any(), eq(userDetail));

			assertDoesNotThrow(() -> metadataInstancesService.checkSetLastUpdate(metadataInstancesDtoList, userDetail));

			verify(metadataInstancesService, never()).findDatabaseMetadataInstanceLastUpdate(any(), eq(userDetail));
			assertEquals(1L, metadataInstancesDto1.getLastUpdate());
			assertEquals(1L, metadataInstancesDto2.getLastUpdate());
			assertEquals(1L, metadataInstancesDto3.getLastUpdate());
		}

		@Test
		@DisplayName("Test input empty or null list")
		void testInputEmptyOrNullList() {
			assertDoesNotThrow(() -> metadataInstancesService.checkSetLastUpdate(new ArrayList<>(), userDetail));
			assertDoesNotThrow(() -> metadataInstancesService.checkSetLastUpdate(null, userDetail));

			verify(metadataInstancesService, never()).findDatabaseMetadataInstanceLastUpdate(any(), eq(userDetail));
		}

		@Test
		@DisplayName("Test find last update return null")
		void testFindLastUpdateReturnNull() {
			String connectionId = new ObjectId().toHexString();
			SourceDto sourceDto = new SourceDto();
			sourceDto.set_id(connectionId);
			List<MetadataInstancesDto> metadataInstancesDtoList = new ArrayList<>();
			MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
			metadataInstancesDto1.setOriginalName("test-original-name1");
			metadataInstancesDto1.setQualifiedName("test-qualified-name1");
			metadataInstancesDto1.setLastUpdate(1L);
			metadataInstancesDto1.setSource(sourceDto);
			metadataInstancesDto1.setConnectionId(connectionId);
			metadataInstancesDtoList.add(metadataInstancesDto1);
			MetadataInstancesDto metadataInstancesDto2 = new MetadataInstancesDto();
			metadataInstancesDto2.setOriginalName("test-original-name2");
			metadataInstancesDto2.setQualifiedName("test-qualified-name2");
			metadataInstancesDto2.setSource(sourceDto);
			metadataInstancesDto2.setLastUpdate(null);
			metadataInstancesDtoList.add(metadataInstancesDto2);
			MetadataInstancesDto metadataInstancesDto3 = new MetadataInstancesDto();
			metadataInstancesDto3.setOriginalName("test-original-name3");
			metadataInstancesDto3.setQualifiedName("test-qualified-name3");
			metadataInstancesDto3.setSource(sourceDto);
			metadataInstancesDto3.setLastUpdate(null);
			metadataInstancesDtoList.add(metadataInstancesDto3);
			doReturn(null).when(metadataInstancesService).findDatabaseMetadataInstanceLastUpdate(connectionId, userDetail);

			assertThrows(BizException.class, () -> metadataInstancesService.checkSetLastUpdate(metadataInstancesDtoList, userDetail));
		}
	}

	@Nested
	@DisplayName("Method findDatabaseMetadataInstanceLastUpdate Test")
	class findDatabaseMetadataInstanceLastUpdateTest {

		private DataSourceService dataSourceService;

		@BeforeEach
		void setUp() {
			dataSourceService = mock(DataSourceServiceImpl.class);
			ReflectionTestUtils.setField(metadataInstancesService, "dataSourceService", dataSourceService);
			metadataInstancesService = spy(metadataInstancesService);
		}

		@Test
		@DisplayName("Test main process")
		void testMainProcess() {
			try (
					MockedStatic<MetaDataBuilderUtils> metaDataBuilderUtilsMockedStatic = mockStatic(MetaDataBuilderUtils.class)
			) {
				long time = new Date().getTime();
				String connectionId = new ObjectId().toHexString();
				DataSourceConnectionDto dataSourceConnectionDto = new DataSourceConnectionDto();
				when(dataSourceService.findById(any(ObjectId.class), eq(userDetail))).thenReturn(dataSourceConnectionDto);
				String qualifiedName = "test-qualified-name";
				metaDataBuilderUtilsMockedStatic.when(() -> MetaDataBuilderUtils.generateQualifiedName(MetaType.database.name(), dataSourceConnectionDto, null)).thenReturn(qualifiedName);
				MetadataInstancesDto databaseMetaDto = new MetadataInstancesDto();
				databaseMetaDto.setLastUpdate(time);
				doAnswer(invocationOnMock -> {
					Object argument1 = invocationOnMock.getArgument(0);
					assertTrue(argument1 instanceof Query);
					Query query = (Query) argument1;
					Document queryObject = query.getQueryObject();
					assertNotNull(queryObject);
					assertEquals(1, queryObject.size());
					assertEquals(qualifiedName, queryObject.get("qualified_name"));
					assertNotNull(query.getFieldsObject());
					assertEquals(1, query.getFieldsObject().size());
					assertEquals(1, query.getFieldsObject().get("lastUpdate"));
					return databaseMetaDto;
				}).when(metadataInstancesService).findOne(any(Query.class));

				Long actual = metadataInstancesService.findDatabaseMetadataInstanceLastUpdate(connectionId, userDetail);
				assertEquals(time, actual);
			}
		}

		@Test
		@DisplayName("Test find connection return null")
		void testFindConnectionReturnNull() {
			String connectionId = new ObjectId().toHexString();
			when(dataSourceService.findById(any(ObjectId.class), eq(userDetail))).thenReturn(null);

			assertThrows(BizException.class, () -> metadataInstancesService.findDatabaseMetadataInstanceLastUpdate(connectionId, userDetail));
		}

		@Test
		@DisplayName("Test find database metadata return null")
		void testFindDatabaseMetadataReturnNull() {
			try (
					MockedStatic<MetaDataBuilderUtils> metaDataBuilderUtilsMockedStatic = mockStatic(MetaDataBuilderUtils.class)
			) {
				long time = new Date().getTime();
				String connectionId = new ObjectId().toHexString();
				DataSourceConnectionDto dataSourceConnectionDto = new DataSourceConnectionDto();
				when(dataSourceService.findById(new ObjectId(connectionId), userDetail)).thenReturn(dataSourceConnectionDto);
				String qualifiedName = "test-qualified-name";
				metaDataBuilderUtilsMockedStatic.when(() -> MetaDataBuilderUtils.generateQualifiedName(MetaType.database.name(), dataSourceConnectionDto, null)).thenReturn(qualifiedName);
				MetadataInstancesDto databaseMetaDto = new MetadataInstancesDto();
				databaseMetaDto.setLastUpdate(time);
				doReturn(null).when(metadataInstancesService).findOne(any(Query.class));

				assertThrows(BizException.class, () -> metadataInstancesService.findDatabaseMetadataInstanceLastUpdate(connectionId, userDetail));
			}
		}
	}
}
