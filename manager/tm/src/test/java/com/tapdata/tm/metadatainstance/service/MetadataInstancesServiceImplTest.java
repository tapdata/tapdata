package com.tapdata.tm.metadatainstance.service;

import cn.hutool.core.bean.BeanUtil;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.LogCollecotrConnConfig;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.dag.logCollector.VirtualTargetNode;
import com.tapdata.tm.commons.dag.nodes.DataNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.dag.process.MigrateFieldRenameProcessorNode;
import com.tapdata.tm.commons.dag.process.MigrateProcessorNode;
import com.tapdata.tm.commons.dag.vo.FieldChangeRule;
import com.tapdata.tm.commons.dag.vo.SyncObjects;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.schema.bean.Schema;
import com.tapdata.tm.commons.schema.bean.SourceDto;
import com.tapdata.tm.commons.schema.bean.Table;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.MetaDataBuilderUtils;
import com.tapdata.tm.commons.util.MetaType;
import com.tapdata.tm.commons.util.PdkSchemaConvert;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.discovery.bean.DiscoveryFieldDto;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionServiceImpl;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.ds.service.impl.DataSourceServiceImpl;
import com.tapdata.tm.metadatainstance.bean.MultiPleTransformReq;
import com.tapdata.tm.metadatainstance.dto.DataType2TapTypeDto;
import com.tapdata.tm.metadatainstance.dto.DataTypeCheckMultipleVo;
import com.tapdata.tm.metadatainstance.entity.MetadataInstancesEntity;
import com.tapdata.tm.metadatainstance.param.ClassificationParam;
import com.tapdata.tm.metadatainstance.param.TablesSupportInspectParam;
import com.tapdata.tm.metadatainstance.repository.MetadataInstancesRepository;
import com.tapdata.tm.metadatainstance.vo.MetaTableCheckVo;
import com.tapdata.tm.metadatainstance.vo.MetadataInstancesVo;
import com.tapdata.tm.metadatainstance.vo.TableSupportInspectVo;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.IDataPermissionHelper;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.dto.UserDto;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MetadataUtil;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.SchemaTransformUtils;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.mapping.TypeExprResult;
import io.tapdata.entity.mapping.type.TapStringMapping;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.utils.DataMap;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.internal.verification.Times;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;

import static com.tapdata.tm.utils.MongoUtils.toObjectId;
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
		metadataInstancesService = spy(metadataInstancesService);
		userDetail = mock(UserDetail.class);
		new DataPermissionHelper(mock(IDataPermissionHelper.class));
	}

	@Nested
	class checkSetLastUpdateTest {

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
	@Nested
	class ModifyByIdTest{
		@Test
		void testModifyByIdNormal(){
			ObjectId id = mock(ObjectId.class);
			MetadataInstancesDto record = new MetadataInstancesDto();
			MetadataInstancesEntity entity = new MetadataInstancesEntity();
			doReturn(entity).when(metadataInstancesRepository).save(any(MetadataInstancesEntity.class),any(UserDetail.class));
			MetadataInstancesDto actual = metadataInstancesService.modifyById(id, record, userDetail);
			verify(metadataInstancesService,new Times(2)).beforeCreateOrUpdate(record,userDetail);
			verify(metadataInstancesService,new Times(1)).beforeUpdateById(id,record);
			verify(metadataInstancesService,new Times(1)).save(record,userDetail);
			verify(metadataInstancesService,new Times(1)).afterUpdateById(id,record);
			assertEquals(null,actual);
		}
	}
	@Nested
	class beforeSaveTest{
		@Test
		@DisplayName("test beforeSave method when record is null")
		void test1(){
			MetadataInstancesDto record = null;
			try (MockedStatic<ObjectId> mb = Mockito
					.mockStatic(ObjectId.class)) {
				mb.when(ObjectId::get).thenReturn(mock(ObjectId.class));
				metadataInstancesService.beforeSave(record,userDetail);
				mb.verify(() -> ObjectId.get(),new Times(0));
				verify(metadataInstancesService,new Times(0)).beforeCreateOrUpdate(record,userDetail);
			}
		}
		@Test
		@DisplayName("test beforeSave method when record fields is empty")
		void test2(){
			MetadataInstancesDto record = new MetadataInstancesDto();
			ArrayList<Field> fields = new ArrayList<>();
			record.setFields(fields);
			try (MockedStatic<ObjectId> mb = Mockito
					.mockStatic(ObjectId.class)) {
				mb.when(ObjectId::get).thenReturn(mock(ObjectId.class));
				metadataInstancesService.beforeSave(record,userDetail);
				mb.verify(() -> ObjectId.get(),new Times(0));
				verify(metadataInstancesService,new Times(1)).beforeCreateOrUpdate(record,userDetail);
			}
		}
		@Test
		@DisplayName("test beforeSave method when record fields is not empty")
		void test3(){
			MetadataInstancesDto record = new MetadataInstancesDto();
			ArrayList<Field> fields = new ArrayList<>();
			fields.add(new Field());
			Field field = new Field();
			field.setId("111");
			fields.add(field);
			record.setFields(fields);
			try (MockedStatic<ObjectId> mb = Mockito
					.mockStatic(ObjectId.class)) {
				mb.when(ObjectId::get).thenReturn(mock(ObjectId.class));
				metadataInstancesService.beforeSave(record,userDetail);
				mb.verify(() -> ObjectId.get(),new Times(1));
				verify(metadataInstancesService,new Times(1)).beforeCreateOrUpdate(record,userDetail);
			}
		}
	}
	@Nested
	class ListTest{
		private Filter filter;
		@BeforeEach
		void beforeEach(){
			filter = new Filter();
		}
		@Test
		@DisplayName("test list method normal")
		void test1(){
			Where where = new Where();
			where.put("source.id","111");
			Map<String, List> classficitionIn = new HashMap<>();
			List<String> classficitionIds = new ArrayList<>();
			classficitionIds.add("65bc933c6129fe73d7858b40");
			classficitionIn.put("$in",classficitionIds);
			where.put("classifications.id",classficitionIn);
			where.put("taskId","333");
			filter.setWhere(where);
			when(metadataInstancesRepository.whereToCriteria(where)).thenReturn(mock(Criteria.class));
			doNothing().when(metadataInstancesService).afterFindAll(anyList());
			metadataInstancesService.list(filter,userDetail);
			verify(metadataInstancesService,new Times(1)).findAll(any(Query.class));
			verify(metadataInstancesService,new Times(0)).find(filter);
		}
		@Test
		@DisplayName("test list method simple")
		void test2(){
			Where where = new Where();
			filter.setWhere(where);
			when(metadataInstancesRepository.whereToCriteria(where)).thenReturn(mock(Criteria.class));
			doNothing().when(metadataInstancesService).afterFindAll(anyList());
			metadataInstancesService.list(filter,userDetail);
			verify(metadataInstancesService,new Times(0)).findAll(any(Query.class));
			verify(metadataInstancesService,new Times(1)).find(filter);
		}
		@Test
		@DisplayName("test list method when filter getLimit equals 0")
		void test3(){
			Where where = new Where();
			where.put("taskId","333");
			filter.setWhere(where);
			filter.setSize(0);
			when(metadataInstancesRepository.whereToCriteria(where)).thenReturn(mock(Criteria.class));
			doNothing().when(metadataInstancesService).afterFindAll(anyList());
			metadataInstancesService.list(filter,userDetail);
			verify(metadataInstancesService,new Times(1)).findAll(any(Query.class));
			verify(metadataInstancesService,new Times(0)).find(filter);
		}
	}
	@Nested
	class FindInspectTest{
		private Filter filter;
		@BeforeEach
		void beforeEach(){
			filter = new Filter();
		}
		@Test
		@DisplayName("test findInspect method when metadataInstancesDtoList is not empty")
		void test1(){
			Where where = new Where();
			where.put("source.id","111");
			filter.setWhere(where);
			List metadataInstancesDtoList = new ArrayList<>();
			MetadataInstancesDto metadataInstancesDto = mock(MetadataInstancesDto.class);
			metadataInstancesDtoList.add(metadataInstancesDto);
			when(metadataInstancesService.findAll(filter)).thenReturn(metadataInstancesDtoList);
			List<MetadataInstancesVo> actual = metadataInstancesService.findInspect(filter, userDetail);
			assertEquals(BeanUtil.copyProperties(metadataInstancesDto, MetadataInstancesVo.class),actual.get(0));
		}
		@Test
		@DisplayName("test findInspect method when metadataInstancesDtoList is empty")
		void test2(){
			Where where = new Where();
			filter.setWhere(where);
			List<MetadataInstancesVo> actual = metadataInstancesService.findInspect(filter, userDetail);
			assertEquals(0,actual.size());
		}
	}
	@Nested
	class QueryByIdTest{
		@Test
		void testQueryByIdNormal(){
			ObjectId id = mock(ObjectId.class);
			com.tapdata.tm.base.dto.Field fields = mock(com.tapdata.tm.base.dto.Field.class);
			MetadataInstancesDto metadataInstancesDto = mock(MetadataInstancesDto.class);
			doReturn(metadataInstancesDto).when(metadataInstancesService).findById(id,fields,userDetail);
			metadataInstancesService.queryById(id,fields,userDetail);
			verify(metadataInstancesService).afterFindOne(metadataInstancesDto,userDetail);
			verify(metadataInstancesService).afterFindById(metadataInstancesDto);
			verify(metadataInstancesService).afterFind(metadataInstancesDto);
		}
	}
	@Nested
	class QueryByOneTest{
		private Filter filter;
		@BeforeEach
		void beforeEach(){
			filter = new Filter();
		}
		@Test
		@DisplayName("test queryByOne method normal")
		void test1(){
			Where where = new Where();
			where.put("source.id","111");
			filter.setWhere(where);
			MetadataInstancesDto metadataInstancesDto = mock(MetadataInstancesDto.class);
			doReturn(metadataInstancesDto).when(metadataInstancesService).findOne(filter,userDetail);
			metadataInstancesService.queryByOne(filter,userDetail);
			verify(metadataInstancesService).afterFindOne(metadataInstancesDto,userDetail);
			verify(metadataInstancesService).afterFind(metadataInstancesDto);
		}
		@Test
		@DisplayName("test queryByOne method when sourceId is null")
		void test2(){
			Where where = new Where();
			filter.setWhere(where);
			MetadataInstancesDto metadataInstancesDto = mock(MetadataInstancesDto.class);
			doReturn(metadataInstancesDto).when(metadataInstancesService).findOne(filter,userDetail);
			metadataInstancesService.queryByOne(filter,userDetail);
			verify(metadataInstancesService).afterFindOne(metadataInstancesDto,userDetail);
			verify(metadataInstancesService).afterFind(metadataInstancesDto);
		}
	}
	@Nested
	class JobStatsTest{
		@Test
		void testJobStatsNormal(){
			long skip = 3L;
			int limit = 1;
			MongoTemplate template = mock(MongoTemplate.class);
			when(metadataInstancesRepository.getMongoOperations()).thenReturn(template);
			AggregationResults aggregationResults = mock(AggregationResults.class);
			when(template.aggregate(any(Aggregation.class),anyString(),any(Class.class))).thenReturn(aggregationResults);
			List result = new ArrayList();
			MetadataInstancesEntity entity = new MetadataInstancesEntity();
			ObjectId id = mock(ObjectId.class);
			entity.setId(id);
			result.add(entity);
			when(aggregationResults.getMappedResults()).thenReturn(result);
			List<MetadataInstancesDto> actual = metadataInstancesService.jobStats(skip, limit);
			assertEquals(id,actual.get(0).getId());
		}
	}
	@Nested
	class SchemaTest{
		private Filter filter = mock(Filter.class);
		@Test
		@DisplayName("test schema method when page getTotal equals 0")
		void test1(){
			Page<MetadataInstancesDto> page = new Page<>();
			page.setTotal(0);
			doReturn(page).when(metadataInstancesService).find(filter,userDetail);
			List<MetadataInstancesDto> actual = metadataInstancesService.schema(filter, userDetail);
			assertEquals(null,actual);
		}
		@Test
		@DisplayName("test schema method normal")
		void test2(){
			try (MockedStatic<SchemaTransformUtils> mb = Mockito
					.mockStatic(SchemaTransformUtils.class)) {
				Schema schema = mock(Schema.class);
				mb.when(()->SchemaTransformUtils.newSchema2oldSchema(anyList())).thenReturn(schema);
				Page<MetadataInstancesDto> page = new Page<>();
				page.setTotal(1);
				List<MetadataInstancesDto> metadataInstancesDtoList = new ArrayList<>();
				MetadataInstancesDto meta1 = new MetadataInstancesDto();
				MetadataInstancesDto meta2 = new MetadataInstancesDto();
				meta1.setMetaType("mongo_view");
				meta1.setPipline(mock(Object.class));
				metadataInstancesDtoList.add(meta1);
				meta2.setMetaType("job");
				metadataInstancesDtoList.add(meta2);
				page.setItems(metadataInstancesDtoList);
				doReturn(page).when(metadataInstancesService).find(filter,userDetail);
				List<MetadataInstancesDto> actual = metadataInstancesService.schema(filter, userDetail);
				assertEquals(2,actual.size());
				assertEquals(schema,actual.get(0).getSchema());
				mb.verify(() -> SchemaTransformUtils.newSchema2oldSchema(anyList()),new Times(1));
			}
		}
	}
	@Nested
	class LienageTest{
		@Test
		void testLienageNormal(){
			MongoTemplate template = mock(MongoTemplate.class);
			when(metadataInstancesRepository.getMongoOperations()).thenReturn(template);
			AggregationResults aggregationResults = mock(AggregationResults.class);
			when(template.aggregate(any(Aggregation.class),anyString(),any(Class.class))).thenReturn(aggregationResults);
			List result = new ArrayList();
			MetadataInstancesEntity entity = new MetadataInstancesEntity();
			ObjectId id = mock(ObjectId.class);
			entity.setId(id);
			result.add(entity);
			when(aggregationResults.getMappedResults()).thenReturn(result);
			List<MetadataInstancesDto> actual = metadataInstancesService.lienage("111");
			assertEquals(id,actual.get(0).getId());
		}
	}
	@Nested
	class BeforeCreateOrUpdateTest{
		private MetadataInstancesDto data;
		private DataSourceServiceImpl dataSourceService;
		private DataSourceDefinitionServiceImpl dataSourceDefinitionService;
		@BeforeEach
		void beforeEach(){
			dataSourceService = mock(DataSourceServiceImpl.class);
			dataSourceDefinitionService = mock(DataSourceDefinitionServiceImpl.class);
			ReflectionTestUtils.setField(metadataInstancesService, "dataSourceService", dataSourceService);
			ReflectionTestUtils.setField(metadataInstancesService, "dataSourceDefinitionService", dataSourceDefinitionService);
		}
		@Test
		@DisplayName("test beforeCreateOrUpdate method normal")
		void test1(){
			try (MockedStatic<PdkSchemaConvert> mb = Mockito
					.mockStatic(PdkSchemaConvert.class)) {
				TapTable tapTable = mock(TapTable.class);
				data = new MetadataInstancesDto();
				data.setConnectionId("CONN_65bc933f6129fe73d7858d6f");
				data.setMetaType("collection");
				ArrayList<Field> fields = new ArrayList<>();
				fields.add(new Field());
				Field field = new Field();
				field.setId("111");
				fields.add(field);
				data.setFields(fields);
				mb.when(()->PdkSchemaConvert.toPdk(data)).thenReturn(tapTable);
				DataSourceConnectionDto connectionDto = mock(DataSourceConnectionDto.class);
				when(connectionDto.getId()).thenReturn(mock(ObjectId.class));
				when(dataSourceService.findById(toObjectId("65bc933f6129fe73d7858d6f"),userDetail)).thenReturn(connectionDto);
				MetadataInstancesDto metadataInstancesDto = mock(MetadataInstancesDto.class);
				doReturn(metadataInstancesDto).when(metadataInstancesService).findOne(any(Query.class));
				when(metadataInstancesDto.getId()).thenReturn(mock(ObjectId.class));
				LinkedHashMap<String, TapField> nameFiledMap = new LinkedHashMap<>();
				TapField tapField = mock(TapField.class);
				nameFiledMap.put("test",tapField);
				when(tapTable.getNameFieldMap()).thenReturn(nameFiledMap);
				when(dataSourceDefinitionService.getByDataSourceType(anyString(),any(UserDetail.class))).thenReturn(mock(DataSourceDefinitionDto.class));
				mb.when(()->PdkSchemaConvert.fromPdk(tapTable)).thenReturn(mock(MetadataInstancesDto.class));
				metadataInstancesService.beforeCreateOrUpdate(data,userDetail);
			}
		}
	}
	@Nested
	class AfterFindByIdTest{
		private MetadataInstancesDto result;
		@Test
		@DisplayName("test afterFindById method when result fields is empty")
		void test1(){
			result = mock(MetadataInstancesDto.class);
			when(result.getFields()).thenReturn(new ArrayList<>());
			metadataInstancesService.afterFindById(result);
			assertEquals(0,result.getFields().size());
		}
		@Test
		@DisplayName("test afterFindById method normal")
		void test2(){
			result = mock(MetadataInstancesDto.class);
			List<Field> fields = new ArrayList<>();
			Field field = new Field();
			field.setPrimaryKeyPosition(1);
			field.setForeignKeyPosition(2);
			fields.add(field);
			when(result.getFields()).thenReturn(fields);
			metadataInstancesService.afterFindById(result);
			assertEquals(true,field.getPrimaryKey());
			assertEquals(true,field.getForeignKey());
		}
		@Test
		@DisplayName("test afterFindById method when key position is null")
		void test3(){
			result = mock(MetadataInstancesDto.class);
			List<Field> fields = new ArrayList<>();
			Field field = new Field();
			fields.add(field);
			when(result.getFields()).thenReturn(fields);
			metadataInstancesService.afterFindById(result);
			assertEquals(false,field.getPrimaryKey());
			assertEquals(false,field.getForeignKey());
		}
		@Test
		@DisplayName("test afterFindById method when key position less than 0")
		void test4(){
			result = mock(MetadataInstancesDto.class);
			List<Field> fields = new ArrayList<>();
			Field field = new Field();
			field.setPrimaryKeyPosition(0);
			field.setForeignKeyPosition(0);
			fields.add(field);
			when(result.getFields()).thenReturn(fields);
			metadataInstancesService.afterFindById(result);
			assertEquals(false,field.getPrimaryKey());
			assertEquals(false,field.getForeignKey());
		}
	}
	@Nested
	class AfterFindOneTest{
		private MetadataInstancesDto result;
		@Test
		@DisplayName("test afterFindOne method when result is null")
		void test1(){
			result = null;
			metadataInstancesService.afterFindOne(result,userDetail);
			verify(metadataInstancesService,never()).findAllDto(any(Query.class),any(UserDetail.class));
		}
		@Test
		@DisplayName("test afterFindOne method when metaType is database")
		void test2(){
			result = mock(MetadataInstancesDto.class);
			when(result.getMetaType()).thenReturn("database");
			when(result.getId()).thenReturn(mock(ObjectId.class));
			metadataInstancesService.afterFindOne(result,userDetail);
			verify(metadataInstancesService,new Times(1)).findAllDto(any(Query.class),any(UserDetail.class));
		}
		@Test
		@DisplayName("test afterFindOne method when metaType is collection")
		void test3(){
			result = mock(MetadataInstancesDto.class);
			when(result.getMetaType()).thenReturn("collection");
			when(result.getId()).thenReturn(mock(ObjectId.class));
			metadataInstancesService.afterFindOne(result,userDetail);
			verify(metadataInstancesService,new Times(1)).findAllDto(any(Query.class),any(UserDetail.class));
		}
		@Test
		@DisplayName("test afterFindOne method when metaType is collection and collections is not empty")
		void test4(){
			result = mock(MetadataInstancesDto.class);
			when(result.getMetaType()).thenReturn("collection");
			List<MetadataInstancesDto> collections = new ArrayList<>();
			MetadataInstancesDto metadataInstancesDto = new MetadataInstancesDto();
			metadataInstancesDto.setOriginalName("111");
			collections.add(metadataInstancesDto);
			doReturn(collections).when(metadataInstancesService).findAllDto(any(Query.class),any(UserDetail.class));
			metadataInstancesService.afterFindOne(result,userDetail);
			verify(metadataInstancesService,new Times(1)).findAllDto(any(Query.class),any(UserDetail.class));
			verify(result,new Times(1)).setDatabase(anyString());
		}
		@Test
		@DisplayName("test afterFindOne method when metaType is view")
		void test5(){
			result = mock(MetadataInstancesDto.class);
			when(result.getMetaType()).thenReturn("view");
			when(result.getId()).thenReturn(mock(ObjectId.class));
			metadataInstancesService.afterFindOne(result,userDetail);
			verify(metadataInstancesService,new Times(1)).findAllDto(any(Query.class),any(UserDetail.class));
		}
	}
	@Nested
	class AfterFindAllTest{
		private List<MetadataInstancesDto> results;
		private UserService userService;
		@BeforeEach
		void beforeEach(){
			userService = mock(UserService.class);
			ReflectionTestUtils.setField(metadataInstancesService,"userService",userService);
		}
		@Test
		@DisplayName("test afterFindAll method normal")
		void test1(){
			MetadataInstancesDto metadataInstancesDto = new MetadataInstancesDto();
			metadataInstancesDto.setDatabaseId("65bc933c6129fe73d7858b40");
			SourceDto sourceDto = new SourceDto();
			sourceDto.setUser_id("65bc933c6129fe73d7858b41");
			sourceDto.setUsername("username");
			metadataInstancesDto.setSource(sourceDto);
			metadataInstancesDto.setMetaType("table");
			metadataInstancesDto.setDatabaseId("222");
			metadataInstancesDto.setComment("test comment");
			results = new ArrayList<>();
			results.add(metadataInstancesDto);
			List<UserDto> userDtos = new ArrayList<>();
			UserDto userDto = new UserDto();
			userDto.setId(new ObjectId("65bc933c6129fe73d7858b41"));
			userDto.setUsername("username");
			userDto.setEmail("email@163.com");
			userDtos.add(userDto);
			when(userService.findAll(any(Query.class))).thenReturn(userDtos);
			List<MetadataInstancesDto> collections = new ArrayList<>();
			MetadataInstancesDto meta = new MetadataInstancesDto();
			meta.setId(new ObjectId("65bc933c6129fe73d7858b41"));
			meta.setOriginalName("originalName");
			collections.add(meta);
			doReturn(collections).when(metadataInstancesService).findAll(any(Query.class));
			metadataInstancesService.afterFindAll(results);
			assertEquals("test comment",results.get(0).getComment());
		}
		@Test
		@DisplayName("test afterFindAll method simple")
		void test2(){
			MetadataInstancesDto metadataInstancesDto = new MetadataInstancesDto();
			metadataInstancesDto.setDatabaseId("65bc933c6129fe73d7858b40");
			metadataInstancesDto.setDatabaseId("222");
			results = new ArrayList<>();
			results.add(metadataInstancesDto);
			List<UserDto> userDtos = new ArrayList<>();
			when(userService.findAll(any(Query.class))).thenReturn(userDtos);
			List<MetadataInstancesDto> collections = new ArrayList<>();
			MetadataInstancesDto meta = new MetadataInstancesDto();
			meta.setId(new ObjectId("65bc933c6129fe73d7858b41"));
			meta.setOriginalName("originalName");
			collections.add(meta);
			doReturn(collections).when(metadataInstancesService).findAll(any(Query.class));
			metadataInstancesService.afterFindAll(results);
			assertEquals("",results.get(0).getComment());
		}
	}
	@Nested
	class AfterFindTest{
		private MetadataInstancesDto metadata;
		@Test
		@DisplayName("test afterFind method when metadata is null")
		void test1(){
			try (MockedStatic<CollectionUtils> mb = Mockito
					.mockStatic(CollectionUtils.class)) {
				metadata = null;
				metadataInstancesService.afterFind(metadata);
				verify(metadataInstancesService,new Times(1)).afterFind(metadata);
				mb.verify(() -> CollectionUtils.isNotEmpty(anyCollection()),new Times(0));
			}
		}
		@Test
		@DisplayName("test afterFind method normal")
		void test2(){
			try (MockedStatic<CollectionUtils> mb = Mockito
					.mockStatic(CollectionUtils.class)) {
				metadata = new MetadataInstancesDto();
				metadata.setFields(new ArrayList<>());
				metadataInstancesService.afterFind(metadata);
				verify(metadataInstancesService,new Times(1)).afterFind(metadata);
				mb.verify(() -> CollectionUtils.isNotEmpty(anyCollection()),new Times(1));
			}
		}
	}
	@Nested
	class AfterFindForListTest{
		private List<MetadataInstancesDto> metadatas;
		private DataSourceService dataSourceService;

		@BeforeEach
		void setUp() {
			metadatas = new ArrayList<>();
			dataSourceService = mock(DataSourceServiceImpl.class);
			ReflectionTestUtils.setField(metadataInstancesService, "dataSourceService", dataSourceService);
		}
		@Test
		@DisplayName("test afterFind for List normal")
		void test1(){
			MetadataInstancesDto metadata = new MetadataInstancesDto();
			List<Field> fields = new ArrayList<>();
			Field field = new Field();
			field.setDeleted(false);
			field.setIsNullable("YES");
			fields.add(field);
			metadata.setFields(fields);
			SourceDto sourceDto = new SourceDto();
			sourceDto.setId(new ObjectId("65bc933c6129fe73d7858b40"));
			metadata.setSource(sourceDto);
			metadata.setMetaType("table");
			metadatas.add(metadata);
			DataSourceConnectionDto connectionDto = new DataSourceConnectionDto();
			connectionDto.setId(new ObjectId("65bc933c6129fe73d7858b41"));
			when(dataSourceService.findById(any(ObjectId.class))).thenReturn(connectionDto);
			metadataInstancesService.afterFind(metadatas);
			assertEquals("65bc933c6129fe73d7858b41",metadatas.get(0).getSource().get_id());
		}
		@Test
		@DisplayName("test afterFind for List when result fields is empty and metaType is job")
		void test2(){
			MetadataInstancesDto metadata = new MetadataInstancesDto();
			List<Field> fields = new ArrayList<>();
			metadata.setFields(fields);
			SourceDto sourceDto = new SourceDto();
			metadata.setSource(sourceDto);
			metadata.setMetaType("job");
			metadatas.add(metadata);
			metadataInstancesService.afterFind(metadatas);
			verify(dataSourceService,new Times(0)).findById(any(ObjectId.class));
		}
		@Test
		@DisplayName("test afterFind for List when result source is null")
		void test3(){
			MetadataInstancesDto metadata = new MetadataInstancesDto();
			List<Field> fields = new ArrayList<>();
			metadata.setFields(fields);
			metadatas.add(metadata);
			metadataInstancesService.afterFind(metadatas);
			verify(dataSourceService,new Times(0)).findById(any(ObjectId.class));
		}
		@Test
		@DisplayName("test afterFind for List when connectionId is null")
		void test4(){
			try (MockedStatic<MongoUtils> mb = Mockito
					.mockStatic(MongoUtils.class)) {
				MetadataInstancesDto metadata = new MetadataInstancesDto();
				List<Field> fields = new ArrayList<>();
				Field field = new Field();
				field.setDeleted(false);
				field.setIsNullable("NO");
				Field field1 = new Field();
				field1.setDeleted(true);
				fields.add(field);
				fields.add(field1);
				metadata.setFields(fields);
				SourceDto sourceDto = new SourceDto();
				sourceDto.setId(null);
				sourceDto.set_id("65bc933c6129fe73d7858b40");
				metadata.setSource(sourceDto);
				metadata.setMetaType("table");
				metadatas.add(metadata);
				DataSourceConnectionDto connectionDto = new DataSourceConnectionDto();
				connectionDto.setId(new ObjectId("65bc933c6129fe73d7858b41"));
				when(dataSourceService.findById(any(ObjectId.class))).thenReturn(connectionDto);
				metadataInstancesService.afterFind(metadatas);
				assertEquals("65bc933c6129fe73d7858b40",metadatas.get(0).getSource().get_id());
				mb.verify(() -> MongoUtils.toObjectId(anyString()),new Times(1));
			}
		}
	}
	@Nested
	class ClassificationsTest{
		private List<ClassificationParam> classificationParamList;
		@BeforeEach
		void beforeEach(){
			classificationParamList = new ArrayList<>();
		}
		@Test
		@DisplayName("test classifications method normal")
		void test1(){
			ClassificationParam classificationParam = new ClassificationParam();
			classificationParam.setId("65bc933c6129fe73d7858b40");
			List<Tag> classifications = new ArrayList<>();
			classifications.add(mock(Tag.class));
			classificationParam.setClassifications(classifications);
			classificationParamList.add(classificationParam);
			MongoTemplate template = mock(MongoTemplate.class);
			when(metadataInstancesRepository.getMongoOperations()).thenReturn(template);
			UpdateResult updateResult = mock(UpdateResult.class);
			when(template.upsert(any(Query.class),any(Update.class),anyString())).thenReturn(updateResult);
			when(updateResult.getModifiedCount()).thenReturn(1L);
			MetadataInstancesDto metadataInstancesDto = mock(MetadataInstancesDto.class);
			doReturn(metadataInstancesDto).when(metadataInstancesService).findById(any(ObjectId.class));
			when(metadataInstancesDto.getMetaType()).thenReturn("database");
			SourceDto sourceDto = mock(SourceDto.class);
			when(metadataInstancesDto.getSource()).thenReturn(sourceDto);
			when(sourceDto.get_id()).thenReturn("65bc933c6129fe73d7858b40");
			Map<String, Object> actual = metadataInstancesService.classifications(classificationParamList);
			assertEquals(1,actual.get("rows"));
		}
		@Test
		@DisplayName("test classifications method when updateResult is null")
		void test2(){
			ClassificationParam classificationParam = new ClassificationParam();
			classificationParam.setId("65bc933c6129fe73d7858b40");
			List<Tag> classifications = new ArrayList<>();
			classifications.add(mock(Tag.class));
			classificationParam.setClassifications(classifications);
			classificationParamList.add(classificationParam);
			MongoTemplate template = mock(MongoTemplate.class);
			when(metadataInstancesRepository.getMongoOperations()).thenReturn(template);
			when(template.upsert(any(Query.class),any(Update.class),anyString())).thenReturn(null);
			Map<String, Object> actual = metadataInstancesService.classifications(classificationParamList);
			assertEquals(0,actual.get("rows"));
			verify(metadataInstancesService,new Times(0)).findById(any(ObjectId.class));
		}
		@Test
		@DisplayName("test classifications method when collectionName is empty")
		void test3(){
			ClassificationParam classificationParam = new ClassificationParam();
			classificationParam.setId("65bc933c6129fe73d7858b40");
			List<Tag> classifications = new ArrayList<>();
			classifications.add(mock(Tag.class));
			classificationParam.setClassifications(classifications);
			classificationParamList.add(classificationParam);
			MongoTemplate template = mock(MongoTemplate.class);
			when(metadataInstancesRepository.getMongoOperations()).thenReturn(template);
			UpdateResult updateResult = mock(UpdateResult.class);
			when(template.upsert(any(Query.class),any(Update.class),anyString())).thenReturn(updateResult);
			when(updateResult.getModifiedCount()).thenReturn(1L);
			MetadataInstancesDto metadataInstancesDto = mock(MetadataInstancesDto.class);
			doReturn(metadataInstancesDto).when(metadataInstancesService).findById(any(ObjectId.class));
			when(metadataInstancesDto.getMetaType()).thenReturn("table");
			SourceDto sourceDto = mock(SourceDto.class);
			when(metadataInstancesDto.getSource()).thenReturn(sourceDto);
			when(sourceDto.get_id()).thenReturn("65bc933c6129fe73d7858b40");
			Map<String, Object> actual = metadataInstancesService.classifications(classificationParamList);
			assertEquals(1,actual.get("rows"));
			verify(template,never()).updateFirst(any(Query.class),any(Update.class),anyString());
		}

	}
	@Nested
	class BeforeUpdateByIdTest{
		private ObjectId id;
		private MetadataInstancesDto data;
		@Test
		@DisplayName("test beforeUpdateById method when id is null")
		void test1(){
			id = null;
			data = mock(MetadataInstancesDto.class);
			metadataInstancesService.beforeUpdateById(id,data);
			verify(metadataInstancesService,never()).findById(any(ObjectId.class));
		}
		@Test
		@DisplayName("test beforeUpdateById method normal")
		void test2(){
			id = new ObjectId("65bc933c6129fe73d7858b40");
			data = new MetadataInstancesDto();
			List<Field> fieldsAfter = new ArrayList<>();
			fieldsAfter.add(mock(Field.class));
			data.setFieldsAfter(fieldsAfter);
			MetadataInstancesDto metadata = new MetadataInstancesDto();
			doReturn(metadata).when(metadataInstancesService).findById(id);
			metadataInstancesService.beforeUpdateById(id,data);
			verify(metadataInstancesService,new Times(1)).findById(any(ObjectId.class));
		}
	}
	@Nested
	class CompareHistoryTest{
		private ObjectId id;
		private int historyVersion;
		@Test
		@DisplayName("test compareHistory method when metadata is null")
		void test1(){
			id = mock(ObjectId.class);
			historyVersion = 1;
			assertThrows(BizException.class,()->metadataInstancesService.compareHistory(id,historyVersion));
		}
		@Test
		@DisplayName("test compareHistory method when histories is empty")
		void test2(){
			id = mock(ObjectId.class);
			historyVersion = 1;
			MetadataInstancesDto metadata = new MetadataInstancesDto();
			doReturn(metadata).when(metadataInstancesService).findById(id);
			assertThrows(BizException.class,()->metadataInstancesService.compareHistory(id,historyVersion));
		}
		@Test
		@DisplayName("test compareHistory method normal")
		void test3(){
			try (MockedStatic<MetadataUtil> mb = Mockito
					.mockStatic(MetadataUtil.class)) {
				id = mock(ObjectId.class);
				historyVersion = 1;
				MetadataInstancesDto metadata = new MetadataInstancesDto();
				List<MetadataInstancesDto> histories = new ArrayList<>();
				MetadataInstancesDto history = new MetadataInstancesDto();
				histories.add(history);
				history.setVersion(1);
				metadata.setHistories(histories);
				doReturn(metadata).when(metadataInstancesService).findById(id);
				metadataInstancesService.compareHistory(id,historyVersion);
				mb.verify(() -> MetadataUtil.compare(any(MetadataInstancesDto.class),any(MetadataInstancesDto.class)),new Times(1));
			}
		}
		@Test
		@DisplayName("test compareHistory method when secondMeta is null")
		void test4(){
			try (MockedStatic<MetadataUtil> mb = Mockito
					.mockStatic(MetadataUtil.class)) {
				id = mock(ObjectId.class);
				historyVersion = 2;
				MetadataInstancesDto metadata = new MetadataInstancesDto();
				List<MetadataInstancesDto> histories = new ArrayList<>();
				MetadataInstancesDto history = new MetadataInstancesDto();
				histories.add(history);
				history.setVersion(1);
				metadata.setHistories(histories);
				doReturn(metadata).when(metadataInstancesService).findById(id);
				assertThrows(BizException.class,()->metadataInstancesService.compareHistory(id,historyVersion));
				mb.verify(() -> MetadataUtil.compare(any(MetadataInstancesDto.class),any(MetadataInstancesDto.class)),new Times(0));
			}
		}

	}
	@Nested
	class TableConnectionTest{
		@Test
		@DisplayName("test tableConnection method normal")
		void test1(){
			String name = "test";
			List<MetadataInstancesDto> metaArr = new ArrayList<>();
			MetadataInstancesDto meta = new MetadataInstancesDto();
			SourceDto sourceDto = new SourceDto();
			sourceDto.setId(mock(ObjectId.class));
			meta.setSource(sourceDto);
			metaArr.add(meta);
			doReturn(metaArr).when(metadataInstancesService).findAllDto(any(Query.class),any(UserDetail.class));
			List<MetadataInstancesDto> actual = metadataInstancesService.tableConnection(name, userDetail);
			assertEquals(metaArr,actual);
		}
		@Test
		@DisplayName("test tableConnection method when source is null")
		void test2(){
			String name = "test";
			List<MetadataInstancesDto> metaArr = new ArrayList<>();
			MetadataInstancesDto meta = new MetadataInstancesDto();
			metaArr.add(meta);
			doReturn(metaArr).when(metadataInstancesService).findAllDto(any(Query.class),any(UserDetail.class));
			List<MetadataInstancesDto> actual = metadataInstancesService.tableConnection(name, userDetail);
			assertEquals(metaArr,actual);
		}
		@Test
		@DisplayName("test tableConnection method when metaArr is null")
		void test3(){
			String name = "test";
			doReturn(new ArrayList<>()).when(metadataInstancesService).findAllDto(any(Query.class),any(UserDetail.class));
			List<MetadataInstancesDto> actual = metadataInstancesService.tableConnection(name, userDetail);
			assertEquals(null,actual);
		}
	}
	@Nested
	class OriginalDataTest{
		private String isTarget;
		private String qualified_name;
		@Test
		void testOriginalDataNormal(){
			metadataInstancesService.originalData(isTarget,qualified_name,userDetail);
			verify(metadataInstancesService).findByQualifiedName(qualified_name,userDetail);
		}
	}
	@Nested
	class FindBySourceIdAndTableNameTest{
		@Test
		void testFindBySourceIdAndTableNameNormal(){
			String sourceId = "111";
			String tableName = "test";
			String taskId = "222";
			metadataInstancesService.findBySourceIdAndTableName(sourceId,tableName,taskId,userDetail);
			verify(metadataInstancesService).findOne(any(Query.class),any(UserDetail.class));
		}
	}
	@Nested
	class FindSourceSchemaBySourceIdTest{
		private String sourceId;
		private List<String> tableNames;
		private String fields;
		@Test
		@DisplayName("test findSourceSchemaBySourceId method when tableName is empty and fields is null")
		void test1(){
			sourceId = "111";
			tableNames = new ArrayList<>();
			fields = null;
			metadataInstancesService.findSourceSchemaBySourceId(sourceId,tableNames,userDetail,fields);
			verify(metadataInstancesService).findAllDto(any(Query.class),any(UserDetail.class));
		}
		@Test
		@DisplayName("test findSourceSchemaBySourceId method normal")
		void test2(){
			sourceId = "111";
			tableNames = new ArrayList<>();
			tableNames.add("test");
			fields = "test_field";
			metadataInstancesService.findSourceSchemaBySourceId(sourceId,tableNames,userDetail,fields);
			verify(metadataInstancesService).findAllDto(any(Query.class),any(UserDetail.class));
		}
	}
	@Nested
	class FindBySourceIdAndTableNameListTest{
		@Test
		void testFindBySourceIdAndTableNameListNormal(){
			String sourceId = "111";
			List<String> tableNames = new ArrayList<>();
			tableNames.add("test");
			String taskId = "222";
			metadataInstancesService.findBySourceIdAndTableNameList(sourceId,tableNames,userDetail,taskId);
			verify(metadataInstancesService).findAllDto(any(Query.class),any(UserDetail.class));
		}
	}
	@Nested
	class FindBySourceIdAndTableNameListNeTaskIdTest{
		@Test
		void testFindBySourceIdAndTableNameListNeTaskIdNormal(){
			String sourceId = "111";
			List<String> tableNames = new ArrayList<>();
			tableNames.add("test");
			metadataInstancesService.findBySourceIdAndTableNameListNeTaskId(sourceId,tableNames,userDetail);
			verify(metadataInstancesService).findAllDto(any(Query.class),any(UserDetail.class));
		}
	}
	@Nested
	class FindEntityBySourceIdAndTableNameListTest{
		@Test
		void testfindEntityBySourceIdAndTableNameListNormal(){
			String sourceId = "111";
			List<String> tableNames = new ArrayList<>();
			tableNames.add("test");
			String taskId = "222";
			metadataInstancesService.findEntityBySourceIdAndTableNameList(sourceId,tableNames,userDetail,taskId);
			verify(metadataInstancesService).findAll(any(Query.class),any(UserDetail.class));
		}
	}
	@Nested
	class FindByQualifiedNameListTest{
		@Test
		void testFindByQualifiedNameListNormal(){
			List<String> qualifiedNames = new ArrayList<>();
			qualifiedNames.add("test");
			String taskId = "111";
			metadataInstancesService.findByQualifiedNameList(qualifiedNames,taskId);
			verify(metadataInstancesService).findAll(any(Query.class));
		}
	}
	@Nested
	class FindByQualifiedNameNotDeleteTest{
		@Test
		void testFindByQualifiedNameNotDeleteNJormal(){
			List<String> qualifiedNames = new ArrayList<>();
			qualifiedNames.add("test");
			String excludeFiled = "field";
			metadataInstancesService.findByQualifiedNameNotDelete(qualifiedNames,userDetail,excludeFiled);
			verify(metadataInstancesService).findAllDto(any(Query.class),any(UserDetail.class));
		}

	}
	@Nested
	class FindDatabaseSchemeNoHistoryTest{
		@Test
		void testFindDatabaseSchemeNoHistoryNormal(){
			List<String> databaseIds = new ArrayList<>();
			databaseIds.add("111");
			metadataInstancesService.findDatabaseSchemeNoHistory(databaseIds,userDetail);
			verify(metadataInstancesService).findAllDto(any(Query.class),any(UserDetail.class));
		}

	}
	@Nested
	class BulkSaveTest{
		private List<MetadataInstancesDto> metadataInstancesDtos;
		private MetadataInstancesDto dataSourceMetadataInstance;
		private DataSourceConnectionDto dataSourceConnectionDto;
		private DAG.Options options;
		private Map<String, MetadataInstancesEntity> existsMetadataInstances;
		@BeforeEach
		void beforeEach(){
			metadataInstancesDtos = new ArrayList<>();
			MetadataInstancesDto metadataInstancesDto = new MetadataInstancesDto();
			metadataInstancesDto.setQualifiedName("test_qualifiedName");
			List<Field> fields = new ArrayList<>();
			Field field = new Field();
			field.setOriginalFieldName("originName");
			fields.add(field);
			metadataInstancesDto.setFields(fields);
			metadataInstancesDto.setOriginalName("table1");
			metadataInstancesDto.setMetaType("table");
			metadataInstancesDtos.add(metadataInstancesDto);
			dataSourceMetadataInstance = new MetadataInstancesDto();
			dataSourceConnectionDto = new DataSourceConnectionDto();
			options = new DAG.Options();
			existsMetadataInstances = new HashMap<>();
			MetadataInstancesEntity entity = new MetadataInstancesEntity();
			List<Field> fields1 = new ArrayList<>();
			Field field1 = new Field();
			entity.setVersion(1);
			field1.setOriginalFieldName("originName");
			field1.setSource("manual");
			field1.setDataType("String");
			field1.setPrecision(1);
			field1.setScale(1);
			fields1.add(field1);
			entity.setFields(fields1);
			existsMetadataInstances.put("test_qualifiedName",entity);
		}
		@Test
		@DisplayName("test bulkSave method when existsMetadataInstances contains qualifiedName and fieldsNameTransform is toUpperCase")
		void test1(){
			options.setRollback("table");
			options.setFieldsNameTransform("toUpperCase");
			options.setRollbackTable("table1");
			BulkOperations bulkOperations = mock(BulkOperations.class);
			when(metadataInstancesRepository.bulkOperations(BulkOperations.BulkMode.UNORDERED)).thenReturn(bulkOperations);
			when(bulkOperations.execute()).thenReturn(mock(BulkWriteResult.class));
			metadataInstancesService.bulkSave(metadataInstancesDtos,dataSourceMetadataInstance,dataSourceConnectionDto,options,userDetail,existsMetadataInstances);
			assertEquals("ORIGINNAME",metadataInstancesDtos.get(0).getFields().get(0).getFieldName());
		}
		@Test
		@DisplayName("test bulkSave method when existsMetadataInstances contains qualifiedName and fieldsNameTransform is toLowerCase")
		void test2(){
			options.setRollback("table");
			options.setFieldsNameTransform("toLowerCase");
			options.setRollbackTable("table1");
			BulkOperations bulkOperations = mock(BulkOperations.class);
			when(metadataInstancesRepository.bulkOperations(BulkOperations.BulkMode.UNORDERED)).thenReturn(bulkOperations);
			when(bulkOperations.execute()).thenReturn(mock(BulkWriteResult.class));
			metadataInstancesService.bulkSave(metadataInstancesDtos,dataSourceMetadataInstance,dataSourceConnectionDto,options,userDetail,existsMetadataInstances);
			assertEquals("originname",metadataInstancesDtos.get(0).getFields().get(0).getFieldName());
		}
		@Test
		@DisplayName("test bulkSave method when existsMetadataInstances contains qualifiedName")
		void test3(){
			options.setRollback("table");
			options.setFieldsNameTransform("testTransform");
			options.setRollbackTable("table1");
			BulkOperations bulkOperations = mock(BulkOperations.class);
			when(metadataInstancesRepository.bulkOperations(BulkOperations.BulkMode.UNORDERED)).thenReturn(bulkOperations);
			when(bulkOperations.execute()).thenReturn(mock(BulkWriteResult.class));
			metadataInstancesService.bulkSave(metadataInstancesDtos,dataSourceMetadataInstance,dataSourceConnectionDto,options,userDetail,existsMetadataInstances);
			assertEquals("originName",metadataInstancesDtos.get(0).getFields().get(0).getFieldName());
			verify(bulkOperations).updateOne(any(Query.class),any(Update.class));
		}
		@Test
		@DisplayName("test bulkSave method when existsMetadataInstances not contains qualifiedName and database type is vika")
		void test4(){
			existsMetadataInstances.clear();
			dataSourceConnectionDto.setId(mock(ObjectId.class));
			dataSourceConnectionDto.setDatabase_type("vika");
			BulkOperations bulkOperations = mock(BulkOperations.class);
			when(metadataInstancesRepository.bulkOperations(BulkOperations.BulkMode.UNORDERED)).thenReturn(bulkOperations);
			when(bulkOperations.execute()).thenReturn(mock(BulkWriteResult.class));
			when(metadataInstancesRepository.buildUpdateSet(any(MetadataInstancesEntity.class),any(UserDetail.class))).thenReturn(mock(Update.class));
			metadataInstancesService.bulkSave(metadataInstancesDtos,dataSourceMetadataInstance,dataSourceConnectionDto,options,userDetail,existsMetadataInstances);
			verify(bulkOperations).upsert(any(Query.class),any(Update.class));
		}
		@Test
		@DisplayName("test bulkSave method when existsMetadataInstances not contains qualifiedName and database type is qingflow")
		void test5(){
			existsMetadataInstances.clear();
			dataSourceConnectionDto.setId(mock(ObjectId.class));
			dataSourceConnectionDto.setDatabase_type("qingflow");
			BulkOperations bulkOperations = mock(BulkOperations.class);
			when(metadataInstancesRepository.bulkOperations(BulkOperations.BulkMode.UNORDERED)).thenReturn(bulkOperations);
			when(bulkOperations.execute()).thenReturn(mock(BulkWriteResult.class));
			when(metadataInstancesRepository.buildUpdateSet(any(MetadataInstancesEntity.class),any(UserDetail.class))).thenReturn(mock(Update.class));
			metadataInstancesService.bulkSave(metadataInstancesDtos,dataSourceMetadataInstance,dataSourceConnectionDto,options,userDetail,existsMetadataInstances);
			verify(bulkOperations).upsert(any(Query.class),any(Update.class));
		}
		@Test
		@DisplayName("test bulkSave method when existsMetadataInstances contains qualifiedName and fieldsNameTransform is blank")
		void test6(){
			options.setRollback("table");
			options.setRollbackTable("table1");
			BulkOperations bulkOperations = mock(BulkOperations.class);
			when(metadataInstancesRepository.bulkOperations(BulkOperations.BulkMode.UNORDERED)).thenReturn(bulkOperations);
			when(bulkOperations.execute()).thenReturn(mock(BulkWriteResult.class));
			metadataInstancesService.bulkSave(metadataInstancesDtos,dataSourceMetadataInstance,dataSourceConnectionDto,options,userDetail,existsMetadataInstances);
			assertEquals("originName",metadataInstancesDtos.get(0).getFields().get(0).getFieldName());
		}
	}
	@Nested
	class BulkSaveTest2{
		private List<MetadataInstancesDto> insertMetaDataDtos;
		private Map<String, MetadataInstancesDto> updateMetaMap;
		private boolean saveHistory;
		private String taskId;
		private String uuid;
		private MetaDataHistoryService metaDataHistoryService;
		@BeforeEach
		void beforeEach(){
			metaDataHistoryService = mock(MetaDataHistoryService.class);
			ReflectionTestUtils.setField(metadataInstancesService,"metaDataHistoryService",metaDataHistoryService);
			BulkOperations bulkOperations = mock(BulkOperations.class);
			when(metadataInstancesRepository.bulkOperations(BulkOperations.BulkMode.UNORDERED)).thenReturn(bulkOperations);
			when(bulkOperations.execute()).thenReturn(mock(BulkWriteResult.class));
		}
		@Test
		@DisplayName("test bulkSave method when insertMetaDataDtos is null and updateMetaMap is not null")
		void test1(){
			uuid = "111";
			saveHistory = false;
			updateMetaMap = new HashMap<>();
			MetadataInstancesDto metadataInstancesDto = new MetadataInstancesDto();
			metadataInstancesDto.setQualifiedName("qualifiedName");
			updateMetaMap.put("test_updateMap",metadataInstancesDto);
			List<MetadataInstancesDto> metadataInstancesDtos = new ArrayList<>();
			MetadataInstancesDto meta = new MetadataInstancesDto();
			meta.setId(mock(ObjectId.class));
			doReturn(metadataInstancesDtos).when(metadataInstancesService).findAllDto(any(Query.class),any(UserDetail.class));
			metadataInstancesService.bulkSave(insertMetaDataDtos,updateMetaMap,userDetail,saveHistory,taskId,uuid);
			verify(metadataInstancesService).deleteAll(any(Query.class),any(UserDetail.class));
		}
		@Test
		@DisplayName("test bulkSave method when insertMetaDataDtos is not empty and saveHistory is true")
		void test2(){
			uuid = "111";
			taskId = "222";
			saveHistory = true;
			insertMetaDataDtos = new ArrayList<>();
			MetadataInstancesDto metadataInstancesDto = new MetadataInstancesDto();
			metadataInstancesDto.setQualifiedName("qualifiedName_222");
			metadataInstancesDto.setLastUpdate(1713846744L);
			metadataInstancesDto.setSource(mock(SourceDto.class));
			insertMetaDataDtos.add(metadataInstancesDto);
			MetadataInstancesDto meta = new MetadataInstancesDto();
			meta.setId(mock(ObjectId.class));
			meta.setTransformUuid("333");
			doReturn(meta).when(metadataInstancesService).findOne(any(Query.class));
			metadataInstancesService.bulkSave(insertMetaDataDtos,updateMetaMap,userDetail,saveHistory,taskId,uuid);
			verify(metadataInstancesService).qualifiedNameLinkLogic(anyList(),any(UserDetail.class),anyString());
		}
		@Test
		@DisplayName("test bulkSave method when write is false")
		void test3(){
			uuid = "111";
			taskId = "222";
			saveHistory = true;
			int actual = metadataInstancesService.bulkSave(insertMetaDataDtos, updateMetaMap, userDetail, saveHistory, taskId, uuid);
			assertEquals(0,actual);
		}
		@Test
		@DisplayName("test bulkSave method when updateMetaMap is not empty and saveHistory is true")
		void test4(){
			uuid = "111";
			taskId = "222";
			saveHistory = true;
			updateMetaMap = new HashMap<>();
			MetadataInstancesDto metadataInstancesDto = new MetadataInstancesDto();
			metadataInstancesDto.setQualifiedName("qualifiedName_222");
			ObjectId id = new ObjectId("65bc933f6129fe73d7858d6f");
			updateMetaMap.put(id.toHexString(),metadataInstancesDto);
			List<MetadataInstancesDto> metadataInstancesDtos = new ArrayList<>();
			MetadataInstancesDto meta = new MetadataInstancesDto();
			meta.setId(id);
			metadataInstancesDtos.add(meta);
			doReturn(metadataInstancesDtos).when(metadataInstancesService).findAllDto(any(Query.class),any(UserDetail.class));
			metadataInstancesService.bulkSave(insertMetaDataDtos,updateMetaMap,userDetail,saveHistory,taskId,uuid);
			verify(metadataInstancesService).qualifiedNameLinkLogic(anyList(),any(UserDetail.class),anyString());
		}
	}
	@Nested
	class BulkUpsetByWhereTest{
		private List<MetadataInstancesDto> metadataInstancesDtos;
		private BulkOperations bulkOperations;
		@BeforeEach
		void beforeEach(){
			metadataInstancesDtos = new ArrayList<>();
			MetadataInstancesDto metadataInstancesDto = new MetadataInstancesDto();
			metadataInstancesDto.setQualifiedName("test_qualifiedName");
			metadataInstancesDtos.add(metadataInstancesDto);
			bulkOperations = mock(BulkOperations.class);
			when(metadataInstancesRepository.bulkOperations(BulkOperations.BulkMode.UNORDERED)).thenReturn(bulkOperations);
			BulkWriteResult execute = mock(BulkWriteResult.class);
			when(bulkOperations.execute()).thenReturn(execute);
			when(execute.getModifiedCount()).thenReturn(1);
			when(execute.getInsertedCount()).thenReturn(1);
		}
		@Test
		@DisplayName("test bulkUpsetByWhere method when num mod 1000 not equals 0")
		void test1(){
			Pair<Integer, Integer> actual = metadataInstancesService.bulkUpsetByWhere(metadataInstancesDtos, userDetail);
			verify(bulkOperations).execute();
			assertEquals(1,actual.getLeft());
			assertEquals(1,actual.getRight());
		}
		@Test
		@DisplayName("test bulkUpsetByWhere method when num mod 1000 equals 0")
		void test2(){
			for (int i=1;i<1000;i++){
				MetadataInstancesDto metadataInstancesDto = new MetadataInstancesDto();
				metadataInstancesDto.setQualifiedName("test_qualifiedName");
				metadataInstancesDtos.add(metadataInstancesDto);
			}
			Pair<Integer, Integer> actual = metadataInstancesService.bulkUpsetByWhere(metadataInstancesDtos, userDetail);
			verify(bulkOperations,new Times(2)).execute();
			assertEquals(2,actual.getLeft());
			assertEquals(2,actual.getRight());
		}
	}
	@Nested
	class TablesTest{
		@Test
		void testTablesNormal(){
			MongoTemplate mongoTemplate = mock(MongoTemplate.class);
			ReflectionTestUtils.setField(metadataInstancesService,"mongoTemplate",mongoTemplate);
			String connectId = "111";
			String sourceType = "mysql";
			List<MetadataInstancesEntity> list = new ArrayList<>();
			MetadataInstancesEntity entity = new MetadataInstancesEntity();
			entity.setOriginalName("originalName");
			list.add(entity);
			when(mongoTemplate.find(any(Query.class),any(Class.class))).thenReturn(list);
			List<String> actual = metadataInstancesService.tables(connectId, sourceType);
			assertEquals("originalName",actual.get(0));
		}
	}
	@Nested
	class TableValuesTest{
		private String connectId;
		private String sourceType;
		private MongoTemplate mongoTemplate;
		@BeforeEach
		void beforeEach(){
			mongoTemplate = mock(MongoTemplate.class);
			ReflectionTestUtils.setField(metadataInstancesService,"mongoTemplate",mongoTemplate);
			connectId = "111";
			sourceType = "mysql";
		}
		@Test
		@DisplayName("test tableValues method when list is empty")
		void test1(){
			when(mongoTemplate.find(any(Query.class),any(Class.class))).thenReturn(new ArrayList());
			List<Map<String, String>> actual = metadataInstancesService.tableValues(connectId, sourceType);
			assertEquals(0,actual.size());
		}
		@Test
		@DisplayName("test tableValues method normal")
		void test2(){
			List<MetadataInstancesEntity> list = new ArrayList();
			MetadataInstancesEntity entity = new MetadataInstancesEntity();
			entity.setOriginalName("originalName");
			entity.setId(new ObjectId("65bc933f6129fe73d7858d6f"));
			list.add(entity);
			when(mongoTemplate.find(any(Query.class),any(Class.class))).thenReturn(list);
			List<Map<String, String>> actual = metadataInstancesService.tableValues(connectId, sourceType);
			assertEquals("originalName",actual.get(0).get("tableName"));
			assertEquals("65bc933f6129fe73d7858d6f",actual.get(0).get("tableId"));
			assertEquals("",actual.get(0).get("tableComment"));
		}
	}
	@Nested
	class PageTablesTest{
		private String connectId;
		private String sourceType;
		private String regex;
		private int skip;
		private int limit;
		private MongoTemplate mongoTemplate;
		@BeforeEach
		void beforeEach(){
			mongoTemplate = mock(MongoTemplate.class);
			ReflectionTestUtils.setField(metadataInstancesService,"mongoTemplate",mongoTemplate);
			connectId = "111";
			sourceType = "mysql";
		}
		@Test
		@DisplayName("test pageTables method when totals greater than 0")
		void test1(){
			regex = "tableName";
			limit = 1;
			when(mongoTemplate.count(any(Query.class),any(Class.class))).thenReturn(1L);
			AggregationResults<Map> aggregate = mock(AggregationResults.class);
			when(mongoTemplate.aggregate(any(Aggregation.class),anyString(),any(Class.class))).thenReturn(aggregate);
			List<Map> list = new ArrayList<>();
			Map map = new HashMap();
			map.put("test","map");
			list.add(map);
			when(aggregate.getMappedResults()).thenReturn(list);
			Page<Map<String, Object>> actual = metadataInstancesService.pageTables(connectId, sourceType, regex, skip, limit);
			assertEquals(map,actual.getItems().get(0));
		}
		@Test
		@DisplayName("test pageTables method when totals equals 0")
		void test2(){
			regex = "tableName";
			limit = 1;
			when(mongoTemplate.count(any(Query.class),any(Class.class))).thenReturn(0L);
			Page<Map<String, Object>> actual = metadataInstancesService.pageTables(connectId, sourceType, regex, skip, limit);
			assertEquals(0,actual.getItems().size());
		}
		@Test
		@DisplayName("test pageTables method when limit equals 0")
		void test3(){
			limit = 0;
			when(mongoTemplate.count(any(Query.class),any(Class.class))).thenReturn(1L);
			AggregationResults<Map> aggregate = mock(AggregationResults.class);
			when(mongoTemplate.aggregate(any(Aggregation.class),anyString(),any(Class.class))).thenReturn(aggregate);
			List<Map> list = new ArrayList<>();
			Map map = new HashMap();
			map.put("test","map");
			list.add(map);
			when(aggregate.getMappedResults()).thenReturn(list);
			Page<Map<String, Object>> actual = metadataInstancesService.pageTables(connectId, sourceType, regex, skip, limit);
			assertEquals(map,actual.getItems().get(0));
		}
	}
	@Nested
	class TableSupportInspectTest{
		@Test
		@DisplayName("test tableSupportInspect method when fields is empty")
		void test1(){
			String connectId = "111";
			String tableName = "tableName";
			MetadataInstancesDto metadataInstancesDtos = new MetadataInstancesDto();
			doReturn(metadataInstancesDtos).when(metadataInstancesService).findOne(any(Query.class));
			TableSupportInspectVo actual = metadataInstancesService.tableSupportInspect(connectId, tableName);
			assertEquals("tableName",actual.getTableName());
			assertEquals(false,actual.getSupportInspect());
		}
		@Test
		@DisplayName("test tableSupportInspect method normal")
		void testTableSupportInspectNormal(){
			String connectId = "111";
			String tableName = "tableName";
			MetadataInstancesDto metadataInstancesDtos = new MetadataInstancesDto();
			List<Field> list = new ArrayList<>();
			Field field = new Field();
			field.setPrimaryKeyPosition(1);
			list.add(field);
			metadataInstancesDtos.setFields(list);
			doReturn(metadataInstancesDtos).when(metadataInstancesService).findOne(any(Query.class));
			TableSupportInspectVo actual = metadataInstancesService.tableSupportInspect(connectId, tableName);
			assertEquals("tableName",actual.getTableName());
			assertEquals(true,actual.getSupportInspect());
		}
	}
	@Nested
	class TablesSupportInspectTest{
		private TablesSupportInspectParam tablesSupportInspectParam;
		@BeforeEach
		void beforeEach(){
			tablesSupportInspectParam = new TablesSupportInspectParam();
			tablesSupportInspectParam.setConnectionId("111");
			List<String> tableNames = new ArrayList<>();
			tableNames.add("tableName");
			tablesSupportInspectParam.setTableNames(tableNames);
		}
		@Test
		@DisplayName("test tablesSupportInspect method when fields is empty")
		void test1(){
			List<MetadataInstancesDto> metadataInstancesDtos = new ArrayList<>();
			MetadataInstancesDto metadataInstancesDto = new MetadataInstancesDto();
			metadataInstancesDto.setOriginalName("originalName");
			metadataInstancesDtos.add(metadataInstancesDto);
			doReturn(metadataInstancesDtos).when(metadataInstancesService).findAll(any(Query.class));
			List<TableSupportInspectVo> actual = metadataInstancesService.tablesSupportInspect(tablesSupportInspectParam);
			assertEquals("originalName",actual.get(0).getTableName());
			assertEquals(false,actual.get(0).getSupportInspect());
		}
		@Test
		@DisplayName("test tablesSupportInspect method normal")
		void testTableSupportInspectNormal(){
			List<MetadataInstancesDto> metadataInstancesDtos = new ArrayList<>();
			MetadataInstancesDto metadataInstancesDto = new MetadataInstancesDto();
			List<Field> list = new ArrayList<>();
			Field field = new Field();
			field.setPrimaryKeyPosition(1);
			list.add(field);
			metadataInstancesDto.setFields(list);
			metadataInstancesDto.setOriginalName("originalName");
			metadataInstancesDtos.add(metadataInstancesDto);
			doReturn(metadataInstancesDtos).when(metadataInstancesService).findAll(any(Query.class));
			List<TableSupportInspectVo> actual = metadataInstancesService.tablesSupportInspect(tablesSupportInspectParam);
			assertEquals("originalName",actual.get(0).getTableName());
			assertEquals(true,actual.get(0).getSupportInspect());
		}
	}
	@Nested
	class GetMetadataTest{
		private String connectionId;
		private String metaType;
		private String tableName;
		private DataSourceServiceImpl dataSourceService;
		@BeforeEach
		void beforeEach(){
			connectionId = "65bc933f6129fe73d7858d6f";
			dataSourceService = mock(DataSourceServiceImpl.class);
			ReflectionTestUtils.setField(metadataInstancesService,"dataSourceService",dataSourceService);
		}
		@Test
		@DisplayName("test getMetadata method when connectionDto is null")
		void test1(){
			when(dataSourceService.findById(toObjectId(connectionId), userDetail)).thenReturn(null);
			Table actual = metadataInstancesService.getMetadata(connectionId, metaType, tableName, userDetail);
			assertEquals(null,actual);
		}
		@Test
		@DisplayName("test getMetadata method when metedata is null")
		void test2(){
			metaType = "table";
			DataSourceConnectionDto dto = new DataSourceConnectionDto();
			dto.setId(mock(ObjectId.class));
			when(dataSourceService.findById(toObjectId(connectionId), userDetail)).thenReturn(dto);
			doReturn(null).when(metadataInstancesService).findOne(any(Query.class));
			Table actual = metadataInstancesService.getMetadata(connectionId, metaType, tableName, userDetail);
			assertEquals(null,actual);
		}
		@Test
		@DisplayName("test getMetadata method normal")
		void test3(){
			metaType = "table";
			DataSourceConnectionDto dto = new DataSourceConnectionDto();
			dto.setId(mock(ObjectId.class));
			when(dataSourceService.findById(toObjectId(connectionId), userDetail)).thenReturn(dto);
			MetadataInstancesDto meta = new MetadataInstancesDto();
			meta.setId(mock(ObjectId.class));
			meta.setOriginalName("origin");
			doReturn(meta).when(metadataInstancesService).findOne(any(Query.class));
			Table actual = metadataInstancesService.getMetadata(connectionId, metaType, tableName, userDetail);
			assertEquals("origin",actual.getTableName());
		}
	}
	@Nested
	class GetMetadataV2Test{
		private String connectionId;
		private String metaType;
		private String tableName;
		private DataSourceServiceImpl dataSourceService;
		private DataSourceDefinitionServiceImpl dataSourceDefinitionService;
		@BeforeEach
		void beforeEach(){
			connectionId = "65bc933f6129fe73d7858d6f";
			dataSourceService = mock(DataSourceServiceImpl.class);
			dataSourceDefinitionService = mock(DataSourceDefinitionServiceImpl.class);
			ReflectionTestUtils.setField(metadataInstancesService,"dataSourceService",dataSourceService);
			ReflectionTestUtils.setField(metadataInstancesService,"dataSourceDefinitionService",dataSourceDefinitionService);
		}
		@Test
		@DisplayName("test getMetadataV2 method when connectionDto is null")
		void test1(){
			when(dataSourceService.findById(toObjectId(connectionId), userDetail)).thenReturn(null);
			TapTable actual = metadataInstancesService.getMetadataV2(connectionId, metaType, tableName, userDetail);
			assertEquals(null,actual);
		}
		@Test
		@DisplayName("test getMetadataV2 method when metedata is null")
		void test2(){
			metaType = "table";
			DataSourceConnectionDto dto = new DataSourceConnectionDto();
			dto.setId(mock(ObjectId.class));
			dto.setDatabase_type("mysql");
			when(dataSourceService.findById(toObjectId(connectionId), userDetail)).thenReturn(dto);
			when(dataSourceDefinitionService.getByDataSourceType(dto.getDatabase_type(),userDetail)).thenReturn(mock(DataSourceDefinitionDto.class));
			doReturn(null).when(metadataInstancesService).findOne(any(Query.class));
			TapTable actual = metadataInstancesService.getMetadataV2(connectionId, metaType, tableName, userDetail);
			assertEquals(null,actual);
		}
//		@Test
		@DisplayName("test getMetadataV2 method normal")
		void test3(){
			metaType = "table";
			DataSourceConnectionDto dto = new DataSourceConnectionDto();
			dto.setId(mock(ObjectId.class));
			when(dataSourceService.findById(toObjectId(connectionId), userDetail)).thenReturn(dto);
			when(dataSourceDefinitionService.getByDataSourceType(dto.getDatabase_type(),userDetail)).thenReturn(mock(DataSourceDefinitionDto.class));
			MetadataInstancesDto meta = new MetadataInstancesDto();
			meta.setId(mock(ObjectId.class));
			meta.setOriginalName("origin");
			doReturn(meta).when(metadataInstancesService).findOne(any(Query.class));
			TapTable actual = metadataInstancesService.getMetadataV2(connectionId, metaType, tableName, userDetail);
			assertEquals("origin",actual.getName());
		}
	}
	@Nested
	class FindOldByNodeIdTest{
		private Filter filter;
		@Test
		@DisplayName("test findOldByNodeId method when where is null")
		void test1(){
			filter = new Filter();
			filter.setWhere(null);
			List<Table> actual = metadataInstancesService.findOldByNodeId(filter, userDetail);
			assertEquals(null,actual);
		}
		@Test
		@DisplayName("test findOldByNodeId method when metadatas is empty")
		void test2(){
			filter = new Filter();
			Where where = new Where();
			where.put("nodeId","111");
			filter.setWhere(where);
			doReturn(null).when(metadataInstancesService).findByNodeId("111",null,userDetail,null);
			List<Table> actual = metadataInstancesService.findOldByNodeId(filter, userDetail);
			assertEquals(0,actual.size());
		}
		@Test
		@DisplayName("test findOldByNodeId method normal")
		void test3(){
			filter = new Filter();
			Where where = new Where();
			where.put("nodeId","111");
			filter.setWhere(where);
			List<MetadataInstancesDto> metadatas = new ArrayList<>();
			MetadataInstancesDto dto = new MetadataInstancesDto();
			dto.setId(mock(ObjectId.class));
			dto.setOriginalName("origin");
			metadatas.add(dto);
			doReturn(metadatas).when(metadataInstancesService).findByNodeId("111",null,userDetail,null);
			List<Table> actual = metadataInstancesService.findOldByNodeId(filter, userDetail);
			assertEquals("origin",actual.get(0).getTableName());
		}
	}
	@Nested
	class FindTableMapByNodeIdTest{
		private Filter filter;
		private TaskService taskService;
		private UserService userService;
		@BeforeEach
		void beforeEach(){
			filter = new Filter();
			taskService = mock(TaskService.class);
			userService = mock(UserService.class);
			ReflectionTestUtils.setField(metadataInstancesService,"taskService",taskService);
			ReflectionTestUtils.setField(metadataInstancesService,"userService",userService);
		}
		@Test
		@DisplayName("test findTableMapByNodeId method when where is null")
		void test1(){
			filter.setWhere(null);
			Map<String, String> actual = metadataInstancesService.findTableMapByNodeId(filter);
			assertEquals(null,actual);
		}
		@Test
		@DisplayName("test findTableMapByNodeId method normal")
		void test2(){
			Where where = new Where();
			where.put("nodeId","111");
			filter.setWhere(where);
			TaskDto taskDto = new TaskDto();
			taskDto.setUserId("65bc933f6129fe73d7858d6f");
			when(taskService.findOne(any(Query.class))).thenReturn(taskDto);
			Map<String, String> actual = metadataInstancesService.findTableMapByNodeId(filter);
			assertEquals(0,actual.size());
		}
	}
	@Nested
	class FindKVByNodeTest{
		private String nodeId;
		private TaskService taskService;
		private UserService userService;
		@BeforeEach
		void beforeEach(){
			nodeId = "111";
			taskService = mock(TaskService.class);
			userService = mock(UserService.class);
			ReflectionTestUtils.setField(metadataInstancesService,"taskService",taskService);
			ReflectionTestUtils.setField(metadataInstancesService,"userService",userService);
		}
		@Test
		void testFindKVByNodeNormal(){
			TaskDto taskDto = new TaskDto();
			taskDto.setUserId("65bc933f6129fe73d7858d6f");
			DAG dag = mock(DAG.class);
			taskDto.setDag(dag);
			taskDto.setId(mock(ObjectId.class));
			when(dag.getNode("111")).thenReturn(mock(Node.class));
			when(taskService.findOne(any(Query.class))).thenReturn(taskDto);
			when(userService.loadUserById(any(ObjectId.class))).thenReturn(mock(UserDetail.class));
			Map<String, String> actual = metadataInstancesService.findKVByNode(nodeId);
			assertEquals(0,actual.size());
		}
	}
	@Nested
	class GetNodeMappingTest{
		TaskDto taskDto;
		Map<String, String> kv;
		Node node;
		@Test
		@DisplayName("test getNodeMapping method when node is processorNode")
		void test1(){
			kv = new HashMap<>();
			node = new MergeTableNode();
			node.setId("111");
			taskDto = new TaskDto();
			taskDto.setId(mock(ObjectId.class));
			DAG dag = mock(DAG.class);
			taskDto.setDag(dag);
			List<Node> predecessors = new ArrayList<>();
			LogCollectorNode logCollectorNode = new LogCollectorNode();
			Map<String, LogCollecotrConnConfig> connConfigs = new HashMap<>();
			connConfigs.put("test",mock(LogCollecotrConnConfig.class));
			logCollectorNode.setId("222");
			logCollectorNode.setLogCollectorConnConfigs(connConfigs);
			predecessors.add(logCollectorNode);
			when(dag.predecessors("111")).thenReturn(predecessors);
			List<MetadataInstancesDto> metadatas = new ArrayList<>();
			metadatas.add(mock(MetadataInstancesDto.class));
			doReturn(metadatas).when(metadataInstancesService).findByNodeId(anyString(),anyList(),any(UserDetail.class),any(TaskDto.class));
			assertThrows(RuntimeException.class,()->metadataInstancesService.getNodeMapping(userDetail,taskDto,kv,node));
		}
	}
	@Nested
	class FindHeartbeatQualifiedNameByNodeIdTest{
		private Filter filter;
		private TaskService taskService;
		private DataSourceServiceImpl dataSourceService;
		@BeforeEach
		void beforeEach(){
			filter = new Filter();
			taskService = mock(TaskService.class);
			dataSourceService = mock(DataSourceServiceImpl.class);
			ReflectionTestUtils.setField(metadataInstancesService,"taskService",taskService);
			ReflectionTestUtils.setField(metadataInstancesService,"dataSourceService",dataSourceService);
		}
		@Test
		@DisplayName("test findHeartbeatQualifiedNameByNodeId method when where is null")
		void test1(){
			filter.setWhere(null);
			String actual = metadataInstancesService.findHeartbeatQualifiedNameByNodeId(filter, userDetail);
			assertEquals(null,actual);
		}
		@Test
		@DisplayName("test findHeartbeatQualifiedNameByNodeId method when nodeId is null")
		void test2(){
			filter = new Filter();
			Where where = new Where();
			where.put("nodeId",null);
			filter.setWhere(where);
			String actual = metadataInstancesService.findHeartbeatQualifiedNameByNodeId(filter, userDetail);
			assertEquals(null,actual);
		}
		@Test
		@DisplayName("test findHeartbeatQualifiedNameByNodeId method when where is null")
		void test3(){
			filter = new Filter();
			Where where = new Where();
			where.put("nodeId","111");
			filter.setWhere(where);
			TaskDto task = new TaskDto();
			DAG dag = mock(DAG.class);
			List<Node> targets = new ArrayList<>();
			DataNode node = new TableNode();
			node.setConnectionId("65bc933c6129fe73d7858b41");
			DataSourceConnectionDto dataSource = mock(DataSourceConnectionDto.class);
			when(dataSourceService.findById(any(ObjectId.class))).thenReturn(dataSource);
			targets.add(node);
			when(dag.getTargets()).thenReturn(targets);
			task.setDag(dag);
			task.setId(new ObjectId("65bc933c6129fe73d7858b40"));
			when(taskService.findOne(any(Query.class),any(UserDetail.class))).thenReturn(task);
			String actual = metadataInstancesService.findHeartbeatQualifiedNameByNodeId(filter, userDetail);
			assertNotEquals(null,actual);
		}
	}
	@Nested
	class GetQualifiedNameByNodeIdTest{
		private Node node;
		private DataSourceConnectionDto dataSource;
		private DataSourceDefinitionDto definitionDto;
		private String taskId;
		private DataSourceServiceImpl dataSourceService;
		private DataSourceDefinitionServiceImpl dataSourceDefinitionService;
		@BeforeEach
		void beforeEach(){
			dataSourceService = mock(DataSourceServiceImpl.class);
			dataSourceDefinitionService = mock(DataSourceDefinitionServiceImpl.class);
			ReflectionTestUtils.setField(metadataInstancesService,"dataSourceService",dataSourceService);
			ReflectionTestUtils.setField(metadataInstancesService,"dataSourceDefinitionService",dataSourceDefinitionService);
		}
		@Test
		@DisplayName("test getQualifiedNameByNodeId method when node is null")
		void test1(){
			node = null;
			String actual = metadataInstancesService.getQualifiedNameByNodeId(node, userDetail, dataSource, definitionDto, taskId);
			assertEquals(null,actual);
		}
		@Test
		@DisplayName("test getQualifiedNameByNodeId method for TableNode")
		void test2(){
			node = new TableNode();
			((TableNode)node).setConnectionId("65bc933c6129fe73d7858b40");
			((TableNode)node).setTableName("tableName");
			DataSourceConnectionDto connectionDto = new DataSourceConnectionDto();
			connectionDto.setId(mock(ObjectId.class));
			connectionDto.setDatabase_type("mongodb");
			connectionDto.setDatabase_uri("mongodb://localhost:27017");
			when(dataSourceService.findById(any(ObjectId.class))).thenReturn(connectionDto);
			DataSourceDefinitionDto dataSourceDefinitionDto = new DataSourceDefinitionDto();
			when(dataSourceDefinitionService.getByDataSourceType(anyString(),any(UserDetail.class))).thenReturn(dataSourceDefinitionDto);
			String actual = metadataInstancesService.getQualifiedNameByNodeId(node, userDetail, dataSource, definitionDto, taskId);
			assertNotEquals(null,actual);
		}
		@Test
		@DisplayName("test getQualifiedNameByNodeId method for ProcessorNode")
		void test3(){
			node = new MergeTableNode();
			String actual = metadataInstancesService.getQualifiedNameByNodeId(node, userDetail, dataSource, definitionDto, taskId);
			assertNotEquals(null,actual);
		}
		@Test
		@DisplayName("test getQualifiedNameByNodeId method for node")
		void test4(){
			node = new VirtualTargetNode();
			String actual = metadataInstancesService.getQualifiedNameByNodeId(node, userDetail, dataSource, definitionDto, taskId);
			assertEquals(null,actual);
		}
	}
	@Nested
	class FindDatabaseNodeQualifiedNameTest{
		private String nodeId;
		private TaskDto taskDto;
		private DataSourceConnectionDto dataSource;
		private DataSourceDefinitionDto definitionDto;
		private List<String> includes;
		private TaskService taskService;
		private DataSourceServiceImpl dataSourceService;
		private DataSourceDefinitionServiceImpl dataSourceDefinitionService;
		@BeforeEach
		void beforeEach(){
			taskService = mock(TaskService.class);
			dataSourceService = mock(DataSourceServiceImpl.class);
			dataSourceDefinitionService = mock(DataSourceDefinitionServiceImpl.class);
			ReflectionTestUtils.setField(metadataInstancesService,"taskService",taskService);
			ReflectionTestUtils.setField(metadataInstancesService,"dataSourceService",dataSourceService);
			ReflectionTestUtils.setField(metadataInstancesService,"dataSourceDefinitionService",dataSourceDefinitionService);
		}
		@Test
		@DisplayName("test findDatabaseNodeQualifiedName method when includes is not empty")
		void test1(){
			nodeId = "111";
			includes = new ArrayList<>();
			includes.add("tableName");
			TaskDto task = new TaskDto();
			DAG dag = mock(DAG.class);
			task.setDag(dag);
			task.setId(mock(ObjectId.class));
			when(taskService.findOne(any(Query.class),any(UserDetail.class))).thenReturn(task);
			Node node = new TableNode();
			((TableNode)node).setConnectionId("65bc933c6129fe73d7858b40");
			List<String> tableNames = new ArrayList<>();
			tableNames.add("tableName");
			when(dag.getNode(nodeId)).thenReturn(node);
			DataSourceConnectionDto connectionDto = new DataSourceConnectionDto();
			connectionDto.setId(mock(ObjectId.class));
			connectionDto.setDatabase_type("mongodb");
			connectionDto.setDatabase_uri("mongodb://localhost:27017");
			when(dataSourceService.findById(any(ObjectId.class))).thenReturn(connectionDto);
			DataSourceDefinitionDto dataSourceDefinitionDto = new DataSourceDefinitionDto();
			when(dataSourceDefinitionService.getByDataSourceType(anyString(),any(UserDetail.class))).thenReturn(dataSourceDefinitionDto);
			List<String> actual = metadataInstancesService.findDatabaseNodeQualifiedName(nodeId, userDetail, taskDto, dataSource, definitionDto, includes);
			assertNotEquals(null,actual.get(0));
		}
		@Test
		@DisplayName("test findDatabaseNodeQualifiedName method sources or targets not contains tableNode")
		void test2(){
			nodeId = "111";
			includes = new ArrayList<>();
			dataSource = new DataSourceConnectionDto();
			definitionDto =new DataSourceDefinitionDto();
			TaskDto task = new TaskDto();
			DAG dag = mock(DAG.class);
			task.setDag(dag);
			task.setId(mock(ObjectId.class));
			when(taskService.findOne(any(Query.class),any(UserDetail.class))).thenReturn(task);
			Node node = new DatabaseNode();
			when(dag.getNode(nodeId)).thenReturn(node);
			assertThrows(BizException.class,()->metadataInstancesService.findDatabaseNodeQualifiedName(nodeId,userDetail,taskDto,dataSource,definitionDto,includes));
		}
		@Test
		@DisplayName("test findDatabaseNodeQualifiedName method sources contains tableNode")
		void test3(){
			nodeId = "111";
			includes = new ArrayList<>();
			dataSource = new DataSourceConnectionDto();
			dataSource.setId(mock(ObjectId.class));
			definitionDto =new DataSourceDefinitionDto();
			TaskDto task = new TaskDto();
			DAG dag = mock(DAG.class);
			task.setDag(dag);
			task.setId(mock(ObjectId.class));
			when(taskService.findOne(any(Query.class),any(UserDetail.class))).thenReturn(task);
			Node node = new DatabaseNode();
			List<String> tableNames = new ArrayList<>();
			tableNames.add("table1");
			((DatabaseNode)node).setTableNames(tableNames);
			when(dag.getNode(nodeId)).thenReturn(node);
			List<Node> source = new ArrayList<>();
			source.add(node);
			when(dag.getSources()).thenReturn(source);
			List<String> actual = metadataInstancesService.findDatabaseNodeQualifiedName(nodeId,userDetail,taskDto,dataSource,definitionDto,includes);
			assertNotEquals(null,actual.get(0));
		}
		@Test
		@DisplayName("test findDatabaseNodeQualifiedName method targets not contains tableNode")
		void test4(){
			nodeId = "111";
			includes = new ArrayList<>();
			dataSource = new DataSourceConnectionDto();
			dataSource.setId(mock(ObjectId.class));
			definitionDto =new DataSourceDefinitionDto();
			TaskDto task = new TaskDto();
			DAG dag = mock(DAG.class);
			task.setDag(dag);
			task.setId(mock(ObjectId.class));
			when(taskService.findOne(any(Query.class),any(UserDetail.class))).thenReturn(task);
			Node node = new DatabaseNode();
			when(dag.getNode(nodeId)).thenReturn(node);
			List<SyncObjects> list = new ArrayList<>();
			SyncObjects syncObjects = new SyncObjects();
			List<String> tableNames = new ArrayList<>();
			tableNames.add("table1");
			syncObjects.setObjectNames(tableNames);
			list.add(syncObjects);
			((DatabaseNode)node).setSyncObjects(list);
			List<Node> target = new ArrayList<>();
			target.add(node);
			when(dag.getTargets()).thenReturn(target);
			List<String> actual = metadataInstancesService.findDatabaseNodeQualifiedName(nodeId,userDetail,taskDto,dataSource,definitionDto,includes);
			assertNotEquals(null,actual.get(0));
		}
	}
	@Nested
	class FindByNodeIdTest{
		@Test
		void testFindByNodeIdWithFieldNormal(){
			String nodeId = "111";
			metadataInstancesService.findByNodeId(nodeId,userDetail);
			verify(metadataInstancesService).findAllDto(any(Query.class),any(UserDetail.class));
		}
	}
	@Nested
	class FindByNodeIdWithFieldsTest{
		@Test
		void testFindByNodeIdWithFieldNormal(){
			String nodeId = "111";
			String taskId = "222";
			String fields = "test";
			metadataInstancesService.findByNodeId(nodeId,userDetail,taskId,fields);
			verify(metadataInstancesService).findAllDto(any(Query.class),any(UserDetail.class));
		}
	}
	@Nested
	class FindByTaskIdTest{
		@Test
		void testFindByTaskIdNormal(){
			String taskId = "111";
			metadataInstancesService.findByTaskId(taskId,userDetail);
			verify(metadataInstancesService).findAllDto(any(Query.class),any(UserDetail.class));
		}
	}
	@Nested
	class FindByNodeIdWithTaskDtoTest{
		@Test
		void testFindByNodeIdWithTaskDtoNormal(){
			String nodeId = "111";
			List<String> fields = new ArrayList<>();
			TaskDto taskDto = new TaskDto();
			doReturn(mock(Page.class)).when(metadataInstancesService).findByNodeId(nodeId, fields, userDetail, taskDto, null, null, 1, 0);
			metadataInstancesService.findByNodeId(nodeId,fields,userDetail,taskDto);
			verify(metadataInstancesService).findByNodeId(nodeId, fields, userDetail, taskDto, null, null, 1, 0);
		}
	}
	@Nested
	class FindByNodeIdsTest{
		@Test
		void testFindByNodeIdsNormal(){
			List<String> nodeIds = new ArrayList<>();
			String nodeId = "111";
			nodeIds.add(nodeId);
			List<String> fields = new ArrayList<>();
			TaskDto taskDto = new TaskDto();
			Page<MetadataInstancesDto> page = new Page<>();
			List<MetadataInstancesDto> items = new ArrayList<>();
			MetadataInstancesDto metadataInstancesDto = new MetadataInstancesDto();
			items.add(metadataInstancesDto);
			page.setItems(items);
			doReturn(page).when(metadataInstancesService).findByNodeId(nodeId, fields, userDetail, taskDto, null, null, 1, 0);
			Map<String, List<MetadataInstancesDto>> actual = metadataInstancesService.findByNodeIds(nodeIds, fields, userDetail, taskDto);
			assertEquals(items,actual.get(nodeId));
		}
	}
	@Nested
	class findByNodeIdWithTableFilterTest{
		String nodeId;
		List<String> fields;
		TaskDto taskDto;
		String tableFilter;
		String filterType;
		int page;
		int pageSize;
		private TaskService taskService;
		@BeforeEach
		void beforeEach(){
			taskService = mock(TaskService.class);
			ReflectionTestUtils.setField(metadataInstancesService,"taskService",taskService);
		}
		@Test
		@DisplayName("test findByNodeId method when taskDto is null")
		void test1(){
			when(taskService.findOne(any(Query.class),any(UserDetail.class))).thenReturn(null);
			assertThrows(BizException.class,()->metadataInstancesService.findByNodeId(nodeId,fields,userDetail,taskDto,tableFilter,filterType,page,pageSize));
		}
//		@Test
		@DisplayName("test findByNodeId method for MigrateProcessorNode")
		void test2(){
			nodeId = "111";
			page = 1;
			pageSize = 10;
			tableFilter = ".*";
			taskDto = new TaskDto();
			taskDto.setId(mock(ObjectId.class));
			DAG dag = mock(DAG.class);
			taskDto.setDag(dag);
			Node node = new MigrateFieldRenameProcessorNode();
			when(dag.getNode(nodeId)).thenReturn(node);
			List<MetadataInstancesDto> all = new ArrayList<>();
			all.add(mock(MetadataInstancesDto.class));
			doReturn(all).when(metadataInstancesService).findAll(any(Query.class));
			metadataInstancesService.findByNodeId(nodeId,fields,userDetail,taskDto,tableFilter,filterType,page,pageSize);
		}
	}
	@Nested
	class SearchTest{
		String type;
		String keyword;
		String lastId;
		Integer pageSize;
		@Test
		@DisplayName("test search method for table")
		void test1(){
			type = "table";
			lastId = "65bc933c6129fe73d7858b40";
			keyword = "test";
			pageSize = 10;
			List<MetadataInstancesDto> metadatas = new ArrayList<>();
			MetadataInstancesDto dto = new MetadataInstancesDto();
			dto.setId(mock(ObjectId.class));
			dto.setName("name");
			dto.setOriginalName("originalName");
			metadatas.add(dto);
			doReturn(metadatas).when(metadataInstancesService).findAllDto(any(Query.class),any(UserDetail.class));
			List<Map<String, Object>> actual = metadataInstancesService.search(type, keyword, lastId, pageSize, userDetail);
			assertEquals("name",((HashMap)actual.get(0).get("table")).get("name"));
			assertEquals("originalName",((HashMap)actual.get(0).get("table")).get("original_name"));
		}
		@Test
		@DisplayName("test search method for column")
		void test2(){
			type = "column";
			keyword = "comment";
			pageSize = 10;
			List<MetadataInstancesDto> metadatas = new ArrayList<>();
			MetadataInstancesDto dto = new MetadataInstancesDto();
			dto.setId(mock(ObjectId.class));
			dto.setName("name");
			dto.setOriginalName("originalName");
			List<Field> fields = new ArrayList<>();
			Field field = new Field();
			field.setFieldName("fieldName");
			field.setOriginalFieldName("origin_fieldName");
			field.setAliasName("aliasName");
			field.setComment("comment");
			field.setJavaType("javaType");
			fields.add(field);
			dto.setFields(fields);
			metadatas.add(dto);
			doReturn(metadatas).when(metadataInstancesService).findAllDto(any(Query.class),any(UserDetail.class));
			List<Map<String, Object>> actual = metadataInstancesService.search(type, keyword, lastId, pageSize, userDetail);
			assertEquals("name",((HashMap)actual.get(0).get("table")).get("name"));
			assertEquals("originalName",((HashMap)actual.get(0).get("table")).get("original_name"));
			assertEquals("fieldName",((HashMap)((List)actual.get(0).get("columns")).get(0)).get("field_name"));
			assertEquals("origin_fieldName",((HashMap)((List)actual.get(0).get("columns")).get(0)).get("original_field_name"));
			assertEquals("comment",((HashMap)((List)actual.get(0).get("columns")).get(0)).get("comment"));
			assertEquals("javaType",((HashMap)((List)actual.get(0).get("columns")).get(0)).get("type"));
		}
	}
	@Nested
	class TableSearchTest{
		@Test
		@DisplayName("test checkTableNames method when metaData is null")
		void test1(){
			String connectionId = "111";
			List<String> names = new ArrayList<>();
			MetaTableCheckVo actual = metadataInstancesService.checkTableNames(connectionId, names, userDetail);
			assertEquals(null,actual);
		}
		@Test
		@DisplayName("test checkTableNames method when collect is null")
		void test2(){
			String connectionId = "111";
			List<String> names = new ArrayList<>();
			names.add("name");
			names.add("errorTable");
			List<MetadataInstancesDto> metaData = new ArrayList<>();
			MetadataInstancesDto dto = new MetadataInstancesDto();
			dto.setName("name");
			metaData.add(dto);
			doReturn(metaData).when(metadataInstancesService).findAllDto(any(Query.class),any(UserDetail.class));
			MetaTableCheckVo actual = metadataInstancesService.checkTableNames(connectionId, names, userDetail);
			assertEquals(1,actual.getExitsTables().size());
			assertEquals(1,actual.getErrorTables().size());
		}
	}
	@Nested
	class CheckTableNamesTest{

	}
	@Nested
	class FindTablesByIdTest{

	}
	@Nested
	class BatchImportTest{

	}
	@Nested
	class GetTapTableTest{

	}
	@Nested
	class GetTapTableWithNodeTest{

	}
	@Nested
	class GetMergeNodeParentFieldTest{

	}
	@Nested
	class GetParentNodeTest{

	}
	@Nested
	class LinkLogicTest{
		@Test
		void test1(){

		}
	}
	@Nested
	class DeleteTaskMetadataTest{
		@Test
		void testDeleteTaskMetadataNormal(){
			String taskId = "111";
			metadataInstancesService.deleteTaskMetadata(taskId,userDetail);
			verify(metadataInstancesService).deleteAll(any(Query.class),any(UserDetail.class));
		}
	}
	@Nested
	class DataType2TapTypeTest{
		private DataSourceDefinitionServiceImpl dataSourceDefinitionService;
		@BeforeEach
		void beforeEach(){
			dataSourceDefinitionService = mock(DataSourceDefinitionServiceImpl.class);
			ReflectionTestUtils.setField(metadataInstancesService,"dataSourceDefinitionService",dataSourceDefinitionService);
		}
		@Test
		void testDataType2TapTypeNormal(){
			try (MockedStatic<DefaultExpressionMatchingMap> mb = Mockito
					.mockStatic(DefaultExpressionMatchingMap.class)) {
				mb.when(() -> DefaultExpressionMatchingMap.map(anyString())).thenReturn(mock(DefaultExpressionMatchingMap.class));
				DataType2TapTypeDto dto = new DataType2TapTypeDto();
				dto.setDatabaseType("databaseType");
				Set<String> dataTypes = new HashSet<>();
				dataTypes.add("dataType");
				dto.setDataTypes(dataTypes);
				DataSourceDefinitionDto definitionDto = new DataSourceDefinitionDto();
				definitionDto.setExpression("expression");
				when(dataSourceDefinitionService.getByDataSourceType(anyString(), any(UserDetail.class))).thenReturn(definitionDto);
				Map<String, TapType> actual = metadataInstancesService.dataType2TapType(dto, userDetail);
				assertEquals(1,actual.size());
			}
		}
	}
	@Nested
	class CheckTableExistTest{
		@Test
		void testCheckTableExistNormal(){
			String connectionId = "111";
			String tableName = "table";
			doReturn(1L).when(metadataInstancesService).count(any(Query.class),any(UserDetail.class));
			boolean actual = metadataInstancesService.checkTableExist(connectionId, tableName, userDetail);
			assertEquals(true,actual);
		}
	}
	@Nested
	class CountUpdateExNumTest{
		@Test
		void testDeleteLogicModelNormal(){
			String nodeId = "111";
			metadataInstancesService.countUpdateExNum(nodeId);
			verify(metadataInstancesService).count(any(Query.class));
		}
	}
	@Nested
	class CountTransformExNumTest{
		@Test
		void testCountTransformExNumNormal(){
			String nodeId = "111";
			metadataInstancesService.countTransformExNum(nodeId);
			verify(metadataInstancesService).count(any(Query.class));
		}
	}
	@Nested
	class CountTotalNumTest{
		@Test
		void testCountTotalNumNormal(){
			String nodeId = "111";
			metadataInstancesService.countTotalNum(nodeId);
			verify(metadataInstancesService).count(any(Query.class));
		}
	}
	@Nested
	class DeleteLogicModelTest{
		@Test
		void testDeleteLogicModelNormal(){
			String taskId = "111";
			String nodeId = "222";
			metadataInstancesService.deleteLogicModel(taskId,nodeId);
			verify(metadataInstancesService).deleteAll(any(Query.class));
		}
	}
	@Nested
	class UpdateTableDescTest{
		private MetadataInstancesDto metadataInstances;
		@Test
		@DisplayName("test updateTableDesc method when metadataInstances id is empty")
		void test1(){
			metadataInstances = new MetadataInstancesDto();
			assertThrows(BizException.class,()->metadataInstancesService.updateTableDesc(metadataInstances,userDetail));
		}
		@Test
		@DisplayName("test updateTableDesc method normal")
		void test2(){
			metadataInstances = new MetadataInstancesDto();
			metadataInstances.setId(new ObjectId());
			metadataInstancesService.updateTableDesc(metadataInstances,userDetail);
			verify(metadataInstancesService).update(any(Query.class),any(Update.class),any(UserDetail.class));
		}
	}
	@Nested
	class UpdateTableFieldDescTest{
		private String id;
		private DiscoveryFieldDto discoveryFieldDto;
		@Test
		@DisplayName("test updateTableFieldDesc method when id is empty")
		void test1(){
			id = "";
			assertThrows(BizException.class,()->metadataInstancesService.updateTableFieldDesc(id,discoveryFieldDto,userDetail));
		}
		@Test
		@DisplayName("test updateTableFieldDesc method normal")
		void test2(){
			id = "65bc933c6129fe73d7858b40";
			discoveryFieldDto = new DiscoveryFieldDto();
			discoveryFieldDto.setId("222");
			metadataInstancesService.updateTableFieldDesc(id,discoveryFieldDto,userDetail);
			verify(metadataInstancesService).update(any(Query.class),any(Update.class),any(UserDetail.class));
		}
	}
	@Nested
	class ImportEntityTest{
		@Test
		void testImportEntityNormal(){
			MetadataInstancesDto metadataInstancesDto = new MetadataInstancesDto();
			metadataInstancesDto.setQualifiedName("qualifiedName");
			metadataInstancesService.importEntity(metadataInstancesDto,userDetail);
			verify(metadataInstancesService).upsert(any(Query.class),any(MetadataInstancesDto.class),any(UserDetail.class));
		}
	}
	@Nested
	class UpdateFieldCustomDescTest{
		String qualifiedName;
		Map<String, String> fieldCustomDescMap;
		@Test
		void testUpdateFieldCustomDescNormal(){
			fieldCustomDescMap = new HashMap<>();
			fieldCustomDescMap.put("fieldName","test_fieldName");
			MetadataInstancesDto dto = new MetadataInstancesDto();
			List<Field> fields = new ArrayList<>();
			Field field = new Field();
			field.setFieldName("fieldName");
			fields.add(field);
			dto.setFields(fields);
			doReturn(dto).when(metadataInstancesService).findOne(any(Query.class),any(UserDetail.class));
			metadataInstancesService.updateFieldCustomDesc(qualifiedName,fieldCustomDescMap,userDetail);
			verify(metadataInstancesService).update(any(Query.class),any(Update.class),any(UserDetail.class));
			assertEquals("test_fieldName",field.getDescription());
		}
	}
	@Nested
	class DataTypeCheckMultipleTest{
		private String databaseType;
		private String dataType;
		private DataSourceDefinitionServiceImpl dataSourceDefinitionService;
		@BeforeEach
		void beforeEach(){
			dataSourceDefinitionService = mock(DataSourceDefinitionServiceImpl.class);
			ReflectionTestUtils.setField(metadataInstancesService,"dataSourceDefinitionService",dataSourceDefinitionService);
		}
		@Test
		@DisplayName("test dataTypeCheckMultiple method when definitionDto is null")
		void test1(){
			try (MockedStatic<DefaultExpressionMatchingMap> mb = Mockito
					.mockStatic(DefaultExpressionMatchingMap.class)) {
				mb.when(() -> DefaultExpressionMatchingMap.map(anyString())).thenReturn(mock(DefaultExpressionMatchingMap.class));
				DataTypeCheckMultipleVo actual = metadataInstancesService.dataTypeCheckMultiple(databaseType, dataType, userDetail);
				assertEquals(false,actual.isResult());
				mb.verify(()->DefaultExpressionMatchingMap.map(anyString()),new Times(0));
			}
		}
		@Test
		@DisplayName("test dataTypeCheckMultiple method when exprResult is null")
		void test2(){
			try (MockedStatic<DefaultExpressionMatchingMap> mb = Mockito
					.mockStatic(DefaultExpressionMatchingMap.class)) {
				DataSourceDefinitionDto definitionDto = new DataSourceDefinitionDto();
				definitionDto.setExpression("expression");
				when(dataSourceDefinitionService.getByDataSourceType(databaseType,userDetail)).thenReturn(definitionDto);
				DefaultExpressionMatchingMap map = mock(DefaultExpressionMatchingMap.class);
				mb.when(() -> DefaultExpressionMatchingMap.map(anyString())).thenReturn(map);
				when(map.get(anyString())).thenReturn(null);
				DataTypeCheckMultipleVo actual = metadataInstancesService.dataTypeCheckMultiple(databaseType, dataType, userDetail);
				assertEquals(false,actual.isResult());
			}
		}
		@Test
		@DisplayName("test dataTypeCheckMultiple method when exprResult getParams is null")
		void test3(){
			try (MockedStatic<DefaultExpressionMatchingMap> mb = Mockito
					.mockStatic(DefaultExpressionMatchingMap.class)) {
				DataSourceDefinitionDto definitionDto = new DataSourceDefinitionDto();
				definitionDto.setExpression("expression");
				when(dataSourceDefinitionService.getByDataSourceType(databaseType,userDetail)).thenReturn(definitionDto);
				DefaultExpressionMatchingMap map = mock(DefaultExpressionMatchingMap.class);
				mb.when(() -> DefaultExpressionMatchingMap.map(anyString())).thenReturn(map);
				TypeExprResult<DataMap> exprResult = mock(TypeExprResult.class);
				when(map.get(anyString())).thenReturn(exprResult);
				when(exprResult.getParams()).thenReturn(null);
				DataTypeCheckMultipleVo actual = metadataInstancesService.dataTypeCheckMultiple(databaseType, dataType, userDetail);
				assertEquals(false,actual.isResult());
				verify(exprResult,never()).getValue();
			}
		}
		@Test
		@DisplayName("test dataTypeCheckMultiple method for TapStringMapping")
		void test4(){
			try (MockedStatic<DefaultExpressionMatchingMap> mb = Mockito
					.mockStatic(DefaultExpressionMatchingMap.class)) {
				dataType = "data(Type";
				DataSourceDefinitionDto definitionDto = new DataSourceDefinitionDto();
				definitionDto.setExpression("expression");
				when(dataSourceDefinitionService.getByDataSourceType(databaseType,userDetail)).thenReturn(definitionDto);
				DefaultExpressionMatchingMap map = mock(DefaultExpressionMatchingMap.class);
				mb.when(() -> DefaultExpressionMatchingMap.map(anyString())).thenReturn(map);
				TypeExprResult<DataMap> exprResult = mock(TypeExprResult.class);
				when(map.get(anyString())).thenReturn(exprResult);
				when(exprResult.getParams()).thenReturn(new HashMap<>());
				DataMap dataMap = mock(DataMap.class);
				when(exprResult.getValue()).thenReturn(dataMap);
				when(dataMap.get("_tapMapping")).thenReturn(mock(TapStringMapping.class));
				DataTypeCheckMultipleVo actual = metadataInstancesService.dataTypeCheckMultiple(databaseType, dataType, userDetail);
				assertEquals(true,actual.isResult());
				assertEquals("data",actual.getOriginType());
			}
		}
	}
	@Nested
	class GetTypeFilterTest{
		@Test
		void testGetTypeFilterNormal(){
			String nodeId = "111";
			List<MetadataInstancesDto> metadataInstancesDtos = new ArrayList<>();
			MetadataInstancesDto metadataInstancesDto = new MetadataInstancesDto();
			List<Field> fields = new ArrayList<>();
			Field field = new Field();
			field.setDataType("(test)dataType");
			fields.add(field);
			metadataInstancesDto.setFields(fields);
			metadataInstancesDtos.add(metadataInstancesDto);
			doReturn(metadataInstancesDtos).when(metadataInstancesService).findByNodeId(nodeId,null,userDetail,null);
			Set<String> actual = metadataInstancesService.getTypeFilter(nodeId, userDetail);
			assertEquals("dataType",actual.stream().findFirst().get());
		}
	}
	@Nested
	class MultiTransformTest{
		private MultiPleTransformReq multiPleTransformReq;
		private DataSourceDefinitionServiceImpl dataSourceDefinitionService;
		@BeforeEach
		void beforeEach(){
			dataSourceDefinitionService = mock(DataSourceDefinitionServiceImpl.class);
			ReflectionTestUtils.setField(metadataInstancesService,"dataSourceDefinitionService",dataSourceDefinitionService);
		}
		@Test
		@DisplayName("test multiTransform method when fields is empty")
		void test1(){
			multiPleTransformReq = new MultiPleTransformReq();
			MetadataInstancesDto actual = metadataInstancesService.multiTransform(multiPleTransformReq, userDetail);
			assertEquals(null,actual.getFields());
		}
		@Test
		@DisplayName("test multiTransform method when rules is empty")
		void test2(){
			multiPleTransformReq= new MultiPleTransformReq();
			List<Field> fields = new ArrayList<>();
			Field field = new Field();
			fields.add(field);
			multiPleTransformReq.setFields(fields);
			MetadataInstancesDto actual = metadataInstancesService.multiTransform(multiPleTransformReq, userDetail);
			assertEquals(fields,actual.getFields());
			verify(dataSourceDefinitionService,never()).getByDataSourceType(anyString(),any(UserDetail.class));
		}
		@Test
		@DisplayName("test multiTransform method when dataSourceDefinitionDto is null")
		void test3(){
			multiPleTransformReq= new MultiPleTransformReq();
			List<Field> fields = new ArrayList<>();
			Field field = new Field();
			fields.add(field);
			multiPleTransformReq.setFields(fields);
			List<FieldChangeRule> rules = new ArrayList<>();
			FieldChangeRule rule = new FieldChangeRule();
			rules.add(rule);
			multiPleTransformReq.setRules(rules);
			MetadataInstancesDto actual = metadataInstancesService.multiTransform(multiPleTransformReq, userDetail);
			assertEquals(fields,actual.getFields());
		}
		@Test
		@DisplayName("test multiTransform method normal")
		void test4(){
			try (MockedStatic<DefaultExpressionMatchingMap> mb = Mockito
					.mockStatic(DefaultExpressionMatchingMap.class)) {
				mb.when(()->DefaultExpressionMatchingMap.map(anyString())).thenReturn(mock(DefaultExpressionMatchingMap.class));
				multiPleTransformReq= new MultiPleTransformReq();
				List<Field> fields = new ArrayList<>();
				Field field = new Field();
				field.setDataTypeTemp("temp");
				fields.add(field);
				multiPleTransformReq.setFields(fields);
				List<FieldChangeRule> rules = new ArrayList<>();
				FieldChangeRule rule = new FieldChangeRule();
				rules.add(rule);
				multiPleTransformReq.setRules(rules);
				multiPleTransformReq.setDatabaseType("databaseType");
				multiPleTransformReq.setNodeId("111");
				multiPleTransformReq.setQualifiedName("qualifiedName");
				DataSourceDefinitionDto dataSourceDefinitionDto = new DataSourceDefinitionDto();
				dataSourceDefinitionDto.setExpression("test_expression");
				when(dataSourceDefinitionService.getByDataSourceType(anyString(),any(UserDetail.class))).thenReturn(dataSourceDefinitionDto);
				MetadataInstancesDto actual = metadataInstancesService.multiTransform(multiPleTransformReq, userDetail);
				assertEquals(fields,actual.getFields());
			}
		}
	}
	@Nested
	class CheckMetadataInstancesIndexTest{
		String cacheKeys;
		String id;
		@Test
		@DisplayName("test checkMetadataInstancesIndex method when columnName is not blank")
		void test1(){
			id = "65bc933c6129fe73d7858b40";
			cacheKeys = "col1";
			MetadataInstancesDto metadataInstancesDto = new MetadataInstancesDto();
			List<TableIndex> indices = new ArrayList<>();
			TableIndex index = new TableIndex();
			List<TableIndexColumn> columns = new ArrayList<>();
			TableIndexColumn column = new TableIndexColumn();
			column.setColumnName("col1");
			columns.add(column);
			index.setColumns(columns);
			indices.add(index);
			metadataInstancesDto.setIndices(indices);
			doReturn(metadataInstancesDto).when(metadataInstancesService).findById(any(ObjectId.class));
			Boolean actual = metadataInstancesService.checkMetadataInstancesIndex(cacheKeys, id);
			assertEquals(true,actual);
		}
		@Test
		@DisplayName("test checkMetadataInstancesIndex method when columnName is blank and dIndex is null")
		void test2(){
			try (MockedStatic<Document> mb = Mockito
					.mockStatic(Document.class)) {
				mb.when(()->Document.parse(anyString())).thenReturn(null);
				id = "65bc933c6129fe73d7858b40";
				cacheKeys = "col1";
				MetadataInstancesDto metadataInstancesDto = new MetadataInstancesDto();
				List<TableIndex> indices = new ArrayList<>();
				TableIndex index = new TableIndex();
				List<TableIndexColumn> columns = new ArrayList<>();
				TableIndexColumn column = new TableIndexColumn();
				columns.add(column);
				index.setColumns(columns);
				index.setIndexName("test_index");
				indices.add(index);
				metadataInstancesDto.setIndices(indices);
				doReturn(metadataInstancesDto).when(metadataInstancesService).findById(any(ObjectId.class));
				Boolean actual = metadataInstancesService.checkMetadataInstancesIndex(cacheKeys, id);
				assertEquals(false,actual);
			}
		}
		@Test
		@DisplayName("test checkMetadataInstancesIndex method when columnName is blank")
		void test3(){
			try (MockedStatic<Document> mb = Mockito
					.mockStatic(Document.class)) {
				Document document = new Document();
				document.put("key",mock(Document.class));
				mb.when(()->Document.parse(anyString())).thenReturn(document);
				id = "65bc933c6129fe73d7858b40";
				cacheKeys = "col1";
				MetadataInstancesDto metadataInstancesDto = new MetadataInstancesDto();
				List<TableIndex> indices = new ArrayList<>();
				TableIndex index = new TableIndex();
				List<TableIndexColumn> columns = new ArrayList<>();
				TableIndexColumn column = new TableIndexColumn();
				columns.add(column);
				index.setColumns(columns);
				index.setIndexName("test_index");
				indices.add(index);
				metadataInstancesDto.setIndices(indices);
				doReturn(metadataInstancesDto).when(metadataInstancesService).findById(any(ObjectId.class));
				Boolean actual = metadataInstancesService.checkMetadataInstancesIndex(cacheKeys, id);
				assertEquals(false,actual);
			}
		}
	}
}
