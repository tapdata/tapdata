package com.tapdata.tm.discovery.service;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.livedataplatform.dto.LiveDataPlatformDto;
import com.tapdata.tm.livedataplatform.service.LiveDataPlatformService;
import com.tapdata.tm.metadatadefinition.dto.MetadataDefinitionDto;
import com.tapdata.tm.metadatadefinition.vo.MetaDataDefinitionVo;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author samuel
 * @Description
 * @create 2024-06-20 17:00
 **/
class DiscoveryServiceImplTest {

	private DiscoveryServiceImpl discoveryService;

	@BeforeEach
	void setUp() {
		discoveryService = new DiscoveryServiceImpl();
	}

	@Nested
	@DisplayName("Method isMDMRoot test")
	class isMDMRootTest {
		@Test
		@DisplayName("test main process")
		void test1() {
			MetadataDefinitionDto metadataDefinitionDto = new MetadataDefinitionDto();
			metadataDefinitionDto.setParent_id(null);
			metadataDefinitionDto.setItemType(new ArrayList<String>() {{
				add(MetadataDefinitionDto.LDP_ITEM_MDM);
			}});
			assertTrue(discoveryService.isMDMRoot(metadataDefinitionDto));

			metadataDefinitionDto.setParent_id("1");
			assertFalse(discoveryService.isMDMRoot(metadataDefinitionDto));

			metadataDefinitionDto.setParent_id(null);
			metadataDefinitionDto.setItemType(new ArrayList<String>() {{
				add(MetadataDefinitionDto.LDP_ITEM_FDM);
			}});
			assertFalse(discoveryService.isMDMRoot(metadataDefinitionDto));
		}

		@Test
		@DisplayName("test input null")
		void test2() {
			assertFalse(discoveryService.isMDMRoot(null));
		}
	}

	@Nested
	@DisplayName("Method addMetadataCriteriaListTags test")
	class addMetadataCriteriaListTagsTest {
		@Test
		@DisplayName("test is mdm root")
		void test1() {
			MetadataDefinitionDto metadataDefinitionDto = new MetadataDefinitionDto();
			metadataDefinitionDto.setParent_id(null);
			metadataDefinitionDto.setItemType(new ArrayList<String>() {{
				add(MetadataDefinitionDto.LDP_ITEM_MDM);
			}});
			List<String> tagIds = new ArrayList<>();
			tagIds.add("1");
			Criteria criteria = new Criteria();
			discoveryService.addMetadataCriteriaListTags(metadataDefinitionDto, tagIds, criteria);

			Document criteriaObject = criteria.getCriteriaObject();
			assertEquals("{\"$or\": [{\"listtags.id\": {\"$exists\": false}}, {\"listtags.id\": {\"$in\": [\"1\"]}}]}", criteriaObject.toJson());
		}

		@Test
		@DisplayName("test is not mdm root")
		void test2() {
			MetadataDefinitionDto metadataDefinitionDto = new MetadataDefinitionDto();
			metadataDefinitionDto.setParent_id("parent1");
			metadataDefinitionDto.setItemType(new ArrayList<String>() {{
				add(MetadataDefinitionDto.LDP_ITEM_MDM);
			}});
			List<String> tagIds = new ArrayList<>();
			tagIds.add("tag1");
			Criteria criteria = new Criteria();
			discoveryService.addMetadataCriteriaListTags(metadataDefinitionDto, tagIds, criteria);

			assertEquals("{\"listtags.id\": {\"$in\": [\"tag1\"]}}", criteria.getCriteriaObject().toJson());
		}

		@Test
		@DisplayName("test tagIds is null")
		void test3() {
			MetadataDefinitionDto metadataDefinitionDto = new MetadataDefinitionDto();
			metadataDefinitionDto.setParent_id(null);
			metadataDefinitionDto.setItemType(new ArrayList<String>() {{
				add(MetadataDefinitionDto.LDP_ITEM_MDM);
			}});
			Criteria criteria = new Criteria();
			discoveryService.addMetadataCriteriaListTags(metadataDefinitionDto, null, criteria);

			assertEquals("{\"$or\": [{\"listtags.id\": {\"$exists\": false}}]}", criteria.getCriteriaObject().toJson());

			metadataDefinitionDto.setParent_id("parent1");
			criteria = new Criteria();
			discoveryService.addMetadataCriteriaListTags(metadataDefinitionDto, null, criteria);

			assertTrue(criteria.getCriteriaObject().isEmpty());
		}
	}

	@Nested
	@DisplayName("Method addMetadataCriteriaMDMConnId test")
	class addMetadataCriteriaMDMConnIdTest {
		@Test
		@DisplayName("test main process")
		void test1() {
			UserDetail user = mock(UserDetail.class);
			Criteria criteria = new Criteria();
			LiveDataPlatformService liveDataPlatformService = mock(LiveDataPlatformService.class);
			ReflectionTestUtils.setField(discoveryService, "liveDataPlatformService", liveDataPlatformService);
			LiveDataPlatformDto liveDataPlatformDto = new LiveDataPlatformDto();
			liveDataPlatformDto.setFdmStorageConnectionId("fdm_conn_id");
			liveDataPlatformDto.setMdmStorageConnectionId("mdm_conn_id");
			when(liveDataPlatformService.findOne(any(Query.class))).thenReturn(liveDataPlatformDto);
			MetadataDefinitionDto metadataDefinitionDto = new MetadataDefinitionDto();
			List<String> itemType = new ArrayList<>();
			itemType.add(MetadataDefinitionDto.LDP_ITEM_FDM);
			itemType.add(MetadataDefinitionDto.LDP_ITEM_MDM);
			metadataDefinitionDto.setItemType(itemType);
			discoveryService.addMetadataCriteriaMDMConnId(user, criteria, metadataDefinitionDto);

			assertEquals("{\"source._id\": {\"$in\": [\"fdm_conn_id\", \"mdm_conn_id\"]}}", criteria.getCriteriaObject().toJson());
		}

		@Test
		@DisplayName("test ldp not exists")
		void test2() {
			UserDetail user = mock(UserDetail.class);
			Criteria criteria = new Criteria();
			LiveDataPlatformService liveDataPlatformService = mock(LiveDataPlatformService.class);
			ReflectionTestUtils.setField(discoveryService, "liveDataPlatformService", liveDataPlatformService);
			when(liveDataPlatformService.findOne(any(Query.class))).thenReturn(null);
			MetadataDefinitionDto metadataDefinitionDto = new MetadataDefinitionDto();
			List<String> itemType = new ArrayList<>();
			itemType.add(MetadataDefinitionDto.LDP_ITEM_FDM);
			itemType.add(MetadataDefinitionDto.LDP_ITEM_MDM);
			metadataDefinitionDto.setItemType(itemType);
			BizException bizException = assertThrows(BizException.class, () -> discoveryService.addMetadataCriteriaMDMConnId(user, criteria, metadataDefinitionDto));

			assertEquals("Ldp.not.exists", bizException.getErrorCode());
		}

		@Test
		@DisplayName("test metadataDefinitionDto is null")
		void test3() {
			UserDetail user = mock(UserDetail.class);
			Criteria criteria = new Criteria();
			LiveDataPlatformService liveDataPlatformService = mock(LiveDataPlatformService.class);
			ReflectionTestUtils.setField(discoveryService, "liveDataPlatformService", liveDataPlatformService);
			LiveDataPlatformDto liveDataPlatformDto = new LiveDataPlatformDto();
			liveDataPlatformDto.setFdmStorageConnectionId("fdm_conn_id");
			liveDataPlatformDto.setMdmStorageConnectionId("mdm_conn_id");
			when(liveDataPlatformService.findOne(any(Query.class))).thenReturn(liveDataPlatformDto);

			assertDoesNotThrow(()-> discoveryService.addMetadataCriteriaMDMConnId(user, criteria, null));

			assertTrue(criteria.getCriteriaObject().isEmpty());
		}

		@Test
		@DisplayName("test itemType only have fdm")
		void test4() {
			UserDetail user = mock(UserDetail.class);
			Criteria criteria = new Criteria();
			LiveDataPlatformService liveDataPlatformService = mock(LiveDataPlatformService.class);
			ReflectionTestUtils.setField(discoveryService, "liveDataPlatformService", liveDataPlatformService);
			LiveDataPlatformDto liveDataPlatformDto = new LiveDataPlatformDto();
			liveDataPlatformDto.setFdmStorageConnectionId("fdm_conn_id");
			liveDataPlatformDto.setMdmStorageConnectionId("mdm_conn_id");
			when(liveDataPlatformService.findOne(any(Query.class))).thenReturn(liveDataPlatformDto);
			MetadataDefinitionDto metadataDefinitionDto = new MetadataDefinitionDto();
			List<String> itemType = new ArrayList<>();
			itemType.add(MetadataDefinitionDto.LDP_ITEM_FDM);
			metadataDefinitionDto.setItemType(itemType);
			discoveryService.addMetadataCriteriaMDMConnId(user, criteria, metadataDefinitionDto);

			assertEquals("{\"source._id\": {\"$in\": [\"fdm_conn_id\"]}}", criteria.getCriteriaObject().toJson());
		}

		@Test
		@DisplayName("test itemType only have mdm")
		void test5() {
			UserDetail user = mock(UserDetail.class);
			Criteria criteria = new Criteria();
			LiveDataPlatformService liveDataPlatformService = mock(LiveDataPlatformService.class);
			ReflectionTestUtils.setField(discoveryService, "liveDataPlatformService", liveDataPlatformService);
			LiveDataPlatformDto liveDataPlatformDto = new LiveDataPlatformDto();
			liveDataPlatformDto.setFdmStorageConnectionId("fdm_conn_id");
			liveDataPlatformDto.setMdmStorageConnectionId("mdm_conn_id");
			when(liveDataPlatformService.findOne(any(Query.class))).thenReturn(liveDataPlatformDto);
			MetadataDefinitionDto metadataDefinitionDto = new MetadataDefinitionDto();
			List<String> itemType = new ArrayList<>();
			itemType.add(MetadataDefinitionDto.LDP_ITEM_MDM);
			metadataDefinitionDto.setItemType(itemType);
			discoveryService.addMetadataCriteriaMDMConnId(user, criteria, metadataDefinitionDto);

			assertEquals("{\"source._id\": {\"$in\": [\"mdm_conn_id\"]}}", criteria.getCriteriaObject().toJson());
		}

		@Test
		@DisplayName("test itemType don't have fdm and mdm")
		void test6() {
			UserDetail user = mock(UserDetail.class);
			Criteria criteria = new Criteria();
			LiveDataPlatformService liveDataPlatformService = mock(LiveDataPlatformService.class);
			ReflectionTestUtils.setField(discoveryService, "liveDataPlatformService", liveDataPlatformService);
			LiveDataPlatformDto liveDataPlatformDto = new LiveDataPlatformDto();
			liveDataPlatformDto.setFdmStorageConnectionId("fdm_conn_id");
			liveDataPlatformDto.setMdmStorageConnectionId("mdm_conn_id");
			when(liveDataPlatformService.findOne(any(Query.class))).thenReturn(liveDataPlatformDto);
			MetadataDefinitionDto metadataDefinitionDto = new MetadataDefinitionDto();
			List<String> itemType = new ArrayList<>();
			itemType.add(MetadataDefinitionDto.ITEM_TYPE_APP);
			metadataDefinitionDto.setItemType(itemType);
			discoveryService.addMetadataCriteriaMDMConnId(user, criteria, metadataDefinitionDto);

			assertTrue(criteria.getCriteriaObject().isEmpty());
		}

		@Test
		@DisplayName("test itemType is null")
		void test7() {
			UserDetail user = mock(UserDetail.class);
			Criteria criteria = new Criteria();
			LiveDataPlatformService liveDataPlatformService = mock(LiveDataPlatformService.class);
			ReflectionTestUtils.setField(discoveryService, "liveDataPlatformService", liveDataPlatformService);
			LiveDataPlatformDto liveDataPlatformDto = new LiveDataPlatformDto();
			liveDataPlatformDto.setFdmStorageConnectionId("fdm_conn_id");
			liveDataPlatformDto.setMdmStorageConnectionId("mdm_conn_id");
			when(liveDataPlatformService.findOne(any(Query.class))).thenReturn(liveDataPlatformDto);
			MetadataDefinitionDto metadataDefinitionDto = new MetadataDefinitionDto();
			metadataDefinitionDto.setItemType(null);
			discoveryService.addMetadataCriteriaMDMConnId(user, criteria, metadataDefinitionDto);

			assertTrue(criteria.getCriteriaObject().isEmpty());
		}

		@Test
		@DisplayName("test fdm connection id is blank")
		void test8() {
			UserDetail user = mock(UserDetail.class);
			Criteria criteria = new Criteria();
			LiveDataPlatformService liveDataPlatformService = mock(LiveDataPlatformService.class);
			ReflectionTestUtils.setField(discoveryService, "liveDataPlatformService", liveDataPlatformService);
			LiveDataPlatformDto liveDataPlatformDto = new LiveDataPlatformDto();
			liveDataPlatformDto.setFdmStorageConnectionId(null);
			liveDataPlatformDto.setMdmStorageConnectionId("mdm_conn_id");
			when(liveDataPlatformService.findOne(any(Query.class))).thenReturn(liveDataPlatformDto);
			MetadataDefinitionDto metadataDefinitionDto = new MetadataDefinitionDto();
			List<String> itemType = new ArrayList<>();
			itemType.add(MetadataDefinitionDto.LDP_ITEM_FDM);
			itemType.add(MetadataDefinitionDto.LDP_ITEM_MDM);
			metadataDefinitionDto.setItemType(itemType);
			discoveryService.addMetadataCriteriaMDMConnId(user, criteria, metadataDefinitionDto);

			assertEquals("{\"source._id\": {\"$in\": [\"mdm_conn_id\"]}}", criteria.getCriteriaObject().toJson());

			criteria = new Criteria();
			liveDataPlatformDto.setFdmStorageConnectionId("");
			discoveryService.addMetadataCriteriaMDMConnId(user, criteria, metadataDefinitionDto);

			assertEquals("{\"source._id\": {\"$in\": [\"mdm_conn_id\"]}}", criteria.getCriteriaObject().toJson());
		}

		@Test
		@DisplayName("test mdm connection id is blank")
		void test9() {
			UserDetail user = mock(UserDetail.class);
			Criteria criteria = new Criteria();
			LiveDataPlatformService liveDataPlatformService = mock(LiveDataPlatformService.class);
			ReflectionTestUtils.setField(discoveryService, "liveDataPlatformService", liveDataPlatformService);
			LiveDataPlatformDto liveDataPlatformDto = new LiveDataPlatformDto();
			liveDataPlatformDto.setFdmStorageConnectionId("fdm_conn_id");
			liveDataPlatformDto.setMdmStorageConnectionId(null);
			when(liveDataPlatformService.findOne(any(Query.class))).thenReturn(liveDataPlatformDto);
			MetadataDefinitionDto metadataDefinitionDto = new MetadataDefinitionDto();
			List<String> itemType = new ArrayList<>();
			itemType.add(MetadataDefinitionDto.LDP_ITEM_FDM);
			itemType.add(MetadataDefinitionDto.LDP_ITEM_MDM);
			metadataDefinitionDto.setItemType(itemType);
			discoveryService.addMetadataCriteriaMDMConnId(user, criteria, metadataDefinitionDto);

			assertEquals("{\"source._id\": {\"$in\": [\"fdm_conn_id\"]}}", criteria.getCriteriaObject().toJson());

			criteria = new Criteria();
			liveDataPlatformDto.setMdmStorageConnectionId("");
			discoveryService.addMetadataCriteriaMDMConnId(user, criteria, metadataDefinitionDto);

			assertEquals("{\"source._id\": {\"$in\": [\"fdm_conn_id\"]}}", criteria.getCriteriaObject().toJson());
		}
	}
}