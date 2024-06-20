package com.tapdata.tm.discovery.service;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.livedataplatform.dto.LiveDataPlatformDto;
import com.tapdata.tm.livedataplatform.service.LiveDataPlatformService;
import com.tapdata.tm.metadatadefinition.dto.MetadataDefinitionDto;
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
			liveDataPlatformDto.setMdmStorageConnectionId("conn1");
			when(liveDataPlatformService.findOne(any(Query.class))).thenReturn(liveDataPlatformDto);
			discoveryService.addMetadataCriteriaMDMConnId(user, criteria);

			assertEquals("{\"source._id\": \"conn1\"}", criteria.getCriteriaObject().toJson());
		}

		@Test
		@DisplayName("test ldp not exists")
		void test2() {
			UserDetail user = mock(UserDetail.class);
			Criteria criteria = new Criteria();
			LiveDataPlatformService liveDataPlatformService = mock(LiveDataPlatformService.class);
			ReflectionTestUtils.setField(discoveryService, "liveDataPlatformService", liveDataPlatformService);
			when(liveDataPlatformService.findOne(any(Query.class))).thenReturn(null);
			BizException bizException = assertThrows(BizException.class, () -> discoveryService.addMetadataCriteriaMDMConnId(user, criteria));

			assertEquals("Ldp.not.exists", bizException.getErrorCode());
		}
	}
}