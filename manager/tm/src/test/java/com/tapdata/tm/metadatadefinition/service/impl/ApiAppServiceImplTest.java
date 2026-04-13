package com.tapdata.tm.metadatadefinition.service.impl;

import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.commons.schema.Tag;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadatadefinition.dto.MetadataDefinitionDto;
import com.tapdata.tm.metadatadefinition.service.MetadataDefinitionService;
import com.tapdata.tm.modules.constant.ModuleStatusEnum;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.modules.service.ModulesService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.*;

class ApiAppServiceImplTest {

	private ApiAppServiceImpl apiAppService;
	private MetadataDefinitionService metadataDefinitionService;
	private ModulesService modulesService;
	private SettingsService settingsService;
	private UserDetail user;

	@BeforeEach
	void setUp() {
		apiAppService = new ApiAppServiceImpl();
		metadataDefinitionService = mock(MetadataDefinitionService.class);
		modulesService = mock(ModulesService.class);
		settingsService = mock(SettingsService.class);
		user = mock(UserDetail.class);
		ReflectionTestUtils.setField(apiAppService, "metadataDefinitionService", metadataDefinitionService);
		ReflectionTestUtils.setField(apiAppService, "modulesService", modulesService);
		ReflectionTestUtils.setField(apiAppService, "settingsService", settingsService);
	}

	@Nested
	class FindTest {

		private Filter filter;

		@BeforeEach
		void setUp() {
			filter = new Filter();
		}

		@Test
		void testFindInCloudEnv() {
			when(settingsService.isCloud()).thenReturn(true);

			ObjectId id1 = new ObjectId();
			MetadataDefinitionDto dto1 = new MetadataDefinitionDto();
			dto1.setId(id1);

			Page<MetadataDefinitionDto> page = new Page<>();
			page.setTotal(1);
			page.setItems(Collections.singletonList(dto1));

			when(metadataDefinitionService.findAndChildAccount(any(Filter.class), eq(false), any(UserDetail.class)))
					.thenReturn(page);

			Tag tag = new Tag();
			tag.setId(id1.toHexString());
			ModulesDto module = new ModulesDto();
			module.setListtags(Collections.singletonList(tag));
			module.setStatus(ModuleStatusEnum.ACTIVE.getValue());
			when(modulesService.findAllDto(any(), any(UserDetail.class)))
					.thenReturn(Collections.singletonList(module));

			Page<MetadataDefinitionDto> result = apiAppService.find(filter, user);

			assertNotNull(result);
			assertEquals(1, result.getTotal());
			assertEquals(1, result.getItems().size());
			assertEquals(1, result.getItems().get(0).getApiCount());
			assertEquals(1, result.getItems().get(0).getPublishedApiCount());
			verify(metadataDefinitionService).findAndChildAccount(filter, false, user);
		}

		@Test
		void testFindInNonCloudEnv() {
			when(settingsService.isCloud()).thenReturn(false);

			Page<MetadataDefinitionDto> page = new Page<>();
			page.setTotal(0);
			page.setItems(new ArrayList<>());

			when(metadataDefinitionService.findAndChildAccount(any(Filter.class), eq(true), any(UserDetail.class)))
					.thenReturn(page);
			when(modulesService.findAllDto(any(), any(UserDetail.class)))
					.thenReturn(new ArrayList<>());

			Page<MetadataDefinitionDto> result = apiAppService.find(filter, user);

			assertNotNull(result);
			assertEquals(0, result.getTotal());
			verify(metadataDefinitionService).findAndChildAccount(filter, true, user);
		}

		@Test
		void testFindWithEmptyItems() {
			when(settingsService.isCloud()).thenReturn(true);

			Page<MetadataDefinitionDto> page = new Page<>();
			page.setTotal(0);
			page.setItems(null);

			when(metadataDefinitionService.findAndChildAccount(any(Filter.class), anyBoolean(), any(UserDetail.class)))
					.thenReturn(page);

			Page<MetadataDefinitionDto> result = apiAppService.find(filter, user);

			assertNotNull(result);
			verify(modulesService, never()).findAllDto(any(), any(UserDetail.class));
		}

		@Test
		void testFindWithMultipleItemsAndPartialPublished() {
			when(settingsService.isCloud()).thenReturn(true);

			ObjectId id1 = new ObjectId();
			MetadataDefinitionDto dto1 = new MetadataDefinitionDto();
			dto1.setId(id1);

			Page<MetadataDefinitionDto> page = new Page<>();
			page.setTotal(1);
			page.setItems(Collections.singletonList(dto1));

			when(metadataDefinitionService.findAndChildAccount(any(Filter.class), anyBoolean(), any(UserDetail.class)))
					.thenReturn(page);

			Tag tag = new Tag();
			tag.setId(id1.toHexString());

			ModulesDto activeModule = new ModulesDto();
			activeModule.setListtags(Collections.singletonList(tag));
			activeModule.setStatus(ModuleStatusEnum.ACTIVE.getValue());

			ModulesDto pendingModule = new ModulesDto();
			pendingModule.setListtags(Collections.singletonList(tag));
			pendingModule.setStatus(ModuleStatusEnum.PENDING.getValue());

			when(modulesService.findAllDto(any(), any(UserDetail.class)))
					.thenReturn(Arrays.asList(activeModule, pendingModule));

			Page<MetadataDefinitionDto> result = apiAppService.find(filter, user);

			assertEquals(2, result.getItems().get(0).getApiCount());
			assertEquals(1, result.getItems().get(0).getPublishedApiCount());
		}
	}
}

