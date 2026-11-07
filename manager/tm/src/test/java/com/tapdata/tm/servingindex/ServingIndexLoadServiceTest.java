package com.tapdata.tm.servingindex;

import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.module.dto.LoadedIndexAttribution;
import com.tapdata.tm.module.dto.LoadedServingIndex;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.module.dto.ServingIndex;
import com.tapdata.tm.module.dto.ServingIndexField;
import com.tapdata.tm.module.entity.Path;
import com.tapdata.tm.modules.service.ModulesService;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * P2-2 · TM 加载服务端上下文装配（{@link ServingIndexLoadService}）。
 *
 * <p>只锚定<b>装配契约</b>：从 {@code moduleId} 解析本 Module 的 Path（→ 匹配判据）与已收录签名，扫同
 * (连接,集合) 的兄弟 Module 得「他 API 已收录」签名→API 名，喂给纯策略 {@code ServingIndexLoadPlanner}
 * （策略本身在 tm-api 单测覆盖）。兄弟需同表、需排除自身。</p>
 *
 * <p><b>两库红线</b>（ADR-0002）：本类只经 {@link ModulesService} 读平台库的 Module 元数据（Module 本就是
 * 平台数据），不碰 {@code MongoTemplate}、不读写用户库索引（索引读写在引擎侧 PDK）。</p>
 */
class ServingIndexLoadServiceTest {

	private static final String THIS_ID = "0000000000000000000000a1";
	private static final String SIB_SAME_TABLE_ID = "0000000000000000000000b2";
	private static final String SIB_OTHER_TABLE_ID = "0000000000000000000000c3";
	private static final String CONN = "0000000000000000000000ff";

	private ModulesService modulesService;
	private ServingIndexLoadService service;

	@BeforeEach
	void setUp() {
		modulesService = mock(ModulesService.class);
		service = new ServingIndexLoadService(modulesService);
	}

	private static TapIndex tapIndex(String name, String... ascFields) {
		TapIndex idx = new TapIndex().name(name).unique(false);
		for (String f : ascFields) {
			idx.indexField(new TapIndexField().name(f).fieldAsc(true));
		}
		return idx;
	}

	private static ServingIndex serving(String name, String... ascFields) {
		List<ServingIndexField> fs = new ArrayList<>();
		for (String f : ascFields) {
			fs.add(new ServingIndexField(f, true));
		}
		return new ServingIndex(name, false, fs);
	}

	private static Path requiring(String... required) {
		Path p = new Path();
		List<Field> rq = new ArrayList<>();
		for (String r : required) {
			Field f = new Field();
			f.setFieldName(r);
			rq.add(f);
		}
		p.setRequiredQueryField(rq);
		return p;
	}

	private static ModulesDto module(String id, String name, String table,
									 List<Path> paths, List<ServingIndex> serving) {
		ModulesDto m = new ModulesDto();
		m.setId(new ObjectId(id));
		m.setName(name);
		m.setConnectionId(CONN);
		m.setTableName(table);
		m.setPaths(paths);
		m.setServingIndexes(serving);
		return m;
	}

	@Test
	@DisplayName("装配本 API 的 Path：匹配 Path 的读回索引 → MATCHES_API、默认勾")
	void wiresThisApiPatternToMatch() {
		ModulesDto self = module(THIS_ID, "OrderApi", "orders",
				Collections.singletonList(requiring("custId")), null);
		when(modulesService.findById(any(ObjectId.class))).thenReturn(self);
		when(modulesService.findByConnectionId(anyString())).thenReturn(Collections.singletonList(self));

		List<LoadedServingIndex> out = service.load(THIS_ID, Collections.singletonList(tapIndex("ix_cust", "custId")));

		assertEquals(1, out.size());
		assertEquals(LoadedIndexAttribution.MATCHES_API, out.get(0).getAttribution());
		assertTrue(out.get(0).isDefaultChecked());
	}

	@Test
	@DisplayName("装配本 API 已收录签名：命中 → COLLECTED_BY_THIS_API、默认勾")
	void wiresThisApiCollectedSignatures() {
		ModulesDto self = module(THIS_ID, "OrderApi", "orders", null,
				Collections.singletonList(serving("ix_legacy", "legacyField")));
		when(modulesService.findById(any(ObjectId.class))).thenReturn(self);
		when(modulesService.findByConnectionId(anyString())).thenReturn(Collections.singletonList(self));

		List<LoadedServingIndex> out = service.load(THIS_ID, Collections.singletonList(tapIndex("ix_legacy", "legacyField")));

		assertEquals(LoadedIndexAttribution.COLLECTED_BY_THIS_API, out.get(0).getAttribution());
		assertTrue(out.get(0).isDefaultChecked());
	}

	@Test
	@DisplayName("扫兄弟：同表兄弟贡献「他 API 已收录」（带 API 名）；异表兄弟与自身排除")
	void siblingSameTableContributesOtherApi_otherTableAndSelfExcluded() {
		ModulesDto self = module(THIS_ID, "OrderApi", "orders", null, null);
		ModulesDto sibSame = module(SIB_SAME_TABLE_ID, "OrderApiB", "orders", null,
				Collections.singletonList(serving("ix_shared", "sharedField")));
		ModulesDto sibOther = module(SIB_OTHER_TABLE_ID, "CustApiC", "customers", null,
				Collections.singletonList(serving("ix_other", "otherField")));
		when(modulesService.findById(any(ObjectId.class))).thenReturn(self);
		when(modulesService.findByConnectionId(anyString()))
				.thenReturn(Arrays.asList(self, sibSame, sibOther));

		List<LoadedServingIndex> out = service.load(THIS_ID, Arrays.asList(
				tapIndex("ix_shared", "sharedField"),
				tapIndex("ix_other", "otherField")));

		// sharedField：同表兄弟 OrderApiB 已收录 → COLLECTED_BY_OTHER_API + API 名
		assertEquals(LoadedIndexAttribution.COLLECTED_BY_OTHER_API, out.get(0).getAttribution());
		assertEquals("OrderApiB", out.get(0).getAttributionApi());
		assertFalse(out.get(0).isDefaultChecked());
		// otherField：异表兄弟 CustApiC 的收录不算数（不同集合）→ UNCLASSIFIED
		assertEquals(LoadedIndexAttribution.UNCLASSIFIED, out.get(1).getAttribution());
	}

	@Test
	@DisplayName("Module 不存在 → 响亮失败")
	void throwsWhenModuleMissing() {
		when(modulesService.findById(any(ObjectId.class))).thenReturn(null);
		assertThrows(IllegalArgumentException.class,
				() -> service.load(THIS_ID, Collections.singletonList(tapIndex("ix", "a"))));
	}
}
