package com.tapdata.tm.modules;

import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.module.dto.ServingIndex;
import com.tapdata.tm.module.dto.ServingIndexField;
import com.tapdata.tm.modules.entity.ModulesEntity;
import com.tapdata.tm.modules.vo.ModulesDetailVo;
import com.tapdata.tm.modules.vo.ModulesListVo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.BeanUtils;

import java.beans.PropertyDescriptor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * TAP-12057 · P2-1：{@code servingIndexes} 必须存在于 Module 读写链路上的<b>每一个载体</b>。
 *
 * <p>平台在这条链路上用 {@code BeanUtils.copyProperties} / {@code BeanUtil.deepCloneList} 按<b>属性名</b>
 * 搬运数据：写侧 DTO→{@link ModulesEntity}，读侧 Entity→DTO→{@link ModulesListVo}（列表，抽屉的数据源）
 * 与 →{@link ModulesDetailVo}（详情）。任何一个载体少这个属性都<b>静默丢弃</b>、无异常无日志——
 * 现象分别是「保存成功但索引没了」和「重开抽屉索引不见了」（2026-07-31 实机验证所见，两处都真实发生过）。</p>
 *
 * <p>因此本用例按载体逐个钉死属性存在性 + 一次真实拷贝，新增载体时同样要加进来。</p>
 */
class ServingIndexesCarrierTest {

	private static ModulesDto dtoWithIndex() {
		ModulesDto dto = new ModulesDto();
		dto.setServingIndexes(new ArrayList<>(Collections.singletonList(
				new ServingIndex("POLICY_ID_1", true,
						new ArrayList<>(Collections.singletonList(new ServingIndexField("POLICY_ID", true)))))));
		return dto;
	}

	@ParameterizedTest(name = "{0} 必须承载 servingIndexes")
	@ValueSource(classes = {ModulesEntity.class, ModulesListVo.class, ModulesDetailVo.class})
	@DisplayName("读写链路上的每个载体都必须有 servingIndexes 属性，否则按属性名的拷贝会静默丢弃")
	void everyCarrierKeepsServingIndexes(Class<?> carrier) throws Exception {
		PropertyDescriptor pd = BeanUtils.getPropertyDescriptor(carrier, "servingIndexes");
		assertNotNull(pd, carrier.getSimpleName() + " 缺 servingIndexes 属性，索引会在这一跳静默丢失");

		Object target = carrier.getDeclaredConstructor().newInstance();
		BeanUtils.copyProperties(dtoWithIndex(), target);

		Object copied = pd.getReadMethod().invoke(target);
		assertNotNull(copied, carrier.getSimpleName() + " 拷贝后 servingIndexes 为 null");
		assertEquals(1, ((List<?>) copied).size());
		assertEquals("POLICY_ID_1", ((ServingIndex) ((List<?>) copied).get(0)).getName());
	}
}
