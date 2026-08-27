package com.tapdata.tm.modules.util;

import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.enums.TableFieldTag;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.module.entity.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * {@link FieldTypeUtil#validCustomWhereIfNeed(ModulesDto)} 的 {@code paths} 分支。
 *
 * <p>2026-08-10（{@code 3c663ab373}，#3418 新建该文件）起，那个分支的卫语句是写反的——
 * {@code if (CollectionUtils.isEmpty(paths))} 之后紧接着 {@code for (Path path : paths)}。
 * 两个后果方向相反，所以要两条用例才钉得住：
 *
 * <ul>
 *   <li>{@code paths == null} ⇒ 直接 NPE。发布 API 时必炸（{@code ModulesService:413}）。</li>
 *   <li>{@code paths} 非空 ⇒ 整块被跳过，<b>path 里的字段从来没被补过 tapType</b>——不报错、不留痕。</li>
 * </ul>
 *
 * <p>第二条是本类存在的理由：只加一个 null 判断（而不改那个反掉的判断）同样能让 NPE 消失、
 * 让既有的两条 {@code normalizesServingIndexes} 用例转绿，但**静默跳过**那半个 bug 原封不动。
 * 那条用例必须在「错的修法」下也是红的，否则它判定不了自己所测的东西。
 */
class FieldTypeUtilValidCustomWhereTest {

	private static Field userCreated(String name) {
		Field f = new Field();
		f.setFieldName(name);
		f.setTag(TableFieldTag.USER_CREATE.getType());
		f.setSimpleTypeName("String");   // FILED_TYPE 里有，且 tapType 留空 ⇒ 该被补上
		return f;
	}

	private static ModulesDto moduleWithPaths(List<Path> paths) {
		ModulesDto dto = new ModulesDto();
		dto.setPaths(paths);
		return dto;
	}

	@Test
	@DisplayName("paths 为 null 不再抛 NPE —— 发布 API 走的就是这条路（ModulesService:413）")
	void nullPathsDoesNotThrow() {
		assertDoesNotThrow(() -> FieldTypeUtil.validCustomWhereIfNeed(moduleWithPaths(null)));
	}

	@Test
	@DisplayName("paths 非空时，path 里的字段确实被补上 tapType —— 只加 null 判断的修法在这条上是红的")
	void nonEmptyPathsAreActuallyProcessed() {
		Field field = userCreated("amount");
		Path path = new Path();
		path.setFields(new ArrayList<>(Collections.singletonList(field)));

		FieldTypeUtil.validCustomWhereIfNeed(moduleWithPaths(new ArrayList<>(Collections.singletonList(path))));

		assertNotNull(field.getTapType(),
				"paths 非空时那个分支必须真的跑起来；卫语句写反或只加 null 判断都会让它被静默跳过");
		assertEquals(FieldTypeUtil.FILED_TYPE.get("String"), field.getTapType());
	}

	@Test
	@DisplayName("paths 为空列表：不抛异常，也不该凭空造出 tapType")
	void emptyPathsIsANoOp() {
		Field orphan = userCreated("unused");
		assertDoesNotThrow(() -> FieldTypeUtil.validCustomWhereIfNeed(moduleWithPaths(new ArrayList<>())));
		assertNull(orphan.getTapType(), "空 paths 不该有任何字段被处理");
	}
}
