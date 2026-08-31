package com.tapdata.tm.group.service;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.group.dto.GroupInfoRecordDetail;
import com.tapdata.tm.group.dto.GroupInfoRecordDto;
import com.tapdata.tm.group.repostitory.GroupInfoRepository;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Git 导出的「永久锁死」修复：{@code checkExportingGroups} 只认还在超时窗口内的 exporting 记录。
 *
 * <p>修复前，一条因网络卡住（或 TM 节点被 kill）而停在 {@code exporting} 的记录会让该分组
 * <b>再也无法导出</b>——{@code performExport} 的 catch 永远执行不到，没人把它翻成 failed。</p>
 *
 * <p>三条用例的鉴别性各不相同，缺一条都会漏掉一种回归：</p>
 * <ol>
 *   <li>{@link #freshRecordStillBlocks()} —— 窗内记录<b>必须照旧抛</b>。
 *       没有它，"把窗口调到 0 / 直接删掉这道闸" 也能让另外两条绿。</li>
 *   <li>{@link #staleRecordNoLongerBlocks()} —— 超窗记录不再阻塞。这是本次修复的正题。</li>
 *   <li>{@link #staleRecordIsMarkedFailed()} —— 超窗记录被翻成 {@code failed}。
 *       没有它，僵尸记录会在导出历史里永远显示「导出中」，每次导出都要重新判一遍死。</li>
 * </ol>
 */
@ExtendWith(MockitoExtension.class)
class GroupInfoServiceExportTimeoutTest {

	private static final String GROUP_ID = "660000000000000000000001";
	private static final String GROUP_NAME = "my-group";
	private static final long MINUTE_MILLIS = 60L * 1000L;

	@Mock
	private GroupInfoRecordService groupInfoRecordService;

	private GroupInfoService groupInfoService;
	private UserDetail user;

	@BeforeEach
	void setUp() {
		groupInfoService = new GroupInfoService(mock(GroupInfoRepository.class));
		ReflectionTestUtils.setField(groupInfoService, "groupInfoRecordService", groupInfoRecordService);
		user = new UserDetail("userId123", "customerId", "testuser", "password", "customerType",
				"accessCode", false, false, false, false,
				Collections.singletonList(new SimpleGrantedAuthority("role")));
	}

	/** 播一条 exporting 记录，operationTime = 现在往前推 ageMinutes 分钟。 */
	private GroupInfoRecordDto seedExportingRecord(long ageMinutes) {
		GroupInfoRecordDetail detail = new GroupInfoRecordDetail();
		detail.setGroupId(GROUP_ID);
		detail.setGroupName(GROUP_NAME);

		GroupInfoRecordDto record = new GroupInfoRecordDto();
		record.setId(new ObjectId());
		record.setType(GroupInfoRecordDto.TYPE_EXPORT);
		record.setStatus(GroupInfoRecordDto.STATUS_EXPORTING);
		record.setOperationTime(new Date(System.currentTimeMillis() - ageMinutes * MINUTE_MILLIS));
		record.setDetails(Collections.singletonList(detail));

		when(groupInfoRecordService.findAllDto(any(Query.class), eq(user)))
				.thenReturn(Collections.singletonList(record));
		return record;
	}

	private void checkExportingGroups() {
		// protected + 同包，直接调用即可
		groupInfoService.checkExportingGroups(Collections.singletonList(GROUP_ID), user);
	}

	@Test
	@DisplayName("窗内的 exporting 记录照旧阻塞导出")
	void freshRecordStillBlocks() {
		seedExportingRecord(5); // 默认窗口 60 分钟

		BizException e = assertThrows(BizException.class, this::checkExportingGroups);
		assertEquals("GroupInfo.Export.InProgress", e.getErrorCode());
		verify(groupInfoRecordService, never()).updateById(any(ObjectId.class), any(), any());
	}

	@Test
	@DisplayName("超过超时窗口的僵尸记录不再阻塞导出")
	void staleRecordNoLongerBlocks() {
		seedExportingRecord(61); // 默认窗口 60 分钟

		assertDoesNotThrow(this::checkExportingGroups);
	}

	@Test
	@DisplayName("僵尸记录被翻成 failed，不会在导出历史里永远显示导出中")
	void staleRecordIsMarkedFailed() {
		GroupInfoRecordDto record = seedExportingRecord(61);

		checkExportingGroups();

		ArgumentCaptor<Update> update = ArgumentCaptor.forClass(Update.class);
		verify(groupInfoRecordService).updateById(eq(record.getId()), update.capture(), eq(user));
		String rendered = update.getValue().getUpdateObject().toString();
		assertTrue(rendered.contains(GroupInfoRecordDto.STATUS_FAILED),
				"应把状态写成 failed，实际: " + rendered);
		assertTrue(rendered.contains("timed out"),
				"失败原因应说明是超时，实际: " + rendered);
	}

	@Test
	@DisplayName("超时窗口可配置：调小到 1 分钟后，2 分钟前的记录即被判死")
	void timeoutWindowIsConfigurable() {
		ReflectionTestUtils.setField(groupInfoService, "exportTimeoutMinutes", 1);
		seedExportingRecord(2);

		assertDoesNotThrow(this::checkExportingGroups);
	}

	@Test
	@DisplayName("没有 exporting 记录时不查不写")
	void noRecordsIsNoOp() {
		when(groupInfoRecordService.findAllDto(any(Query.class), eq(user)))
				.thenReturn(Collections.<GroupInfoRecordDto>emptyList());

		assertDoesNotThrow(this::checkExportingGroups);
		verify(groupInfoRecordService, never()).updateById(any(ObjectId.class), any(), any());
	}

	/**
	 * 回归：{@code GroupInfoService} 类上带 {@code @Setter(onMethod_ = { @Autowired })}，
	 * Lombok 会给<b>每一个</b>实例字段生成一个标了 {@code @Autowired} 的 setter。
	 * 给这个类加一个 {@code @Value} 的 int 字段，因此会生成
	 * {@code @Autowired setExportTimeoutMinutes(int)} —— Spring 造不出 int 类型的 Bean，
	 * 整个 TM 在启动时就炸，不是降级、是起不来。
	 *
	 * <p>这条用例查的是编译产物而不是源码：这个坑由 Lombok 在编译期生成，
	 * 源码里根本看不见那个 setter，读代码是发现不了的。</p>
	 */
	@Test
	@DisplayName("不得存在参数为简单类型的 @Autowired setter —— 那会让 TM 起不来")
	void noAutowiredSetterTakesASimpleType() {
		List<String> offenders = new ArrayList<>();
		for (Method m : GroupInfoService.class.getDeclaredMethods()) {
			if (!m.isAnnotationPresent(Autowired.class)) {
				continue;
			}
			Class<?>[] params = m.getParameterTypes();
			if (params.length == 1 && (params[0].isPrimitive() || params[0] == String.class)) {
				offenders.add(m.getName() + "(" + params[0].getSimpleName() + ")");
			}
		}
		assertTrue(offenders.isEmpty(),
				"这些 setter 被 Lombok 标成了 @Autowired，但参数是简单类型，Spring 无法注入，"
						+ "TM 启动必然失败。给对应字段加 @Setter(AccessLevel.NONE)：" + offenders);
	}
}
