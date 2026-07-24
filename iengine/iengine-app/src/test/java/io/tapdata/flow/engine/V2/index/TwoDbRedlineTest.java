package io.tapdata.flow.engine.V2.index;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * P1-3 两库红线运行期断言（TAP-12057 / ADR-0002）。
 * 核心：目标解析到平台自有库时<b>响亮失败</b>；仅同 host:port/db 才算命中，auth/options 不影响判定。
 */
class TwoDbRedlineTest {

	private static final String PLATFORM = "mongodb://mongo:27017/tapdata";

	@Test
	@DisplayName("目标 uri 与平台库完全相同 → 抛红线违规")
	void sameUri_throws() {
		assertThrows(TwoDbRedlineViolationException.class,
				() -> TwoDbRedline.assertTargetIsUserDb(PLATFORM, PLATFORM));
	}

	@Test
	@DisplayName("同 host:port/db 但带 auth+options → 归一化后仍命中，抛红线违规")
	void sameHostPortDb_withAuthAndOptions_throws() {
		String target = "mongodb://user:pass@mongo:27017/tapdata?replicaSet=rs0&authSource=admin";
		assertThrows(TwoDbRedlineViolationException.class,
				() -> TwoDbRedline.assertTargetIsUserDb(target, PLATFORM));
	}

	@Test
	@DisplayName("同 host:port 但库名不同（用户库 mdm）→ 放行")
	void sameHostPort_differentDb_ok() {
		assertDoesNotThrow(
				() -> TwoDbRedline.assertTargetIsUserDb("mongodb://mongo:27017/mdm", PLATFORM));
	}

	@Test
	@DisplayName("不同 host → 放行")
	void differentHost_ok() {
		assertDoesNotThrow(
				() -> TwoDbRedline.assertTargetIsUserDb("mongodb://userhost:27017/tapdata", PLATFORM));
	}

	@Test
	@DisplayName("目标非 mongo uri（无法与平台 mongo 库相撞）→ 放行")
	void nonMongoTarget_ok() {
		assertDoesNotThrow(
				() -> TwoDbRedline.assertTargetIsUserDb("mysql://mongo:3306/tapdata", PLATFORM));
	}

	@Test
	@DisplayName("目标或平台 uri 缺失（无从比对）→ 放行、不误伤")
	void blankOrNull_ok() {
		assertDoesNotThrow(() -> TwoDbRedline.assertTargetIsUserDb(null, PLATFORM));
		assertDoesNotThrow(() -> TwoDbRedline.assertTargetIsUserDb("", PLATFORM));
		assertDoesNotThrow(() -> TwoDbRedline.assertTargetIsUserDb(PLATFORM, null));
	}
}
