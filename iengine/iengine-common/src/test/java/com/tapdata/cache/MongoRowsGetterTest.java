package com.tapdata.cache;

import com.tapdata.processor.ScriptConnection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class MongoRowsGetterTest {

	@Test
	@DisplayName("TAP-13150 review: close releases the script connection (MongoClient)")
	void closeReleasesScriptConnection() throws Exception {
		// 构造时 ScriptUtil.initScriptConnection 会新建 MongoClient；close 不释放就会随 getter 过期重建而泄漏
		MongoRowsGetter rowsGetter = mock(MongoRowsGetter.class, CALLS_REAL_METHODS);
		ScriptConnection scriptConnection = mock(ScriptConnection.class);
		Field field = MongoRowsGetter.class.getDeclaredField("scriptConnection");
		field.setAccessible(true);
		field.set(rowsGetter, scriptConnection);

		rowsGetter.close();

		verify(scriptConnection).close();
	}

	@Test
	@DisplayName("TAP-13150 review: releaseDataSource must not fall back to close")
	void defaultReleaseDataSourceDoesNotClose() {
		// close 可能销毁新旧 getter 共用的缓存数据，过期淘汰只应释放回源连接器
		ICacheGetter getter = mock(ICacheGetter.class, CALLS_REAL_METHODS);

		getter.releaseDataSource();

		verify(getter, never()).close();
	}
}
