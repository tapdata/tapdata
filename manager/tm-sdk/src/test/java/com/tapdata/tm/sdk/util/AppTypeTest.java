package com.tapdata.tm.sdk.util;

import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.concurrent.Callable;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/3/4 17:36 Create
 */
public class AppTypeTest {
	public static <T> T callInType(Callable<T> callable, AppType appType, AppType... otherTypes) throws Exception {
		synchronized (AppType.class) {
			try (MockedStatic<AppType> mocked = Mockito.mockStatic(AppType.class)) {
				if (null == otherTypes) {
					mocked.when(AppType::init).thenReturn(appType);
				} else {
					mocked.when(AppType::init).thenReturn(appType, otherTypes);
				}

				return callable.call();
			}
		}
	}
}
