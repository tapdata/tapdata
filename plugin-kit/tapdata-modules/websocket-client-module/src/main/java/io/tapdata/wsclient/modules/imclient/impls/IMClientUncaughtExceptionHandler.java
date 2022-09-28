package io.tapdata.wsclient.modules.imclient.impls;

import io.tapdata.entity.logger.TapLogger;

public class IMClientUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
	private static final String TAG = IMClientUncaughtExceptionHandler.class.getSimpleName();

	@Override
	public void uncaughtException(Thread t, Throwable e) {
		TapLogger.warn(TAG, "UncaughtException on thread {} error {}", t, e);
	}
}
