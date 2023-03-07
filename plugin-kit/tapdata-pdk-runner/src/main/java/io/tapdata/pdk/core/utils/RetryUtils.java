package io.tapdata.pdk.core.utils;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connection.ErrorHandleFunction;
import io.tapdata.pdk.apis.functions.connection.RetryOptions;
import io.tapdata.pdk.core.api.Node;
import io.tapdata.pdk.core.entity.params.PDKMethodInvoker;
import io.tapdata.pdk.core.error.PDKRunnerErrorCodes;
import io.tapdata.pdk.core.executor.ExecutorsManager;

import java.io.IOException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;

/**
 * @author samuel
 * @Description
 * @create 2022-11-30 11:38
 **/
public class RetryUtils extends CommonUtils {
	private static final AutoRetryPolicy autoRetryPolicy = AutoRetryPolicy.ALWAYS;
	private static List<Class<? extends Throwable>> defaultRetryIncludeList;
	private static BiPredicate<Throwable, Class<? extends Throwable>> matchFilter;

	static {
		defaultRetryIncludeList = new ArrayList<Class<? extends Throwable>>() {{
			add(IOException.class);
			add(SQLRecoverableException.class);
			add(SQLTimeoutException.class);
		}};
		matchFilter = (throwable, aClass) -> null != matchThrowable(throwable, aClass);
	}

	private static boolean needDefaultRetry(Throwable throwable) {
		for (Class<? extends Throwable> match : defaultRetryIncludeList) {
			if (matchFilter.test(throwable, match)) {
				return true;
			}
		}
		return false;
	}

	public static void autoRetry(Node node, PDKMethod method, PDKMethodInvoker invoker) {
		CommonUtils.AnyError runnable = invoker.getR();
		String message = invoker.getMessage();
		String logTag = invoker.getLogTag();
		boolean async = invoker.isAsync();
		long retryPeriodSeconds = invoker.getRetryPeriodSeconds();
		if (retryPeriodSeconds <= 0) {
			throw new IllegalArgumentException("PeriodSeconds can not be zero or less than zero");
		}
		while (invoker.getRetryTimes() >= 0) {
			try {
				runnable.run();
				break;
			} catch (Throwable errThrowable) {
				CommonUtils.FunctionAndContext functionAndContext = CommonUtils.FunctionAndContext.create();
				CommonUtils.prepareFunctionAndContextForNode(node, functionAndContext);
				ErrorHandleFunction errorHandleFunction = functionAndContext.errorHandleFunction();
				switch (autoRetryPolicy) {
					case WHEN_NEED:
						if (null == errorHandleFunction) {
							TapLogger.debug(logTag, "This PDK data source not support retry. ");
							if (errThrowable instanceof CoreException) {
								throw (CoreException) errThrowable;
							}
							throw new CoreException(PDKRunnerErrorCodes.COMMON_UNKNOWN, message + " execute failed, " + errThrowable.getMessage(), errThrowable);
						}
						break;
					case ALWAYS:
					default:
						break;
				}

				long retryTimes = invoker.getRetryTimes();
				if (retryTimes > 0) {
					boolean needDefaultRetry = needDefaultRetry(errThrowable);
					RetryOptions retryOptions = callErrorHandleFunctionIfNeed(method, message, errThrowable, errorHandleFunction, functionAndContext.tapConnectionContext());
					if (!needDefaultRetry) {
						throwIfNeed(retryOptions, message, errThrowable);
					}
					TapLogger.warn(logTag, "AutoRetry info: retry times ({}) | periodSeconds ({} s) | error [{}] Please wait...", invoker.getRetryTimes(), retryPeriodSeconds, getLastCause(errThrowable).getMessage());
					Optional.ofNullable(invoker.getLogListener())
							.ifPresent(log -> log.warn(String.format("AutoRetry info: retry times (%s) | periodSeconds (%s s) | error [%s] Please wait...", invoker.getRetryTimes(), retryPeriodSeconds, errThrowable.getMessage())));
					invoker.setRetryTimes(retryTimes - 1);
					if (async) {
						ExecutorsManager.getInstance().getScheduledExecutorService().schedule(() -> autoRetry(node, method, invoker), retryPeriodSeconds, TimeUnit.SECONDS);
						break;
					} else {
						synchronized (invoker) {
							try {
								invoker.wait(retryPeriodSeconds * 1000);
							} catch (InterruptedException e) {
								//e.printStackTrace();
							}
						}
					}
					callBeforeRetryMethodIfNeed(retryOptions, logTag);
				} else {
					if (errThrowable instanceof CoreException) {
						throw (CoreException) errThrowable;
					}
					throw new CoreException(PDKRunnerErrorCodes.COMMON_UNKNOWN, message + " execute failed, " + getLastCause(errThrowable).getMessage(), errThrowable);
				}
			}
		}
	}

	private static Throwable getLastCause(Throwable e) {
		Throwable last = e;
		while(null != last.getCause()) {
			last = last.getCause();
		}
		return last;
	}

	private static RetryOptions callErrorHandleFunctionIfNeed(PDKMethod method, String message, Throwable errThrowable, ErrorHandleFunction function, TapConnectionContext tapConnectionContext) {
		if (null == function) {
			return null;
		}
		RetryOptions retryOptions;
		try {
			retryOptions = function.needRetry(tapConnectionContext, method, errThrowable);
		} catch (Throwable e) {
			throw new CoreException(PDKRunnerErrorCodes.COMMON_UNKNOWN, message + " execute need retry function failed, " + e.getMessage(), e);
		}
		return retryOptions;
	}

	private static void throwIfNeed(RetryOptions retryOptions, String message, Throwable errThrowable) {
		if (null == retryOptions) {
			return;
		}
		try {
			if (!retryOptions.isNeedRetry()) {
				throw errThrowable;
			}
		} catch (Throwable e) {
			if (errThrowable instanceof CoreException) {
				throw (CoreException) errThrowable;
			}
			throw new CoreException(PDKRunnerErrorCodes.COMMON_UNKNOWN, message + " execute failed, " + errThrowable.getMessage(), errThrowable);
		}
	}

	private static void callBeforeRetryMethodIfNeed(RetryOptions retryOptions, String logTag) {
		if (null == retryOptions) {
			return;
		}
		if (null == retryOptions.getBeforeRetryMethod()) {
			return;
		}
		CommonUtils.ignoreAnyError(() -> retryOptions.getBeforeRetryMethod().run(), logTag);
	}

	public static Throwable matchThrowable(Throwable throwable, Class<? extends Throwable> match) {
		if (null == throwable) {
			return null;
		}
		if (throwable.getClass().equals(match)) {
			return throwable;
		}
		List<Throwable> throwables = new ArrayList<>();
		throwables.add(throwable);
		Throwable matched = null;
		while (!Thread.currentThread().isInterrupted()) {
			Throwable cause = throwables.get(throwables.size() - 1).getCause();
			if (null == cause) {
				break;
			}
			if (throwables.contains(cause)) {
				break;
			}
			if (match.isInstance(cause)) {
				matched = cause;
				break;
			}
			throwables.add(cause);
		}
		return matched;
	}

	private enum AutoRetryPolicy {
		ALWAYS,
		WHEN_NEED,
	}
}
