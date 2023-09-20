package io.tapdata.pdk.core.utils;

import io.tapdata.ErrorCodeConfig;
import io.tapdata.ErrorCodeEntity;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.exception.TapCodeException;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connection.ErrorHandleFunction;
import io.tapdata.pdk.apis.functions.connection.RetryOptions;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.ConnectionNode;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.Node;
import io.tapdata.pdk.core.entity.params.PDKMethodInvoker;
import io.tapdata.pdk.core.error.TapPdkRunnerExCode_18;
import io.tapdata.pdk.core.error.TapPdkRunnerUnknownException;
import io.tapdata.pdk.core.executor.ExecutorsManager;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;

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

	private static String nodeInfo(Node node) {
		if (node instanceof ConnectorNode) {
			ConnectorNode connectorNode = (ConnectorNode)node;
			TapNodeInfo info = connectorNode.getTapNodeInfo();
			TapNodeSpecification tapNodeSpecification = info.getTapNodeSpecification();
			return String.format("Node type: %s, dag id: %s", tapNodeSpecification.getName(), node.getDagId());
		} else {
			return node.toString();
		}
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
		boolean isRetry = false;
		while (invoker.getRetryTimes() >= 0) {
			try {
				runnable.run();
				if (isRetry) {
					Optional.ofNullable(invoker.getLogListener())
							.ifPresent(log -> log.info(String.format("Auto retry info: node status restored, retry successfully. %s", nodeInfo(node))));
				}
				break;
			} catch (Throwable errThrowable) {
				CommonUtils.FunctionAndContext functionAndContext = CommonUtils.FunctionAndContext.create();
				CommonUtils.prepareFunctionAndContextForNode(node, functionAndContext);
				ErrorHandleFunction errorHandleFunction = functionAndContext.errorHandleFunction();
				switch (autoRetryPolicy) {
					case WHEN_NEED:
						if (null == errorHandleFunction) {
							Optional.ofNullable(invoker.getLogListener())
									.ifPresent(log -> log.debug("This PDK data source not support retry. " + nodeInfo(node)));
							Optional.ofNullable(invoker.getResetRetry()).ifPresent(Runnable::run);
							errorHandle(errThrowable);
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
						throwIfNeed(invoker, retryOptions, message, errThrowable);
					}
					Optional.ofNullable(invoker.getLogListener())
							.ifPresent(log -> log.warn(
									String.format("Auto retry info: retry times (%s), period seconds %ss, error msg: %s. %s, please wait",
											invoker.getRetryTimes(),
											retryPeriodSeconds,
											errThrowable.getMessage(),
											nodeInfo(node))));
					invoker.setRetryTimes(retryTimes - 1);
					isRetry = true;
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
					if (null != invoker.getStartRetry()) {
						invoker.getStartRetry().run();
					}
				} else {
					errorHandle(errThrowable);
				}
			}
		}
	}

	private static void errorHandle(Throwable errThrowable) {
		Throwable matchThrowable = CommonUtils.matchThrowable(errThrowable, TapCodeException.class);
		if (null != matchThrowable) {
			throw (TapCodeException) matchThrowable;
		}
		throw new TapPdkRunnerUnknownException(errThrowable);
	}

	private static Throwable getLastCause(Throwable e) {
		Throwable last = e;
		while(null != last.getCause()) {
			last = last.getCause();
		}
		return last;
	}

	private static RetryOptions callErrorHandleFunctionIfNeed(PDKMethod method, String message, Throwable errThrowable, ErrorHandleFunction function, TapConnectionContext tapConnectionContext) {
		RetryOptions retryOptions = null;
		if (null != function) {
			try {
				retryOptions = function.needRetry(tapConnectionContext, method, errThrowable);
			} catch (Throwable e) {
				throw new TapCodeException(TapPdkRunnerExCode_18.CALL_ERROR_HANDLE_API_ERROR, "Call error handle function failed", e);
			}
		}
		if (null == retryOptions) {
			retryOptions = RetryOptions.create();
		}
		if (errThrowable instanceof TapCodeException) {
			String code = ((TapCodeException) errThrowable).getCode();
			ErrorCodeEntity errorCode = ErrorCodeConfig.getInstance().getErrorCode(code);
			if (errorCode.isRecoverable()) {
				retryOptions.needRetry(true);
			} else {
				if (!TapPdkRunnerExCode_18.UNKNOWN_ERROR.equals(code)) {
					retryOptions.needRetry(false);
				}
			}
		}
		return retryOptions;
	}

	private static void throwIfNeed(PDKMethodInvoker invoker, RetryOptions retryOptions, String message, Throwable errThrowable) {
		if (null == retryOptions) {
			return;
		}
		try {
			if (!retryOptions.isNeedRetry()) {
				throw errThrowable;
			}
		} catch (Throwable e) {
			Optional.ofNullable(invoker.getResetRetry()).ifPresent(Runnable::run);
			errorHandle(e);
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
