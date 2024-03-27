package io.tapdata.exception;

import com.tapdata.entity.ResponseBody;
import com.tapdata.tm.sdk.available.TmStatusService;

/**
 * REST API 服务不可用异常
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/2/18 11:23 Create
 */
public class TmUnavailableException extends RestException {
	public TmUnavailableException(String uri, String method, Object param, ResponseBody responseBody) {
		super(uri, method, param, responseBody);
	}

	public TmUnavailableException(Throwable cause, String uri, String method, Object param, ResponseBody responseBody) {
		super(cause, uri, method, param, responseBody);
	}

	public static boolean isInstance(Throwable cause) {
		if (TmStatusService.isNotEnable()) return false;

		if (!(cause instanceof TmUnavailableException)) {
			if (null == cause.getCause()) {
				return false;
			}

			return isInstance(cause.getCause());
		}
		return true;
	}

	public static boolean notInstance(Throwable cause) {
		return !isInstance(cause);
	}
}
