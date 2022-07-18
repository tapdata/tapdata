package io.tapdata.exception;

import com.tapdata.entity.ResponseBody;

/**
 * REST API 不重试异常
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/4/13 2:48 PM Create
 */
public class RestDoNotRetryException extends RestException {
	public RestDoNotRetryException(String uri, String method, Object param, ResponseBody responseBody) {
		super(uri, method, param, responseBody);
	}

	public RestDoNotRetryException(Throwable cause, String uri, String method, Object param, ResponseBody responseBody) {
		super(cause, uri, method, param, responseBody);
	}
}
