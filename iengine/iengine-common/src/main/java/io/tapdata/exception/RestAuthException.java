package io.tapdata.exception;

import com.tapdata.entity.ResponseBody;

/**
 * REST API 授权异常
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/4/13 2:55 PM Create
 */
public class RestAuthException extends RestDoNotRetryException {
	public RestAuthException(String uri, String method, Object param, ResponseBody responseBody) {
		super(uri, method, param, responseBody);
	}

	public RestAuthException(Throwable cause, String uri, String method, Object param, ResponseBody responseBody) {
		super(cause, uri, method, param, responseBody);
	}
}
