package com.tapdata.cache.exception;

import io.tapdata.exception.TapCodeException;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/4/24 14:22 Create
 */
public class ShareCacheException extends TapCodeException {

	private String cacheName;

	public ShareCacheException(String code) {
		super(code);
	}

	public ShareCacheException(String code, String message) {
		super(code, message);
	}

	public ShareCacheException(String code, String message, Throwable cause) {
		super(code, message, cause);
	}

	public ShareCacheException(String code, Throwable cause) {
		super(code, cause);
	}

	public ShareCacheException cacheName(String value) {
		this.cacheName = value;
		return this;
	}

	public String getCacheName() {
		return cacheName;
	}
}
