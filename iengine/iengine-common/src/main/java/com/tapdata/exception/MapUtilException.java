package com.tapdata.exception;

/**
 * @author samuel
 * @Description
 * @create 2024-04-19 11:16
 **/
public class MapUtilException extends RuntimeException {
	public MapUtilException(String message) {
		super(message);
	}

	public MapUtilException(String message, Throwable cause) {
		super(message, cause);
	}
}
