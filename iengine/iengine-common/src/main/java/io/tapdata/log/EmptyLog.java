package io.tapdata.log;

import io.tapdata.entity.logger.Log;

/**
 * @author samuel
 * @Description
 * @create 2024-11-14 18:06
 **/
public class EmptyLog implements Log {
	@Override
	public void debug(String message, Object... params) {

	}

	@Override
	public void info(String message, Object... params) {

	}

	@Override
	public void trace(String message, Object... params) {

	}

	@Override
	public void warn(String message, Object... params) {

	}

	@Override
	public void error(String message, Object... params) {

	}

	@Override
	public void error(String message, Throwable throwable) {

	}

	@Override
	public void fatal(String message, Object... params) {

	}
}
