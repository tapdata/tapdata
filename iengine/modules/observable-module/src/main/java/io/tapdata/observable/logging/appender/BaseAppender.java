package io.tapdata.observable.logging.appender;

import org.apache.logging.log4j.core.Logger;

import java.io.Serializable;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2023-03-30 17:10
 **/
public abstract class BaseAppender<T> implements Appender<T>, Serializable {
	private static final long serialVersionUID = 7137498633584081775L;

	protected void removeAppenders(Logger logger) {
		if (null == logger) {
			return;
		}
		Map<String, org.apache.logging.log4j.core.Appender> appenders = logger.getAppenders();
		for (org.apache.logging.log4j.core.Appender appender : appenders.values()) {
			logger.removeAppender(appender);
			appender.stop();
		}
	}
}
