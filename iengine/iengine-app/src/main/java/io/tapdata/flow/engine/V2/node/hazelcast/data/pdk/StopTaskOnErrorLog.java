package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.utils.FormatUtils;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.node.hazelcast.HazelcastBaseNode;

/**
 * @author aplomb
 */
public class StopTaskOnErrorLog implements Log {
	private Log log;
	private HazelcastBaseNode hazelcastBaseNode;
	public StopTaskOnErrorLog(Log log, HazelcastBaseNode hazelcastBaseNode) {
		this.log = log;
		this.hazelcastBaseNode = hazelcastBaseNode;
	}
	@Override
	public void debug(String message, Object... params) {
		log.debug(message, params);
	}

	@Override
	public void info(String message, Object... params) {
		log.info(message, params);
	}

	@Override
	public void warn(String message, Object... params) {
		log.warn(message, params);
	}

	@Override
	public void error(String message, Object... params) {
		if(hazelcastBaseNode != null) {
            String msg  = FormatUtils.format(message, params);
			hazelcastBaseNode.errorHandle(new TapCodeException(msg, new RuntimeException(msg)), null);
		} else {
			log.error(message, params);
		}
	}

	@Override
	public void error(String message, Throwable throwable) {
		if(hazelcastBaseNode != null) {
			hazelcastBaseNode.errorHandle(throwable, message);
		} else {
			log.error(message, throwable);
		}
	}

	@Override
	public void fatal(String message, Object... params) {
		if(hazelcastBaseNode != null) {
			hazelcastBaseNode.errorHandle(new TapCodeException(FormatUtils.format(message, params)), null);
		} else {
			log.fatal(message, params);
		}
	}
}
