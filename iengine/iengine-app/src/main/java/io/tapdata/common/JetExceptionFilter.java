package io.tapdata.common;

import com.hazelcast.spi.exception.RetryableHazelcastException;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.Node;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.core.util.Builder;

import java.util.concurrent.CancellationException;

/**
 * @author samuel
 * @Description
 * @create 2023-02-09 21:11
 **/
@Plugin(name = "JetCancellationExceptionFilter",
		category = Node.CATEGORY,
		elementType = Filter.ELEMENT_TYPE, printObject = true)
public class JetExceptionFilter extends AbstractFilter {

	@Override
	public Result filter(LogEvent event) {
		Throwable thrown = event.getThrown();
		if (thrown instanceof CancellationException
				|| thrown instanceof RetryableHazelcastException) {
			return this.onMismatch;
		}
		return Result.NEUTRAL;
	}

	public static class TapLogBuilder implements Builder<JetExceptionFilter> {

		@Override
		public JetExceptionFilter build() {
			return new JetExceptionFilter();
		}
	}
}
