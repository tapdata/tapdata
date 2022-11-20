package io.tapdata.modules.api.async.master;

/**
 * @author aplomb
 */
public interface AsyncJob {
	JobContext<?> run(AsyncTools asyncTools, JobContext<?> previousJobContext);
}
