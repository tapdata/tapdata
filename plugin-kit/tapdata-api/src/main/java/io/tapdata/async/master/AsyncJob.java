package io.tapdata.async.master;

/**
 * @author aplomb
 */
public interface AsyncJob {
	JobContext run(JobContext jobContext);
}
