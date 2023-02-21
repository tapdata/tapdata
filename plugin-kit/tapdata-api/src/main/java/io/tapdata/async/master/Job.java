package io.tapdata.async.master;

/**
 * @author aplomb
 */
public interface Job extends JobBase {
	JobContext run(JobContext jobContext);
}
