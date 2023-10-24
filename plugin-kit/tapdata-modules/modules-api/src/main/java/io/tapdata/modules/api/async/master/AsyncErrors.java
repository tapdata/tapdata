package io.tapdata.modules.api.async.master;

/**
 * @author aplomb
 */
public interface AsyncErrors {
	int MISSING_JOB_CONTEXT = 9000;
	int MISSING_TAP_UTILS = 9001;
	int ASYNC_JOB_STOPPED = 9002;
	int ILLEGAL_ARGUMENTS = 9003;
	int MISSING_JOB_CLASS_FOR_TYPE = 9004;
	int INITIATE_JOB_CLASS_FAILED = 9005;
	int UNKNOWN_JOB = 9006;
	int PARALLEL_WORKER_STOPPED = 9007;
	int PARALLEL_WORKER_FINISHED = 9008;
}
