/**
 * @title: JobEditStatusProocessor
 * @description:
 * @author lk
 * @date 2021/7/19
 */
package com.tapdata.job;

@JobStatusHandler({JobStatus.EDIT, JobStatus.DONE})
public class JobEditStatusProcessor extends JobStatusProcessor {

	public int a = 1;
	@Override
	void beforeHandle(String source, String target) {
		System.out.println("Job " + target + " status before...");
	}

	@Override
	void afterHandle(String source, String target) {
		System.out.println("Job " + target + " status after...");
	}
}
