package io.tapdata.service;

import com.tapdata.entity.Connections;
import com.tapdata.entity.Job;
import org.apache.logging.log4j.Logger;

import java.util.List;

public interface ITriggerMode {

	void clearTriggerLog(Context context);

	class Context {
		private List<Job> jobs;
		private int triggerLogRemainTime;
		private Job job;
		private Connections sourceConn;
		private Logger logger;

		public Context(List<Job> jobs, int triggerLogRemainTime, Job job, Connections sourceConn, Logger logger) {
			this.jobs = jobs;
			this.triggerLogRemainTime = triggerLogRemainTime;
			this.job = job;
			this.sourceConn = sourceConn;
			this.logger = logger;
		}

		public List<Job> getJobs() {
			return jobs;
		}

		public int getTriggerLogRemainTime() {
			return triggerLogRemainTime;
		}

		public Job getJob() {
			return job;
		}

		public Logger getLogger() {
			return logger;
		}

		public Connections getSourceConn() {
			return sourceConn;
		}
	}
}
