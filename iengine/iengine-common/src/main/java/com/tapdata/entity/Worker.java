package com.tapdata.entity;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author tapdata
 * @date 06/12/2017
 */
public class Worker implements Serializable {

	public final static String PING_TIME = "ping_time";
	public final static String PROCESS_ID = "process_id";

	private String id;

	private String user_id;

	/**
	 * ip
	 */
	private String worker_ip;

	/**
	 * 进程id
	 */
	private String process_id;

	/**
	 * Worker 开始时间
	 */
	private long start_time;

	/**
	 * worker 最后一次心跳时间
	 */
	private long ping_time;

	/**
	 * worker type
	 */
	private String worker_type;

	/**
	 * 总线程数
	 */
	private int total_thread;

	/**
	 * 运行中的线程
	 */
	private int running_thread;

	/**
	 * 运行中的jobId
	 */
	private List<String> job_ids;

	private String version;

	private String gitCommitId;

	private String hostname;

	private double cpuLoad;

	private long usedMemory;

	private Map<String, Object> metricValues;

	private Map<String, String> platformInfo;

	private boolean deleted;
	private Boolean isDeleted;

	private boolean stopping;

	public Worker() {
	}

	public Worker(String process_id, long start_time, String worker_type, int total_thread, int running_thread, String user_id, String version, String hostname, double cpuLoad, long usedMemory) {
		this.process_id = process_id;
		this.start_time = start_time;
		this.worker_type = worker_type;
		this.total_thread = total_thread;
		this.running_thread = running_thread;
		this.user_id = user_id;
		this.version = version;
		this.hostname = hostname;
		this.cpuLoad = cpuLoad;
		this.usedMemory = usedMemory;
	}

	public String getWorker_ip() {
		return worker_ip;
	}

	public void setWorker_ip(String worker_ip) {
		this.worker_ip = worker_ip;
	}

	public String getProcess_id() {
		return process_id;
	}

	public void setProcess_id(String process_id) {
		this.process_id = process_id;
	}

	public long getStart_time() {
		return start_time;
	}

	public void setStart_time(long start_time) {
		this.start_time = start_time;
	}

	public long getPing_time() {
		return ping_time;
	}

	public void setPing_time(long ping_time) {
		this.ping_time = ping_time;
	}

	public String getWorker_type() {
		return worker_type;
	}

	public void setWorker_type(String worker_type) {
		this.worker_type = worker_type;
	}

	public int getTotal_thread() {
		return total_thread;
	}

	public void setTotal_thread(int total_thread) {
		this.total_thread = total_thread;
	}

	public int getRunning_thread() {
		return running_thread;
	}

	public void setRunning_thread(int running_thread) {
		this.running_thread = running_thread;
	}

	public List<String> getJob_ids() {
		return job_ids;
	}

	public void setJob_ids(List<String> job_ids) {
		this.job_ids = job_ids;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getUser_id() {
		return user_id;
	}

	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public Map<String, Object> getMetricValues() {
		return metricValues;
	}

	public void setMetricValues(Map<String, Object> metricValues) {
		this.metricValues = metricValues;
	}

	public Map<String, String> getPlatformInfo() {
		return platformInfo;
	}

	public void setPlatformInfo(Map<String, String> platformInfo) {
		this.platformInfo = platformInfo;
	}

	public String getGitCommitId() {
		return gitCommitId;
	}

	public void setGitCommitId(String gitCommitId) {
		this.gitCommitId = gitCommitId;
	}

	public void setDeleted(boolean deleted) {
		deleted = deleted;
	}

	public void setStopping(boolean stopping) {
		this.stopping = stopping;
	}

	public boolean isDeleted() {
		return deleted;
	}

	public Boolean getIsDeleted() {
		return isDeleted;
	}

	public void setIsDeleted(Boolean deleted) {
		isDeleted = deleted;
	}

	public boolean isStopping() {
		return stopping;
	}

	@Override
	public String toString() {
		return "Worker{" +
				"id='" + id + '\'' +
				", user_id='" + user_id + '\'' +
				", worker_ip='" + worker_ip + '\'' +
				", process_id='" + process_id + '\'' +
				", start_time=" + start_time +
				", ping_time=" + ping_time +
				", worker_type='" + worker_type + '\'' +
				", total_thread=" + total_thread +
				", running_thread=" + running_thread +
				", job_ids=" + job_ids +
				", version='" + version + '\'' +
				", gitCommitId='" + gitCommitId + '\'' +
				", hostname='" + hostname + '\'' +
				", cpuLoad=" + cpuLoad +
				", usedMemory=" + usedMemory +
				", metricValues=" + metricValues +
				", platformInfo=" + platformInfo +
				", isDeleted=" + isDeleted +
				", stopping=" + stopping +
				'}';
	}
}
