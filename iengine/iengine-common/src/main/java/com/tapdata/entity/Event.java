package com.tapdata.entity;

import java.util.List;
import java.util.Map;

public class Event {

	public enum EventName {
		WARN_EMAIL("warn-email"),

		DDL_WARN_EMAIL("ddl-warn-email"),

		CDC_LAG_WARN_EMAIL("cdc-lag-warn-email"),

		TEST_CONNECTION_EMAIL("test-connection-email"),

		JOB_OPERATION_NOTICE_EMAIL("job-operation-notice-email"),

		AGENT_NOTICE_EMAIL("agent-notice-email"),

		TIMEOUT_TXN_EMAIL("timeout-txn-email"),
		;

		public String name;

		EventName(String name) {
			this.name = name;
		}
	}

	public static final String EVENT_STATUS_SUCCESSED = "successed";

	public static final String EVENT_STATUS_FAILED = "failed";

	public static final String EVENT_STATUS_WAITING = "waiting";

	public static final String EVENT_TAG_SYSTEM = "system";

	public static final String EVENT_TAG_USER = "user";

	public String id;

	private String job_id;

	private String type;

	private String name;

	private String tag = EVENT_TAG_SYSTEM;

	private Map<String, Object> initiator;

	private long ts = System.currentTimeMillis();

	private long consume_ts;

	private String event_status = EVENT_STATUS_WAITING;

	private Map<String, Object> failed_result;

	private Map<String, Object> event_data;

	private List<String> receivers;

	public Event() {
	}

	public Event(String job_id, String type, String name, String tag, Map<String, Object> event_data) {
		this.job_id = job_id;
		this.type = type;
		this.name = name;
		this.tag = tag;
		this.event_data = event_data;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	public Map<String, Object> getInitiator() {
		return initiator;
	}

	public void setInitiator(Map<String, Object> initiator) {
		this.initiator = initiator;
	}

	public long getTs() {
		return ts;
	}

	public void setTs(long ts) {
		this.ts = ts;
	}

	public String getEvent_status() {
		return event_status;
	}

	public void setEvent_status(String event_status) {
		this.event_status = event_status;
	}

	public Map<String, Object> getEvent_data() {
		return event_data;
	}

	public void setEvent_data(Map<String, Object> event_data) {
		this.event_data = event_data;
	}

	public String getJob_id() {
		return job_id;
	}

	public void setJob_id(String job_id) {
		this.job_id = job_id;
	}

	public Map<String, Object> getFailed_result() {
		return failed_result;
	}

	public void setFailed_result(Map<String, Object> failed_result) {
		this.failed_result = failed_result;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public long getConsume_ts() {
		return consume_ts;
	}

	public void setConsume_ts(long consume_ts) {
		this.consume_ts = consume_ts;
	}

	public List<String> getReceivers() {
		return receivers;
	}

	public void setReceivers(List<String> receivers) {
		this.receivers = receivers;
	}

	@Override
	public String toString() {
		return "Event{" +
				"id='" + id + '\'' +
				", job_id='" + job_id + '\'' +
				", type='" + type + '\'' +
				", name='" + name + '\'' +
				", tag='" + tag + '\'' +
				", initiator=" + initiator +
				", ts=" + ts +
				", consume_ts=" + consume_ts +
				", event_status='" + event_status + '\'' +
				", failed_result=" + failed_result +
				", event_data=" + event_data +
				", receivers=" + receivers +
				'}';
	}
}
