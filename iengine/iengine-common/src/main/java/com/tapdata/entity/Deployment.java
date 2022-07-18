package com.tapdata.entity;


import org.apache.commons.lang3.StringUtils;

/**
 * @author huangjq
 * @ClassName: Deployment
 * @Description: TODO
 * @date 17-10-20
 * @since 1.0
 */
public class Deployment {

	private String sync_time;

	private Long sync_time_ts;

	public Deployment(String sync_time, Long sync_time_ts) {
		this.sync_time = sync_time;
		this.sync_time_ts = sync_time_ts;
	}

	public String getSync_time() {
		return sync_time;
	}

	public void setSync_time(String sync_time) {
		this.sync_time = sync_time;
	}

	public Long getSync_time_ts() {
		return sync_time_ts;
	}

	public void setSync_time_ts(Long sync_time_ts) {
		this.sync_time_ts = sync_time_ts;
	}

	@Override
	public String toString() {
		return "Deployment{" +
				"sync_time='" + sync_time + '\'' +
				", sync_time_ts=" + sync_time_ts +
				'}';
	}

	public boolean isEmpty() {
		return StringUtils.isAnyBlank(sync_time)
				|| sync_time_ts <= 0;
	}
}
