package com.tapdata.entity;

import com.tapdata.entity.dataflow.SyncProgress;

/**
 * @author samuel
 * @Description
 * @create 2022-06-15 11:41
 **/
public class TapdataShareLogEvent extends TapdataEvent {

	private static final long serialVersionUID = 5639631218965384484L;

	public TapdataShareLogEvent() {
		this.type = SyncProgress.Type.LOG_COLLECTOR;
	}
}
