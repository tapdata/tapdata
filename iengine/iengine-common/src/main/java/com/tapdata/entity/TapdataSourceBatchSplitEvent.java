package com.tapdata.entity;

/**
 * @author samuel
 * @Description
 * @create 2024-09-11 15:52
 **/
public class TapdataSourceBatchSplitEvent extends TapdataEvent {
	@Override
	public boolean isConcurrentWrite() {
		return false;
	}
}
