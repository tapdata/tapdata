package com.tapdata.entity;

/**
 * @author samuel
 * @Description
 * @create 2023-03-10 15:22
 **/
public class TapdataStartedCdcEvent extends TapdataEvent {
	private Long cdcStartTime;

	protected TapdataStartedCdcEvent() {
	}

	public static TapdataStartedCdcEvent create() {
		return new TapdataStartedCdcEvent();
	}

	public void setCdcStartTime(Long cdcStartTime) {
		this.cdcStartTime = cdcStartTime;
	}

	public Long getCdcStartTime() {
		return cdcStartTime;
	}

	@Override
	protected void clone(TapdataEvent tapdataEvent) {
		super.clone(tapdataEvent);
		if (tapdataEvent instanceof TapdataStartedCdcEvent) {
			((TapdataStartedCdcEvent) tapdataEvent).setCdcStartTime(cdcStartTime);
		}
	}
}
