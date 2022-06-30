package com.tapdata.entity.inspect;

import lombok.Getter;
import lombok.Setter;

import java.time.Instant;

/**
 * 数据校验 - 增量窗口数据
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/11/3 下午4:49 Create
 */
@Getter
@Setter
public class InspectCdcWinData {
	private Instant winBegin;
	private Instant winEnd;
	private Instant dataBegin;
	private Instant dataEnd;
	private String beginOffset;
	private String endOffset;

	public InspectCdcWinData() {
	}

	/**
	 * @param winBegin    窗口开始时间
	 * @param winEnd      窗口结束时间
	 * @param beginOffset 开始偏移量
	 */
	public InspectCdcWinData(Instant winBegin, Instant winEnd, String beginOffset) {
		this.winBegin = winBegin;
		this.winEnd = winEnd;
		this.beginOffset = beginOffset;
	}

}
