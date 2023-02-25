package io.tapdata.connector.mysql.entity;

import io.tapdata.entity.event.TapEvent;

/**
 * @author samuel
 * @Description
 * @create 2022-05-24 23:42
 **/
public class MysqlStreamEvent {
	private TapEvent tapEvent;
	private MysqlStreamOffset mysqlStreamOffset;

	public MysqlStreamEvent(TapEvent tapEvent, MysqlStreamOffset mysqlStreamOffset) {
		this.tapEvent = tapEvent;
		this.mysqlStreamOffset = mysqlStreamOffset;
	}

	public TapEvent getTapEvent() {
		return tapEvent;
	}

	public MysqlStreamOffset getMysqlStreamOffset() {
		return mysqlStreamOffset;
	}
}
