package io.tapdata.connector.mariadb.entity;

import io.tapdata.entity.event.TapEvent;

/**
 * @author samuel
 * @Description
 * @create 2022-05-24 23:42
 **/
public class MariadbStreamEvent {
	private TapEvent tapEvent;
	private MariadbStreamOffset mysqlStreamOffset;

	public MariadbStreamEvent(TapEvent tapEvent, MariadbStreamOffset mysqlStreamOffset) {
		this.tapEvent = tapEvent;
		this.mysqlStreamOffset = mysqlStreamOffset;
	}

	public TapEvent getTapEvent() {
		return tapEvent;
	}

	public MariadbStreamOffset getMysqlStreamOffset() {
		return mysqlStreamOffset;
	}
}
