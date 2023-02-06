package io.tapdata.connector.doris.streamload;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;

/**
 * @author samuel
 * @Description
 * @create 2022-12-26 12:36
 **/
public interface MessageSerializer {
	byte[] serialize(TapTable table, TapRecordEvent recordEvent) throws Throwable;
	byte[] lineEnd();
	default byte[] batchStart(){
		return new byte[0];
	}
	default byte[] batchEnd() {
		return new byte[0];
	}
}
