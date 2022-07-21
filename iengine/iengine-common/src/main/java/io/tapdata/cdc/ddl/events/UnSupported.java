package io.tapdata.cdc.ddl.events;

import io.tapdata.cdc.ddl.DdlEvent;
import io.tapdata.cdc.ddl.DdlOperator;

/**
 * 未支持的 DDL
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/13 上午12:32 Create
 */
public class UnSupported extends DdlEvent {
	public UnSupported() {
	}

	public UnSupported(String ddl) {
		super(DdlOperator.UnSupported, ddl);
	}
}
