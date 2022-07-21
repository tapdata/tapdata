package io.tapdata.cdc.ddl;

import lombok.Getter;
import lombok.Setter;

/**
 * DDL事件
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/11 下午5:53 Create
 */
@Setter
@Getter
public abstract class DdlEvent {

	private DdlOperator op;
	private String ddl;

	public DdlEvent() {
	}

	public DdlEvent(DdlOperator op, String ddl) {
		setOp(op);
		setDdl(ddl);
	}
}
