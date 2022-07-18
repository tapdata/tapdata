package io.tapdata.cdc.ddl.events;

import io.tapdata.cdc.ddl.DdlEvent;
import io.tapdata.cdc.ddl.DdlOperator;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * DDL事件 - 修改表
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/11 下午6:03 Create
 */
@Setter
@Getter
public abstract class AbsStruct extends DdlEvent {
	private List<String> namespace;

	protected AbsStruct() {
	}

	protected AbsStruct(DdlOperator op, String ddl, List<String> namespace) {
		super(op, ddl);
		this.namespace = namespace;
	}
}
