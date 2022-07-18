package io.tapdata.ddlp.events;

import io.tapdata.ddlp.DDLEvent;
import io.tapdata.ddlp.DDLOperator;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * DDL事件 - 结构
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/16 上午2:28 Create
 */
@Setter
@Getter
public abstract class AbsStruct extends DDLEvent {
	private List<String> namespace;

	protected AbsStruct() {
	}

	protected AbsStruct(DDLOperator op, String ddl, List<String> namespace) {
		super(op, ddl);
		this.namespace = namespace;
	}
}
