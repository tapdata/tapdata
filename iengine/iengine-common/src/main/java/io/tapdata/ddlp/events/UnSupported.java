package io.tapdata.ddlp.events;

import io.tapdata.ddlp.DDLEvent;
import io.tapdata.ddlp.DDLOperator;
import lombok.Getter;
import lombok.Setter;

/**
 * 未支持的 DDL
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/16 上午2:52 Create
 */
@Getter
@Setter
public class UnSupported extends DDLEvent {

	private int pos;
	private String msg;

	public UnSupported() {
		this(null);
	}

	public UnSupported(String ddl) {
		super(DDLOperator.Unsupported, ddl);
	}

	public UnSupported(String ddl, int pos, String msg) {
		this(ddl);
		this.pos = pos;
		this.msg = msg;
	}
}
