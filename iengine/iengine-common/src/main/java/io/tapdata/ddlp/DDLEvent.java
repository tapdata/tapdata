package io.tapdata.ddlp;

import com.alibaba.fastjson.JSON;
import lombok.Getter;
import lombok.Setter;

/**
 * DDL事件基类
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/16 上午12:45 Create
 */
@Getter
@Setter
public abstract class DDLEvent {

	private DDLOperator op;
	private String ddl;

	protected DDLEvent() {
	}

	protected DDLEvent(DDLOperator op, String ddl) {
		setOp(op);
		setDdl(ddl);
	}

	@Override
	public String toString() {
		return JSON.toJSONString(this);
	}

}
