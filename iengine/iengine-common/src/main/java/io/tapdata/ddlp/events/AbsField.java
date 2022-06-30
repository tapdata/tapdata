package io.tapdata.ddlp.events;

import io.tapdata.ddlp.DDLOperator;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * DDL事件 - 列操作
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/11 下午6:04 Create
 */
@Setter
@Getter
public abstract class AbsField extends AbsStruct {
	private String name;

	protected AbsField() {
	}

	protected AbsField(DDLOperator op, String ddl, List<String> namespace, String name) {
		super(op, ddl, namespace);
		setName(name);
	}
}
