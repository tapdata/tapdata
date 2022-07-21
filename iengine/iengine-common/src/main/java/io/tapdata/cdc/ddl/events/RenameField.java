package io.tapdata.cdc.ddl.events;

import io.tapdata.cdc.ddl.DdlOperator;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * DDL事件 - 修改字段名
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/9/10 下午9:24 Create
 * @since JDK1.1
 */
@Setter
@Getter
public class RenameField extends AbsField {
	private String rename;

	public RenameField() {
	}

	public RenameField(String ddl, List<String> namespace, String field, String rename) {
		super(DdlOperator.RenameField, ddl, namespace, field);
		setRename(rename);
	}
}
