package io.tapdata.cdc.ddl.events;

import io.tapdata.cdc.ddl.DdlOperator;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * DDL事件 - 删除字段
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/11 下午6:03 Create
 */
@Setter
@Getter
public class DropField extends AbsField {

	public DropField() {
	}

	public DropField(String ddl, List<String> namespace, String name) {
		super(DdlOperator.DropField, ddl, namespace, name);
	}

}
