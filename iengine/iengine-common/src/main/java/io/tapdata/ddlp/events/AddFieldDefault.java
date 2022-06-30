package io.tapdata.ddlp.events;

import io.tapdata.ddlp.DDLOperator;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * DDL事件 - 添加字段默认值
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/2/9 2:51 PM Create
 */
@Setter
@Getter
public class AddFieldDefault extends AbsField {

	private String value;

	public AddFieldDefault() {
	}

	public AddFieldDefault(String ddl, List<String> namespace, String name, String value) {
		super(DDLOperator.AddFieldDefault, ddl, namespace, name);
		setValue(value);
	}

}
