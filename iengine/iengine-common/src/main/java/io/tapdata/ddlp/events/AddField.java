package io.tapdata.ddlp.events;

import io.tapdata.ddlp.DDLOperator;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * DDL事件 - 添加字段
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/11 下午6:02 Create
 */
@Setter
@Getter
public class AddField extends AbsField {

	// todo: 如需要实现异构 DDL 同步需要进行解析
	// 因时间有限，只实现同库同步，将类型定义放在一个字段
	private String type;

	public AddField() {
	}

	public AddField(String ddl, List<String> namespace, String name, String type) {
		super(DDLOperator.AddField, ddl, namespace, name);
		setType(type);
	}

}
