package io.tapdata.cdc.ddl.events;

import io.tapdata.cdc.ddl.DdlOperator;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * DDL事件 - 修改字段
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/12 下午9:59 Create
 */
@Setter
@Getter
public class AlterField extends AbsField {

	// todo: 如需要实现异构 DDL 同步需要进行解析
	// 因时间有限，只实现同库同步，将类型定义放在一个字段
	private String type;

	public AlterField() {
	}

	public AlterField(String ddl, List<String> namespace, String name, String type) {
		super(DdlOperator.AlterField, ddl, namespace, name);
		setType(type);
	}

}
