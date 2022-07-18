package io.tapdata.cdc.ddl.events;

import io.tapdata.cdc.ddl.DdlOperator;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * DDL事件 - 修改结构名
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/9/10 下午4:57 Create
 * @since JDK1.1
 */
@Setter
@Getter
public class RenameStruct extends AbsStruct {
	private String rename;

	public RenameStruct() {
	}

	public RenameStruct(String ddl, List<String> namespace, String rename) {
		super(DdlOperator.RenameStruct, ddl, namespace);
		setRename(rename);
	}
}
