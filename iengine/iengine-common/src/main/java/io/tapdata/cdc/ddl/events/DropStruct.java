package io.tapdata.cdc.ddl.events;

import io.tapdata.cdc.ddl.DdlOperator;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * DDL事件 - 删除结构
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/12 上午2:35 Create
 */
@Setter
@Getter
public class DropStruct extends AbsStruct {
	public DropStruct() {
	}

	public DropStruct(String ddl, List<String> namespace) {
		super(DdlOperator.DropStruct, ddl, namespace);
	}
}
