package io.tapdata.cdc.ddl;

/**
 * DDL事件操作类型
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/9/10 下午4:51 Create
 * @since JDK1.1
 */
public enum DdlOperator {
	UnSupported,
	RenameStruct, CommentStruct, DropStruct, CreateStruct, AlertStruct,
	RenameField, CommentField, DropField, AddField, AlterField,
	;
}
