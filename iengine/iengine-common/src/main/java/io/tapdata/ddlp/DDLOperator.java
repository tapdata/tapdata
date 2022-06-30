package io.tapdata.ddlp;

/**
 * DDL事件类型
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/16 上午2:30 Create
 */
public enum DDLOperator {
	Unsupported,
	RenameStruct, CommentStruct, DropStruct, CreateStruct, AlertStruct,
	RenameField, CommentField, DropField, AddField, AddFieldDefault, AlterField,
	Other,
	;
}
