package io.tapdata.connector.mysql;

import io.tapdata.common.ddl.DDLFilter;
import io.tapdata.common.ddl.type.DDLParserType;
import io.tapdata.common.ddl.type.DDLType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author samuel
 * @Description
 * @create 2022-06-30 10:20
 **/
public class DDLFilterTest {
	@Test
	public void testFilter() {
		DDLParserType ddlParserType = DDLParserType.MYSQL_CCJ_SQL_PARSER;
		DDLType ddlType = new DDLFilter().testAndGetType(ddlParserType, "alter table TEST.DDL_TEST ADD COLUMN F1 INT");
		Assertions.assertEquals(DDLType.Type.ADD_COLUMN, ddlType.getType());
		ddlType = new DDLFilter().testAndGetType(ddlParserType, "alter table TEST.DDL_TEST ADD F1 INT");
		Assertions.assertEquals(DDLType.Type.ADD_COLUMN, ddlType.getType());
		ddlType = new DDLFilter().testAndGetType(ddlParserType, "alter table TEST.DDL_TEST CHANGE");
		Assertions.assertNull(ddlType);
		ddlType = new DDLFilter().testAndGetType(ddlParserType, "alter table TEST.DDL_TEST CHANGE F1 F1_NEW varchar(50)");
		Assertions.assertEquals(DDLType.Type.CHANGE_COLUMN, ddlType.getType());
		ddlType = new DDLFilter().testAndGetType(ddlParserType, "alter table TEST.DDL_TEST RENAME COLUMN F1 TO F1_NEW");
		Assertions.assertEquals(DDLType.Type.RENAME_COLUMN, ddlType.getType());
		ddlType = new DDLFilter().testAndGetType(ddlParserType, "alter table TEST.DDL_TEST DROP column F1");
		Assertions.assertEquals(DDLType.Type.DROP_COLUMN, ddlType.getType());
	}
}
