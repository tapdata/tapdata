package io.tapdata.connector.mysql;

import io.tapdata.connector.mysql.ddl.DDLFilter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author samuel
 * @Description
 * @create 2022-06-30 10:20
 **/
public class DDLFilterTest {
//	@Test
//	public void testFilter() {
//		DDLFilter.DDLType ddlType = DDLFilter.testAndGetType("alter table TEST.DDL_TEST ADD COLUMN F1 INT");
//		Assertions.assertEquals(DDLFilter.DDLType.ADD_COLUMN, ddlType);
//		ddlType = DDLFilter.testAndGetType("alter table TEST.DDL_TEST ADD F1 INT");
//		Assertions.assertEquals(DDLFilter.DDLType.ADD_COLUMN, ddlType);
//		ddlType = DDLFilter.testAndGetType("alter table TEST.DDL_TEST CHANGE");
//		Assertions.assertNull(ddlType);
//		ddlType = DDLFilter.testAndGetType("alter table TEST.DDL_TEST CHANGE F1 F1_NEW varchar(50)");
//		Assertions.assertEquals(DDLFilter.DDLType.CHANGE_COLUMN, ddlType);
//		ddlType = DDLFilter.testAndGetType("alter table TEST.DDL_TEST RENAME COLUMN F1 TO F1_NEW");
//		Assertions.assertEquals(DDLFilter.DDLType.RENAME_COLUMN, ddlType);
//		ddlType = DDLFilter.testAndGetType("alter table TEST.DDL_TEST DROP column F1");
//		Assertions.assertEquals(DDLFilter.DDLType.DROP_COLUMN, ddlType);
//	}
}
