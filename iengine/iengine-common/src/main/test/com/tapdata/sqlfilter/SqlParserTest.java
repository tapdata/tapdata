package com.tapdata.sqlfilter;

import io.tapdata.sqlfilter.ConditionExpressionFilter;
import io.tapdata.sqlfilter.SqlUtils;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.junit.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class SqlParserTest {

	@Test
	public void filterTest() throws JSQLParserException {
		String sql = "select * from user where name like '%s%' and status in (1,2) and uid>0";

		Select stmt = (Select) CCJSqlParserUtil.parse(sql);

		PlainSelect plainSelect = (PlainSelect) stmt.getSelectBody();

		Expression where = plainSelect.getWhere();

		Map<String, Object> data = new HashMap<String, Object>() {{
			put("uid", 1);
			put("name", "sam");
			put("age", 20);
			put("sex", "m");
			put("create_time", new Date());
			put("status", 3);
		}};
		ConditionExpressionFilter conditionExpressionFilter = new ConditionExpressionFilter(data);
		boolean result = conditionExpressionFilter.filter(where);

		assertFalse(result);

		data.put("status", 1);
		conditionExpressionFilter = new ConditionExpressionFilter(data);
		result = conditionExpressionFilter.filter(where);

		assertTrue(result);
	}

	@Test
	public void sqlQueryMapTest() throws JSQLParserException {
		Map<String, Object> data = new HashMap<String, Object>() {{
			put("uid", null);
			put("name", "sam");
			put("age", 20);
			put("sex", "m");
			put("create_time", new Date());
			put("status", 3);
			put("REGN", "XX");
		}};

		String sql = "SELECT \"IV_CELL_MASTR_MERGE\".\"CM_ID\" \"CM_ID\",\"IV_CELL_MASTR_MERGE\".\"BRAND_GRP\" \"BRAND_GRP\"," +
				"\"IV_CELL_MASTR_MERGE\".\"BRAND\" \"BRAND\",\"IV_CELL_MASTR_MERGE\".\"PROD_CLASS\" \"PROD_CLASS\"," +
				"\"IV_CELL_MASTR_MERGE\".\"PROD_TYPE\" \"PROD_TYPE\",\"IV_CELL_MASTR_MERGE\".\"STYLE_GRP_ID\" \"STYLE_GRP_ID\"," +
				"\"IV_CELL_MASTR_MERGE\".\"MATRL_CATG\" \"MATRL_CATG\",\"IV_CELL_MASTR_MERGE\".\"PRICE_WT_MTHD\" \"PRICE_WT_MTHD\"," +
				"\"IV_CELL_MASTR_MERGE\".\"LOWER_BOUND\" \"LOWER_BOUND\",\"IV_CELL_MASTR_MERGE\".\"UPPER_BOUND\" \"UPPER_BOUND\"," +
				"\"IV_CELL_MASTR_MERGE\".\"PROC_IND\" \"PROC_IND\",\"IV_CELL_MASTR_MERGE\".\"CM_NBR\" \"CM_NBR\",\"IV_CELL_MASTR_MERGE\"." +
				"\"ALLOC_PROD_CLASS\" \"ALLOC_PROD_CLASS\",\"IV_CELL_MASTR_MERGE\".\"PRICE_WT_DESC\" \"PRICE_WT_DESC\",\"IV_CELL_MASTR_MERGE\"." +
				"\"STAT_CDE\" \"STAT_CDE\",\"IV_CELL_MASTR_MERGE\".\"ENTRY_DTE\" \"ENTRY_DTE\",\"IV_CELL_MASTR_MERGE\".\"ENTRY_USER_ID\" \"ENTRY_USER_ID\"," +
				"\"IV_CELL_MASTR_MERGE\".\"CM_DESC\" \"CM_DESC\",\"IV_CELL_MASTR_MERGE\".\"REGN\" \"REGN1\" " +
				"FROM \"SYS_IV\".\"IV_CELL_MASTR_MERGE\"@MISHK.WORLD \"IV_CELL_MASTR_MERGE\" " +
				"WHERE \"IV_CELL_MASTR_MERGE\".\"REGN\"='XX'";

		SqlUtils sqlUtils = new SqlUtils(sql);

		Map<String, Object> newData = sqlUtils.sqlQueryMap(data);

		assertThat(newData.size(), is(1));
		Map<String, Object> matchMap = new HashMap<String, Object>() {{
			put("REGN1", "XX");
		}};
		assertThat(newData, is(matchMap));
	}
}
