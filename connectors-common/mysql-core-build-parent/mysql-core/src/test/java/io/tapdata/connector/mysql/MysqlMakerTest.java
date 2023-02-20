package io.tapdata.connector.mysql;

import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.partition.TapPartitionFilter;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author samuel
 * @Description
 * @create 2022-12-22 16:13
 **/
public class MysqlMakerTest {
	@Test
	public void selectSqlTest() throws Throwable {
		TapPartitionFilter tapPartitionFilter = TapPartitionFilter.create()
				.leftBoundary(QueryOperator.gt("id", 1))
				.rightBoundary(QueryOperator.lte("id", null))
				.match("gender", "m");
		DataMap connConfig = DataMap.create();
		connConfig.put("database", "test");
		TapConnectorContext tapConnectorContext = new TapConnectorContext(new TapNodeSpecification(), connConfig, DataMap.create(), new TapLog());
		TapTable tapTable = new TapTable("USERS");
		MysqlMaker mysqlMaker = new MysqlMaker();
		String sql = mysqlMaker.selectSql(tapConnectorContext, tapTable, tapPartitionFilter);
		Assertions.assertEquals("SELECT * FROM `test`.`USERS` WHERE `id`>1 AND `id`<=null AND `gender`<=>'m'", sql);
	}
}
