package io.tapdata.ddl;

import com.tapdata.entity.DatabaseTypeEnum;
import io.tapdata.ddlp.DDLEvent;
import io.tapdata.ddlp.DDLProcessor;
import io.tapdata.ddlp.converters.DDLConverter;
import io.tapdata.ddlp.exception.DDLException;
import io.tapdata.ddlp.parsers.DDLParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 * DDL测试
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/9/11 下午3:08 Create
 * @since JDK1.8
 */
public class DDLTester {

	private Logger logger = LogManager.getLogger(getClass());
	private boolean INTERRUPT_OF_FAILED = false;

	private String[] MYSQL_TEST_DDL_ARRAY = new String[]{
			"create table `db`.`test`(`id` int primary key, `name` varchar(64) not null)"
			, "drop table `db`.`test`"
			, "alter table `db`.`test` rename to `test1`"
			, "alter table `db`.`test` add column `name` varchar(32) null after `id`"
			, "alter table `db`.`test` rename `name` to `alias`"
			, "alter table `db`.`test` rename column `name` to `alias`"
			, "alter table `db`.`test` drop column `name`"
//    , "alter table `db`.`test` modify column `name` `alias` varchar(32) null after `id`"
	};
	private String[] MSSQL_TEST_DDL_ARRAY = new String[]{
			"drop table [cdc].[dbo_HSTEST_1207_INDEX_1639110539318_CT]"
			, "ALTER TABLE [dbo].[HSTEST_1207_INDEX] DROP COLUMN [ADD]"
			, "ALTER TABLE [dbo].[HSTEST_1207_INDEX] ADD [F1] varchar(255) NULL"
			, "ALTER TABLE [dbo].[HSTEST_1207_INDEX] ADD [F4] varchar(10) NOT NULL"
			, "ALTER TABLE [dbo].[HSTEST_1207_INDEX] ALTER COLUMN [F3] datetime2(6) NOT NULL"
			, "ALTER TABLE [dbo].[HSTEST_1207_INDEX] ALTER COLUMN [F2] varchar(3) COLLATE Chinese_PRC_CI_AS NULL"
			, "ALTER TABLE [dbo].[HSTEST_1207_INDEX] ADD CONSTRAINT [fk_hstest_1207_index] FOREIGN KEY ([SEQ]) REFERENCES [dbo].[HSTEST_1207_INDEX] ([ID])"
			, "alter table [hstest0208_alter_field_default] add default 0 for age"
			, "alter table [hstest0208_alter_field_default] add default '[ '' \"\" 1_23 ]' for \"name\""
			, "alter table test_100_4\n\tadd test int"
			, "alter table test_100_1 drop column f1"
			, "CREATE TABLE [dbo].[usrXYPJ](\n [ID] [bigint] NOT NULL,\n [INBBM] [int] NULL,\n [IGSDM] [int] NULL,\n [XXFBRQ] [datetime] NULL,\n [PJLB] [int] NULL,\n [PJJG] [varchar](200) NULL,\n [QYBH] [int] NULL,\n [PJRQ] [datetime] NULL,\n [PJMS] [varchar](50) NULL,\n [XYJB] [int] NULL,\n [GKBZ] [tinyint] NOT NULL,\n [XGRY] [int] NOT NULL,\n [XGRY2] [int] NOT NULL,\n [XGSJ] [datetime] NOT NULL,\n [FBSJ] [datetime] NULL,\n [SHRY] [int] NULL,\n [JSID] [bigint] NOT NULL,\n [XYDJBZ] [int] NULL,\n [PJTX] [int] NULL,\n [XXLB] [int] NULL,\n [QYMC] [varchar](200) NULL,\n [BH] [int] NULL,\n [XXLY] [varchar](200) NULL,\n [ZTMS] [varchar](500) NULL,\n [PJZT] [int] NULL,\n [PJZW] [varchar](100) NULL,\n [XXLYLB] [int] NULL,\n [PJZWLB] [int] NULL,\n [PJRQDY] [int] NULL,\n [PJYY] [int] NULL,\n [BZ] [varchar](1000) NULL,\n CONSTRAINT [pk_usrXYPJ] PRIMARY KEY NONCLUSTERED \n(\n [ID] ASC\n)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]\n) ON [PRIMARY]"
	};

	@Test
	public void mysql2Mssql() {
		testCase("Test mysql 2 mssql", DatabaseTypeEnum.MYSQL, DatabaseTypeEnum.MSSQL, MYSQL_TEST_DDL_ARRAY);
	}

	@Test
	public void mssql2Mysql() {
		testCase("Test mssql 2 mysql", DatabaseTypeEnum.MSSQL, DatabaseTypeEnum.MYSQL, MSSQL_TEST_DDL_ARRAY);
		testCase("Test mssql 2 mssql", DatabaseTypeEnum.MSSQL, DatabaseTypeEnum.MSSQL, MSSQL_TEST_DDL_ARRAY);
	}

	private <I, O, E extends DDLEvent> void testCase(String tag, DatabaseTypeEnum sourceType, DatabaseTypeEnum targetType, I... ddlArr) {
		boolean isOk = true;
		logger.info(tag + " begin.");
		DDLParser<I, E> parser = DDLProcessor.getParser(sourceType);
		DDLConverter<E, O> converter = DDLProcessor.getConverter(targetType);
		for (I ddl : ddlArr) {
			if (INTERRUPT_OF_FAILED && !isOk) break;
			isOk = parser.ddl2Event(ddl, (e) -> converter.event2ddl(e, (sql) -> {
				logger.info("[OK]：" + sql);
			}, (errEvent, err) -> {
				if (err instanceof DDLException) {
					logger.warn(err.getMessage());
					return true;
				}
				logger.error(err.getMessage()); // convert failed log
				return false;
			}), (errDDL, err) -> {
				if (err instanceof DDLException) {
					logger.warn(errDDL, err);
				} else {
					logger.error(errDDL, err); // parse failed log
				}
				return false;
			});
		}
		Assert.assertTrue(tag, isOk);
	}
}
