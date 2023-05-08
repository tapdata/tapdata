//package io.tapdata.connector.mysql;
//
//import io.tapdata.connector.tencent.db.mysql.MysqlJdbcContext;
//import io.tapdata.entity.logger.TapLog;
//import io.tapdata.entity.utils.DataMap;
//import io.tapdata.entity.utils.InstanceFactory;
//import io.tapdata.entity.utils.cache.KVMap;
//import io.tapdata.pdk.apis.context.TapConnectorContext;
//import io.tapdata.pdk.apis.spec.TapNodeSpecification;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//
///**
// * @author samuel
// * @Description
// * @create 2022-06-29 16:22
// **/
//public class StreamReadTest {
//	private static final String SERVER_NAME = "test";
//	private TapConnectorContext tapConnectorContext;
//	private MysqlJdbcContext mysqlJdbcContext;
//	private MysqlReader mysqlReader;
//
//	@BeforeEach
//	void beforeEach() {
//		DataMap connectionConfig = new DataMap();
//		connectionConfig.kv("host", "localhost");
//		connectionConfig.kv("port", 3307);
//		connectionConfig.kv("username", "root");
//		connectionConfig.kv("password", "tapdata");
//		connectionConfig.kv("database", "test");
//		TapNodeSpecification tapNodeSpecification = new TapNodeSpecification();
//		tapNodeSpecification.setId("mysql");
//		tapConnectorContext = new TapConnectorContext(tapNodeSpecification, connectionConfig, new DataMap(), new TapLog());
//		KVMap<Object> stateMap = InstanceFactory.instance(KVMap.class);
//		stateMap.init("streamTest", Object.class);
//		tapConnectorContext.setStateMap(stateMap);
//		mysqlJdbcContext = new MysqlJdbcContext(tapConnectorContext);
//		mysqlReader = new MysqlReader(mysqlJdbcContext);
//	}
//
//	/**
//	 * CREATE DATABASE if not exists `test`;
//	 * CREATE TABLE if not exists `test`.`test` (
//	 * `id` int,
//	 * `name` varchar(50) DEFAULT NULL
//	 * ) ENGINE=InnoDB;
//	 *
//	 * @throws Throwable
//	 */
//	@Test
//	void streamReadDDLTest1() throws Throwable {
////		String fieldName = "tapDDLTest";
////		String newFieldName = "tapDDLTestNew";
////		String[] ddlSql = new String[]{
////				"alter table test.test add column " + fieldName + " varchar(20) not null default 'tapdata' comment 'test'",
////				"alter table test.test change column " + fieldName + " " + newFieldName + " varchar(50) null default 'tapdata_new' comment 'test_new'",
////				"alter table test.test change " + newFieldName + " " + fieldName + " varchar(20) not null default 'tapdata' comment 'test'",
////				"alter table test.test rename column " + fieldName + " to " + newFieldName + "",
////				"alter table test.test modify column " + newFieldName + " varchar(50) null default 'tapdata_new' comment 'test_new'",
////				"alter table test.test rename column " + newFieldName + " to " + fieldName,
////				"alter table test.test modify " + fieldName + " varchar(20) not null default 'tapdata' comment 'test'",
////				"alter table test.test drop column " + fieldName,
////				"alter table test.test add " + fieldName + " varchar(20)",
////				"alter table test.test drop " + fieldName,
////		};
////		mysqlJdbcContext.execute("create database if not exists `test`");
////		mysqlJdbcContext.execute("drop table if exists `test`.`tapDDLTest`");
////		mysqlJdbcContext.execute("create table `test`.`tapDDLTest`(id int, name varchar(50))");
////		List<TapEvent> tapEventList = new ArrayList<>(ddlSql.length);
////		mysqlReader.readBinlog(tapConnectorContext, Collections.singletonList("test"), null, 1000,
////				StreamReadConsumer.create((tapEvents, o) -> {
////					System.out.println("Found ddl size: " + tapEvents.size());
////					tapEventList.addAll(tapEvents);
////					if (tapEventList.size() >= 12) {
////						mysqlReader.close();
////					}
////				}).stateListener((from, to) -> {
////					if (to.equals(StreamReadConsumer.STATE_STREAM_READ_STARTED)) {
////						try {
////							Thread.sleep(5000L);
////						} catch (InterruptedException ignored) {
////						}
////						System.out.println("Stream read started");
////						try {
////							for (int i = 0; i < ddlSql.length; i++) {
////								String ddl = ddlSql[i];
////								System.out.println((i + 1) + " ===>>> execute ddl: " + ddl);
////							}
////						} catch (Throwable e) {
////							e.printStackTrace();
////						}
////					}
////				})
////		);
////		Assertions.assertEquals(12, tapEventList.size());
////		mysqlJdbcContext.execute("drop table if exists `test`.`tapDDLTest`");
//	}
//}
