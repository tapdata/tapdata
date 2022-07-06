package io.tapdata.connector.mysql;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.engine.DebeziumEngine;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;

import static io.tapdata.connector.mysql.util.MysqlUtil.randomServerId;

/**
 * @author samuel
 * @Description
 * @create 2022-06-29 16:22
 **/
public class StreamReadTest {
	private static final String SERVER_NAME = "test";

	public static void main(String[] args) {
		Configuration.Builder builder = Configuration.create()
				.with("name", SERVER_NAME)
				.with("connector.class", "io.debezium.connector.mysql.MySqlConnector")
				.with("database.hostname", "127.0.0.1")
				.with("database.port", 3307)
				.with("database.user", "root")
				.with("database.password", "tapdata")
				.with("database.server.name", SERVER_NAME)
				.with("threadName", "Debezium-Mysql-Connector-" + SERVER_NAME)
				.with("database.history.skip.unparseable.ddl", true)
				.with("database.history.store.only.monitored.tables.ddl", true)
				.with("database.history.store.only.captured.tables.ddl", true)
				.with(MySqlConnectorConfig.SNAPSHOT_LOCKING_MODE, MySqlConnectorConfig.SnapshotLockingMode.NONE)
				.with("max.queue.size", 1000 * 8)
				.with("max.batch.size", 1000)
				.with(MySqlConnectorConfig.SERVER_ID, randomServerId())
				.with("time.precision.mode", "adaptive_time_microseconds");
		builder.with(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, "INSURANCE");
		builder.with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, "INSURANCE.DDL_TEST");
		builder.with("snapshot.mode", "SCHEMA_ONLY_RECOVERY");
		builder.with(MySqlConnectorConfig.DATABASE_HISTORY, "io.tapdata.connector.mysql.StateMapHistoryBackingStore");
		builder.with(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, "offset");
		Configuration configuration = builder.build();
		StringBuilder configStr = new StringBuilder("Starting binlog reader with config {\n");
		configuration.withMaskedPasswords().asMap().forEach((k, v) -> configStr.append("  ")
				.append(k)
				.append(": ")
				.append(v)
				.append("\n"));
		configStr.append("}");
		System.out.println(configStr);
		EmbeddedEngine embeddedEngine = null;
		try {
			embeddedEngine = (EmbeddedEngine) new EmbeddedEngine.BuilderImpl()
					.using(configuration)
					.notifying(StreamReadTest::sourceRecordConsumer)
					.using(new DebeziumEngine.ConnectorCallback() {
						@Override
						public void taskStarted() {
							System.out.println("CDC engine started");
						}
					})
					.using((result, message, throwable) -> {
						if (result) {
							if (StringUtils.isNotBlank(message)) {
								System.out.println("CDC engine stopped: " + message);
							} else {
								System.out.println("CDC engine stopped");
							}
						} else {
							if (null != throwable) {
								if (StringUtils.isNotBlank(message)) {
									System.err.println(message);
									throwable.printStackTrace();
								} else {
									throwable.printStackTrace();
								}
							} else {
								System.err.println(message);
							}
						}
					})
					.build();
			embeddedEngine.run();
		} finally {
			if (null != embeddedEngine) {
				try {
					embeddedEngine.close();
				} catch (IOException ignore) {
				}
			}
		}
	}

	private static void sourceRecordConsumer(SourceRecord record) {
		if (null != record.valueSchema().field("ddl")) {
			String ddl = ((Struct) record.value()).getString("ddl");
			try {
				Statement statement = CCJSqlParserUtil.parse(ddl);
				System.out.println(statement);
			} catch (JSQLParserException e) {
				e.printStackTrace();
			}
		}
	}
}
