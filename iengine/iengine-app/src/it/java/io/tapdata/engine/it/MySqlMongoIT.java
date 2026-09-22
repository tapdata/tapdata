package io.tapdata.engine.it;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.tapdata.constant.StringUtil;
import io.tapdata.dbforge.sdk.model.DbType;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.it.config.ConnectionConfigLoader;
import io.tapdata.it.schema.TestDataType;
import io.tapdata.it.schema.TestFieldSpec;
import io.tapdata.it.verifier.ConnectorVerifier;
import io.tapdata.it.verifier.JdbcVerifier;
import io.tapdata.it.verifier.MongoVerifier;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/**
 * MySQL(源) → MongoDB(目标) 组合的引擎集成测试：纯配置具体类。
 * <p>
 * 实现 {@link EngineIT} 的扩展点：动态提供源/目标连接器连接规格与测试表字段规格；
 * 数据库环境支持环境变量覆盖，便于指向不同环境。全部通用集成用例（引擎冒烟/
 * 生命周期/全量读/增量/目标写/断点续跑，声明在 {@link EngineIT}）经 JUnit 继承在本类自动执行。
 * <p>
 * <b>数据库来源（二选一）</b>：默认经 {@link DbForgeProvisioner} 从 DBForge 控制面<b>动态申请</b>
 * MySQL/MongoDB 租约，并直连租约返回的 {@code external_host:nodePort}（连接器/验证器按普通 host/port
 * 直连，要求运行测试的 JVM 能路由到该地址；公网外运行设 {@code DBF_IT_HOST_MAPPING} 把内网 host 映射为
 * 公网 host，详见 {@link DbForgeProvisioner}）；仅当设置了 {@code DBF_IT_ENDPOINT} 时启用。未启用时回退到
 * 静态 {@code config/engine-connection.json}（可用 {@code IT_MYSQL_*}/{@code IT_MONGO_*} 环境变量覆盖）。
 * <p>
 * 扩展新连接器组合（如 PostgreSQL → Doris）：仿照本类新增配置具体类即可，
 * {@link EngineIT} 通用用例直接生效；本组合特有的用例（如仅 MySQL 源或仅 MongoDB 目标
 * 才成立的场景）在继承本类的单独类中编写。
 */
public class MySqlMongoIT extends EngineIT {

	/** 本组合专用连接 id（24 位 hex）。每个组合用互不相同的 id，避免 JVM 单例引擎的连接/连接器
	 *  缓存跨组合串扰（见 {@link EngineIT} 注释）；此对值为本组合历史值，保持不变。 */
	private static final String SOURCE_CONN_ID = "00000000000000000000000a";
	private static final String TARGET_CONN_ID = "00000000000000000000000b";

	// ===================== 数据库环境（统一 JSON 配置） =====================

	/**
	 * 统一连接配置：按数据源分组（mysql/mongodb/...），来源见 {@link #loadConnection()}——
	 * 设置 {@code DBF_IT_ENDPOINT} 时经 {@link DbForgeProvisioner} 从 DBForge 控制面动态申请、直连
	 * 租约返回的 {@code external_host:nodePort}；否则读取 src/it/resources/config/engine-connection.json（classpath 优先，
	 * 其次文件系统）。两条路径产出同构 JSON，故下方 {@link #cfg} 的取值逻辑完全一致
	 * （{@code IT_MYSQL_*}/{@code IT_MONGO_*} 兼容旧版的高优先级覆盖仍生效）。
	 */
	private static final DataMap CONNECTION = loadConnection();

	/** MySQL 源库环境（JSON 为事实源；IT_MYSQL_* 为兼容旧版的高优先级覆盖） */
	protected static final String MYSQL_HOST = cfg("IT_MYSQL_HOST", "mysql.host", "127.0.0.1");
	protected static final int MYSQL_PORT = Integer.parseInt(cfg("IT_MYSQL_PORT", "mysql.port", "13306"));
	protected static final String MYSQL_USER = cfg("IT_MYSQL_USER", "mysql.user", "root");
	protected static final String MYSQL_PASSWORD = cfg("IT_MYSQL_PASSWORD", "mysql.password", "root");
	protected static final String MYSQL_DB = cfg("IT_MYSQL_DB", "mysql.database", "it_smoke_db");

	/** MongoDB 目标库环境 */
	protected static final String MONGO_HOST = cfg("IT_MONGO_HOST", "mongodb.host", "127.0.0.1");
	protected static final int MONGO_PORT = Integer.parseInt(cfg("IT_MONGO_PORT", "mongodb.port", "27017"));
	protected static final String MONGO_USER = cfg("IT_MONGO_USER", "mongodb.user", "");
	protected static final String MONGO_PASSWORD = cfg("IT_MONGO_PASSWORD", "mongodb.password", "");
	protected static final String MONGO_DB = cfg("IT_MONGO_DB", "mongodb.database", "it_smoke_db");

	// ===================== EngineIT 扩展点实现 =====================

	/**
	 * 源库必须存在：mysql connector 连接 URL 含库名，库不存在时连接器 init 直接失败。
	 * 环境预创建仅限本 MySQL 组合层（用例层仍不直连特定数据源）。
	 * <p>
	 * 经 DBForge 动态申请时，源库由 dbforge 建好并随租约返回（且供应账号通常无全局建库权限），
	 * 故直接跳过建库；仅在回退到静态配置（预置 MySQL 实例）时才 {@code CREATE DATABASE IF NOT EXISTS}。
	 */
	@Override
	protected void prepareEnvironment() throws Exception {
		if (DbForgeProvisioner.enabled()) {
			return;
		}
		String url = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT
				+ "/?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
		try (Connection conn = DriverManager.getConnection(url, MYSQL_USER, MYSQL_PASSWORD);
				Statement stmt = conn.createStatement()) {
			stmt.execute("CREATE DATABASE IF NOT EXISTS " + MYSQL_DB);
		}
	}

	/** 源：MySQL 连接规格（config key 与 mysql connector 的 connection 组一致） */
	@Override
	protected TaskFixture.ConnSpec sourceSpec() {
		return TaskFixture.ConnSpec.mysql(SOURCE_CONN_ID, MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB);
	}

	/** 目标：MongoDB 连接规格（config key 与 mongodb connector 的 MongodbConfig 一致） */
	@Override
	protected TaskFixture.ConnSpec targetSpec() {
		return TaskFixture.ConnSpec.mongodb(TARGET_CONN_ID, MONGO_HOST, MONGO_PORT, MONGO_USER, MONGO_PASSWORD, MONGO_DB);
	}

	/** 测试表字段：id INT 自增主键 + name VARCHAR(255)（MySQL 方言，直接拼入旁路建表） */
	@Override
	protected List<TestFieldSpec> testTableFields() {
		return Arrays.asList(
				TestFieldSpec.builder().name("id").dataType("INT AUTO_INCREMENT")
						.testDataType(TestDataType.INT).primaryKey(true).autoInc(true).nullable(false).build(),
				TestFieldSpec.builder().name("name").dataType("VARCHAR(255)")
						.testDataType(TestDataType.VARCHAR).build()
		);
	}

	/** JVM 级共享直连验证器（不依赖引擎连接器生命周期，供任务完成后断言） */
	private static ConnectorVerifier directSource;
	private static ConnectorVerifier directTarget;

	/** 直连 MySQL 源库（DriverManager 逐次建连，无连接池） */
	@Override
	protected synchronized ConnectorVerifier directSourceVerifier() {
		if (directSource == null) {
			String url = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DB
					+ "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
			directSource = new JdbcVerifier(new JdbcVerifier.DriverManagerDataSource(url, MYSQL_USER, MYSQL_PASSWORD));
		}
		return directSource;
	}

	/** 直连 MongoDB 目标库（测试 classpath 的驱动，经 spring-boot-starter-data-mongodb 传递） */
	@Override
	protected synchronized ConnectorVerifier directTargetVerifier() {
		if (directTarget == null) {
			// 统一走 TaskFixture.mongoUri：带账号时内嵌凭证 + authSource + directConnection（与连接器建连一致）
			directTarget = new MongoVerifier(
					MongoClients.create(TaskFixture.mongoUri(MONGO_HOST, MONGO_PORT, MONGO_USER, MONGO_PASSWORD, MONGO_DB)),
					MONGO_DB);
		}
		return directTarget;
	}

	/**
	 * 加载统一连接配置（失败时抛 IllegalStateException，避免静默使用默认值掩盖配置问题）。
	 * <p>
	 * 设置 {@code DBF_IT_ENDPOINT} 时走 {@link DbForgeProvisioner}：申请 MySQL/MongoDB 租约、
	 * 直连租约返回的 {@code external_host:nodePort}，返回与静态 JSON 同构的连接配置；
	 * 否则读取 classpath/文件系统的 {@code config/engine-connection.json}（含外部接口与环境变量覆盖）。
	 */
	private static DataMap loadConnection() {
		if (DbForgeProvisioner.enabled()) {
			// 声明本组合依赖的库：MySQL 源 + MongoDB 目标（组名即 mysql/mongodb，与静态 JSON 同构）。
			// 换成其它组合只需改这里，如 provision(DbType.MYSQL, DbType.ORACLE)、provision(DbType.SQLSERVER, DbType.ORACLE)。
			return DbForgeProvisioner.provision(DbType.MYSQL, DbType.MONGODB);
		}
		try {
			return ConnectionConfigLoader.load("config/engine-connection.json");
		} catch (IOException e) {
			throw new IllegalStateException("Failed to load engine IT connection config: config/engine-connection.json", e);
		}
	}

	/**
	 * 取配置值：旧版 IT_* 环境变量优先（兼容既有 CI/本地脚本），
	 * 其次统一 JSON（点号路径），最后默认值。
	 */
	private static String cfg(String legacyEnv, String jsonPath, String defaultValue) {
		String legacy = System.getenv(legacyEnv);
		if (legacy != null && !legacy.isEmpty()) {
			return legacy;
		}
		String fromJson = ConnectionConfigLoader.getString(CONNECTION, jsonPath);
		return fromJson != null && !fromJson.isEmpty() ? fromJson : defaultValue;
	}

	@Test
	public void triggerIT() {
		int result = 10 * 5;
		Assertions.assertEquals(50, result);
	}
}
