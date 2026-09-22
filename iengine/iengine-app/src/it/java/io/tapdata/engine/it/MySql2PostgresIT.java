package io.tapdata.engine.it;

import io.tapdata.dbforge.sdk.model.DbType;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.it.config.ConnectionConfigLoader;
import io.tapdata.it.schema.TestDataType;
import io.tapdata.it.schema.TestFieldSpec;
import io.tapdata.it.verifier.ConnectorVerifier;
import io.tapdata.it.verifier.JdbcVerifier;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/**
 * MySQL(源) → PostgreSQL(目标) 组合的引擎集成测试：纯配置具体类，参照实现。
 * <p>
 * 演示 {@code tapdata-wiki} 《引擎 IT 模板 · 组合扩展实战》给出的配方：继承 {@link EngineIT}
 * 只需实现源/目标连接规格、测试表字段规格与直连旁路验证器四个扩展点，全部通用集成用例
 * （引擎冒烟 / 生命周期 / 全量读 / 增量 / 目标写 / 断点续跑，声明在 {@link EngineIT}）经 JUnit 5
 * 继承在本类自动执行。
 * <p>
 * <b>与 {@link MySqlMongoIT} 的差异</b>（换目标库要动的点）：
 * <ul>
 *   <li>{@code targetSpec()} 用 {@link TaskFixture.ConnSpec#postgresql} —— 目标从 Mongo 换成 JDBC 库，
 *       {@code databaseType} 为连接器注册 id {@code "postgres"}（非配置组名 {@code "postgresql"}）；
 *       Postgres 库/模式分离，需额外下发 {@code schema}（缺省 {@code public}）。</li>
 *   <li>{@code directTargetVerifier()} 从 {@code MongoVerifier} 换成 {@link JdbcVerifier}
 *       （{@code postgres-connector} 继承 {@code CommonDbConnector}，含 {@code JdbcContext}，
 *       任务节点旁路验证器也由 {@code VerifierFactory} 自动装配为 JdbcVerifier）。</li>
 *   <li>{@code loadConnection()} 申请类型改为 {@code provision(DbType.MYSQL, DbType.POSTGRESQL)}。</li>
 * </ul>
 * 源端不变：仍是 MySQL，{@code testTableFields()} 用 MySQL 方言（目标表由引擎在全量迁移时自动建）。
 * <p>
 * <b>数据库来源（二选一）</b>：设置 {@code DBF_IT_ENDPOINT} 时经 {@link DbForgeProvisioner} 从 DBForge
 * 控制面动态申请 MySQL/PostgreSQL 租约、直连 {@code external_host:nodePort}（公网外运行设
 * {@code DBF_IT_HOST_MAPPING} 做 host 映射）；否则回退静态 {@code config/engine-connection.json}
 * 的 {@code mysql}/{@code postgresql} 组（可用 {@code IT_MYSQL_*}/{@code IT_PG_*} 环境变量覆盖）。
 * <p>
 * <b>跑通要点</b>：本组合的 CDC 源是 MySQL binlog，增量用例不依赖 Postgres 的逻辑解码——
 * {@code wal_level} 只影响以 Postgres 为 CDC <i>源</i> 的组合（如未来的 Postgres2XxxIT）；
 * DBForge 已把 Postgres 驱动改为 {@code wal_level=logical}（见 {@code dbforge/pkg/driver/postgresql/driver.go}）。
 * 真正决定 MySQL→Postgres 能否跑通的是三处异类 JDBC 目标前置（详见
 * {@code tapdata-wiki/06_研发流程/tapdata/集成测试用例规范/模板/引擎IT模板.md} 的
 * 「异类 JDBC 目标必踩的三个隐性前置」）：① {@code postgres-connector} 需装 <b>shaded fat jar</b> 进 {@code ~/.m2}
 * （thin jar 缺父类会 {@code Source not found}）；② 目标建表类型由 {@code TaskFixture} 自动从 connector
 * spec 的 {@code dataTypes} 注入 {@code DataSourceDefinitionDto.expression} 推导（否则空列 DDL）；
 * ③ MockTM 已容错目标端建表的表名回写端点（CDC 重启用例依赖）。三者已落代码，用例可直接跑。
 */
public class MySql2PostgresIT extends EngineIT {

	/** 本组合专用连接 id（24 位 hex），与 {@link MySqlMongoIT} 的 …0a/…0b 互不相同。
	 *  关键：目标端 …0b 在 Mongo 组合里是 mongodb、若本组合复用同 id 作 postgres，JVM 单例
	 *  引擎的连接/连接器缓存会把本组合目标任务解成错误的 Mongo 连接器（跨组合串扰）。 */
	private static final String SOURCE_CONN_ID = "00000000000000000000000c";
	private static final String TARGET_CONN_ID = "00000000000000000000000d";

	// ===================== 数据库环境（统一 JSON 配置） =====================

	/** 统一连接配置：按数据源分组（mysql/postgresql/...），来源见 {@link #loadConnection()}。 */
	private static final DataMap CONNECTION = loadConnection();

	/** MySQL 源库环境（JSON 为事实源；IT_MYSQL_* 为兼容旧版的高优先级覆盖） */
	protected static final String MYSQL_HOST = cfg("IT_MYSQL_HOST", "mysql.host", "127.0.0.1");
	protected static final int MYSQL_PORT = Integer.parseInt(cfg("IT_MYSQL_PORT", "mysql.port", "13306"));
	protected static final String MYSQL_USER = cfg("IT_MYSQL_USER", "mysql.user", "root");
	protected static final String MYSQL_PASSWORD = cfg("IT_MYSQL_PASSWORD", "mysql.password", "root");
	protected static final String MYSQL_DB = cfg("IT_MYSQL_DB", "mysql.database", "it_smoke_db");

	/** PostgreSQL 目标库环境（IT_PG_* 覆盖；schema 缺省 public） */
	protected static final String PG_HOST = cfg("IT_PG_HOST", "postgresql.host", "127.0.0.1");
	protected static final int PG_PORT = Integer.parseInt(cfg("IT_PG_PORT", "postgresql.port", "5432"));
	protected static final String PG_USER = cfg("IT_PG_USER", "postgresql.user", "postgres");
	protected static final String PG_PASSWORD = cfg("IT_PG_PASSWORD", "postgresql.password", "postgres");
	protected static final String PG_DB = cfg("IT_PG_DB", "postgresql.database", "TAPDATA");
	protected static final String PG_SCHEMA = cfg("IT_PG_SCHEMA", "postgresql.schema", "public");

	// ===================== EngineIT 扩展点实现 =====================

	/**
	 * 源库必须存在：mysql connector 连接 URL 含库名，库不存在时连接器 init 直接失败。
	 * 经 DBForge 动态申请时源库由 dbforge 建好（供应账号通常无建库权限），直接跳过；
	 * 仅回退静态配置（预置 MySQL 实例）时才 {@code CREATE DATABASE IF NOT EXISTS}。
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

	/** 源：MySQL 连接规格 */
	@Override
	protected TaskFixture.ConnSpec sourceSpec() {
		return TaskFixture.ConnSpec.mysql(SOURCE_CONN_ID, MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB);
	}

	/** 目标：PostgreSQL 连接规格（databaseType=postgres，含 schema） */
	@Override
	protected TaskFixture.ConnSpec targetSpec() {
		return TaskFixture.ConnSpec.postgresql(TARGET_CONN_ID, PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, PG_DB, PG_SCHEMA);
	}

	/** 测试表字段：id INT 自增主键 + name VARCHAR(255)（源端 MySQL 方言，用于旁路建源表） */
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

	/**
	 * 直连 PostgreSQL 目标库。currentSchema 指定与连接器一致的 schema，
	 * 令非限定的 {@code SELECT ... FROM <table>} 命中引擎建在 public 下的目标表。
	 * 依赖 org.postgresql JDBC 驱动在测试 classpath（iengine-app pom 以 test scope 引入）。
	 */
	@Override
	protected synchronized ConnectorVerifier directTargetVerifier() {
		if (directTarget == null) {
			String url = "jdbc:postgresql://" + PG_HOST + ":" + PG_PORT + "/" + PG_DB
					+ "?currentSchema=" + PG_SCHEMA;
			directTarget = new JdbcVerifier(new JdbcVerifier.DriverManagerDataSource(url, PG_USER, PG_PASSWORD));
		}
		return directTarget;
	}

	/**
	 * 加载统一连接配置（失败抛 IllegalStateException，避免静默用默认值掩盖配置问题）。
	 * 设置 {@code DBF_IT_ENDPOINT} 时走 {@link DbForgeProvisioner}：申请 MySQL/PostgreSQL 租约；
	 * 否则读取 classpath/文件系统的 {@code config/engine-connection.json}。
	 */
	private static DataMap loadConnection() {
		if (DbForgeProvisioner.enabled()) {
			// 声明本组合依赖的库：MySQL 源 + PostgreSQL 目标（组名即 mysql/postgresql，与静态 JSON 同构）。
			return DbForgeProvisioner.provision(DbType.MYSQL, DbType.POSTGRESQL);
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
}
