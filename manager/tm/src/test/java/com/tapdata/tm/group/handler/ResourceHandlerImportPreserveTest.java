package com.tapdata.tm.group.handler;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 导入侧：包里缺敏感字段时，绝不用空值覆盖目标环境已有的配置（ADR-0034 D5/D6/D7）。
 *
 * 反面教材就是这条链路今天的行为：导出无条件脱敏 → 包里 uri 被抹空 → GROUP_IMPORT 走
 * handleGroupImportConnection → importSave 整文档覆盖 → 目标连接的 uri 变成空，
 * 导入还报成功。实撞记录：`Create MongoDB client failed, error: uri is blank`。
 */
@DisplayName("ResourceHandler import-side secret preservation")
public class ResourceHandlerImportPreserveTest {

    /** definition.properties.connection.properties.{uri -> apiServerKey=database_uri} */
    private DataSourceDefinitionDto definitionWithUri() {
        Map<String, Object> uriMeta = new LinkedHashMap<>();
        uriMeta.put("apiServerKey", "database_uri");
        Map<String, Object> connProps = new LinkedHashMap<>();
        connProps.put("uri", uriMeta);
        Map<String, Object> connection = new LinkedHashMap<>();
        connection.put("properties", connProps);
        LinkedHashMap<String, Object> properties = new LinkedHashMap<>();
        properties.put("connection", connection);

        DataSourceDefinitionDto definition = new DataSourceDefinitionDto();
        definition.setProperties(properties);
        return definition;
    }

    private DataSourceConnectionDto connection(Map<String, Object> config) {
        DataSourceConnectionDto conn = new DataSourceConnectionDto();
        conn.setName("MDM_CUSTOMER");
        conn.setConfig(config);
        return conn;
    }

    @Test
    @DisplayName("包里 uri 缺失时保留目标既有 uri，并报告该字段")
    void missingSensitiveField_keepsExistingValueAndReportsIt() {
        DataSourceConnectionDto incoming = connection(new LinkedHashMap<>(Map.of("isUri", true)));
        DataSourceConnectionDto existing = connection(new LinkedHashMap<>(Map.of(
                "isUri", true,
                "uri", "mongodb://real-host:27017/dmp")));

        List<String> preserved = ResourceHandler.restoreMissingSecretsFromExisting(
                incoming, existing, definitionWithUri());

        assertEquals("mongodb://real-host:27017/dmp", incoming.getConfig().get("uri"),
                "目标既有 uri 必须被保留——包里那个空缺是脱敏的产物，不是用户的配置");
        assertEquals(List.of("uri"), preserved,
                "被保留的字段必须报出来，否则就是静默（ADR-0034 D7）");
    }

    /**
     * ES-2b：导出把 config 与顶层镜像**两处**一起抹了，导入就必须两处一起补回来——
     * 只补 config 的话，等于把「凭据被抹空」从 config 挪到了顶层，坑还在。
     */
    @Test
    @DisplayName("顶层镜像字段（database_uri / password 等）同样保留目标既有值并报出来")
    void missingMirroredFields_keepExistingValuesAndAreReported() {
        DataSourceConnectionDto incoming = connection(new LinkedHashMap<>(Map.of("isUri", true)));
        DataSourceConnectionDto existing = connection(new LinkedHashMap<>(Map.of("isUri", true)));
        existing.setDatabase_uri("mongodb://real-host:27017/dmp");
        existing.setDatabase_password("s3cr3t");
        existing.setDatabase_port(27017);

        List<String> preserved = ResourceHandler.restoreMissingSecretsFromExisting(
                incoming, existing, definitionWithUri());

        assertEquals("mongodb://real-host:27017/dmp", incoming.getDatabase_uri());
        assertEquals("s3cr3t", incoming.getDatabase_password());
        assertEquals(27017, incoming.getDatabase_port());
        assertTrue(preserved.containsAll(List.of("database_uri", "database_password", "database_port")),
                "三个被保留的顶层字段都要报出来，实际：" + preserved);
    }

    @Test
    @DisplayName("包里带了值就不动它——那是用户要写入的新凭据，不是脱敏留下的洞")
    void presentValuesAreNotOverwritten() {
        DataSourceConnectionDto incoming = connection(new LinkedHashMap<>(Map.of("isUri", true)));
        incoming.setDatabase_uri("mongodb://new-host:27017/dmp");
        DataSourceConnectionDto existing = connection(new LinkedHashMap<>(Map.of("isUri", true)));
        existing.setDatabase_uri("mongodb://real-host:27017/dmp");

        List<String> preserved = ResourceHandler.restoreMissingSecretsFromExisting(
                incoming, existing, definitionWithUri());

        assertEquals("mongodb://new-host:27017/dmp", incoming.getDatabase_uri(),
                "包里给了值就是用户的意图，保护不能反过来把新凭据顶掉");
        assertTrue(preserved.isEmpty(), "什么都没保留就不该报，否则报告全是噪声：" + preserved);
    }

    // =====================================================================
    // [ADR-0036] D10 第二行：DSN 没写库名 ⇒ 目标环境既有库名不被覆盖。
    //
    // ⚠ 这一组的对照必须是 incoming(包/源环境) vs existing(目标环境) 两个不同对象。
    // 老用例 ResourceHandlerVaultTest#d10_missingDatabaseNameWarnsAndKeepsOld 正是
    // 栽在这里：它把 "olddb" 塞在 **incoming** 上、断言 inject 没动它，然后把结论写成
    // 「保留目标既有库名」。inject 本来就不会动它（injectParsedField 遇 null 直接 no-op），
    // 所以那条断言恒真、与目标环境毫无关系——覆盖发生在 GROUP_IMPORT 整文档替换那一层，
    // 而 incoming 携带的恰恰是**源环境**的库名。一条绿着的用例把这个缺陷盖了下去。
    // =====================================================================

    /** JDBC 型 definition：库名是独立字段 database_name。 */
    private DataSourceDefinitionDto definitionWithDatabaseName() {
        Map<String, Object> dbMeta = new LinkedHashMap<>();
        dbMeta.put("apiServerKey", "database_name");
        Map<String, Object> hostMeta = new LinkedHashMap<>();
        hostMeta.put("apiServerKey", "database_host");
        Map<String, Object> connProps = new LinkedHashMap<>();
        connProps.put("database", dbMeta);
        connProps.put("host", hostMeta);
        Map<String, Object> connection = new LinkedHashMap<>();
        connection.put("properties", connProps);
        LinkedHashMap<String, Object> properties = new LinkedHashMap<>();
        properties.put("connection", connection);

        DataSourceDefinitionDto definition = new DataSourceDefinitionDto();
        definition.setProperties(properties);
        return definition;
    }

    private DataSourceConnectionDto named(String name, Map<String, Object> config) {
        DataSourceConnectionDto conn = new DataSourceConnectionDto();
        conn.setName(name);
        conn.setConfig(config);
        return conn;
    }

    @Test
    @DisplayName("JDBC：DSN 漏写库名 ⇒ 目标既有库名压过包里的源环境库名")
    void jdbc_dsnOmitsDatabase_targetNameWins() {
        // 包来自 SIT，带着 sit_orders；目标 PROD 上这条连接已有 prod_orders。
        DataSourceConnectionDto incoming = named("ORDERS_PG",
                new LinkedHashMap<>(Map.of("database", "sit_orders", "host", "pg.prod.internal")));
        incoming.setDatabase_name("sit_orders");
        DataSourceConnectionDto existing = named("ORDERS_PG",
                new LinkedHashMap<>(Map.of("database", "prod_orders", "host", "pg.prod.internal")));
        existing.setDatabase_name("prod_orders");

        Map<String, String> vault = new LinkedHashMap<>();
        vault.put("ORDERS_PG_DSN", "tapuser@pg.prod.internal:5432");   // 漏了 /prod_orders
        vault.put("ORDERS_PG_PASSWORD", "pw");

        String preserved = ResourceHandler.restoreDatabaseNameWhenDsnOmitsIt(
                incoming, existing, definitionWithDatabaseName(), vault);

        assertEquals("prod_orders", incoming.getConfig().get("database"),
                "DSN 没写库名时，落地的必须是目标既有库名，而不是包里那个源环境的");
        assertEquals("prod_orders", incoming.getDatabase_name(),
                "顶层镜像要一起改——MetaDataBuilderUtils 建限定名读的是顶层，只改 config 会得到半改状态");
        assertEquals("prod_orders", preserved, "保留了什么必须报出来（[ADR-0034] D7 绝不静默）");
    }

    @Test
    @DisplayName("JDBC：DSN 写了库名 ⇒ DSN 赢，目标既有库名不得反压回来")
    void jdbc_dsnCarriesDatabase_dsnWins() {
        DataSourceConnectionDto incoming = named("ORDERS_PG",
                new LinkedHashMap<>(Map.of("database", "newdb")));
        incoming.setDatabase_name("newdb");
        DataSourceConnectionDto existing = named("ORDERS_PG",
                new LinkedHashMap<>(Map.of("database", "prod_orders")));
        existing.setDatabase_name("prod_orders");

        Map<String, String> vault = new LinkedHashMap<>();
        vault.put("ORDERS_PG_DSN", "tapuser@pg.prod.internal:5432/newdb");

        String preserved = ResourceHandler.restoreDatabaseNameWhenDsnOmitsIt(
                incoming, existing, definitionWithDatabaseName(), vault);

        assertEquals("newdb", incoming.getConfig().get("database"),
                "「库名可逐环境不同」是本期的核心能力——保护绝不能把它顶掉");
        assertNull(preserved, "什么都没保留就不该报");
    }

    @Test
    @DisplayName("非格式 3（没有 _DSN 键）⇒ 一个字段都不碰")
    void notFormat3_untouched() {
        DataSourceConnectionDto incoming = named("ORDERS_PG",
                new LinkedHashMap<>(Map.of("database", "sit_orders")));
        DataSourceConnectionDto existing = named("ORDERS_PG",
                new LinkedHashMap<>(Map.of("database", "prod_orders")));

        Map<String, String> vault = new LinkedHashMap<>();
        vault.put("ORDERS_PG_URL", "pg.prod.internal:5432");   // 格式 2
        vault.put("ORDERS_PG_PASSWORD", "pw");

        String preserved = ResourceHandler.restoreDatabaseNameWhenDsnOmitsIt(
                incoming, existing, definitionWithDatabaseName(), vault);

        assertEquals("sit_orders", incoming.getConfig().get("database"),
                "[ADR-0036] D5 推论：库名只被格式 3 覆盖，格式 1/2 的 database_name 不被新逻辑碰到");
        assertNull(preserved);
    }

    @Test
    @DisplayName("MongoDB：DSN 漏写库名 ⇒ 目标既有库名拼回 uri，query 串与种子列表保真")
    void mongo_dsnOmitsDatabase_targetNameSplicedBack() {
        DataSourceConnectionDto incoming = named("ORDERS_MONGO", new LinkedHashMap<>(Map.of(
                "uri", "mongodb://tapuser:pw@h1:27017,h2:27017/?replicaSet=rs0")));
        incoming.setDatabase_uri("mongodb://tapuser:pw@h1:27017,h2:27017/?replicaSet=rs0");
        DataSourceConnectionDto existing = named("ORDERS_MONGO", new LinkedHashMap<>(Map.of(
                "uri", "mongodb://tapuser:oldpw@h1:27017/prod_orders")));
        existing.setDatabase_uri("mongodb://tapuser:oldpw@h1:27017/prod_orders");

        Map<String, String> vault = new LinkedHashMap<>();
        vault.put("ORDERS_MONGO_DSN", "mongodb://tapuser:@h1:27017,h2:27017/?replicaSet=rs0");
        vault.put("ORDERS_MONGO_PASSWORD", "pw");

        String preserved = ResourceHandler.restoreDatabaseNameWhenDsnOmitsIt(
                incoming, existing, definitionWithUri(), vault);

        String uri = (String) incoming.getConfig().get("uri");
        assertEquals("mongodb://tapuser:pw@h1:27017,h2:27017/prod_orders?replicaSet=rs0", uri,
                "库名要拼进 path 段，且种子列表与 ?replicaSet= 必须原样保真");
        assertEquals(uri, incoming.getDatabase_uri(), "顶层镜像与 config 必须一致");
        assertEquals("prod_orders", preserved);
    }

    @Test
    @DisplayName("MongoDB：DSN 写了库名 ⇒ DSN 赢")
    void mongo_dsnCarriesDatabase_dsnWins() {
        DataSourceConnectionDto incoming = named("ORDERS_MONGO", new LinkedHashMap<>(Map.of(
                "uri", "mongodb://tapuser:pw@h1:27017/newdb")));
        incoming.setDatabase_uri("mongodb://tapuser:pw@h1:27017/newdb");
        DataSourceConnectionDto existing = named("ORDERS_MONGO", new LinkedHashMap<>(Map.of(
                "uri", "mongodb://tapuser:oldpw@h1:27017/prod_orders")));

        Map<String, String> vault = new LinkedHashMap<>();
        vault.put("ORDERS_MONGO_DSN", "mongodb://tapuser:@h1:27017/newdb");

        String preserved = ResourceHandler.restoreDatabaseNameWhenDsnOmitsIt(
                incoming, existing, definitionWithUri(), vault);

        assertEquals("mongodb://tapuser:pw@h1:27017/newdb", incoming.getConfig().get("uri"));
        assertNull(preserved);
    }

    @Test
    @DisplayName("目标环境也没有库名 ⇒ 不写空值，安静放过")
    void targetHasNoDatabaseEither_noop() {
        DataSourceConnectionDto incoming = named("ORDERS_PG",
                new LinkedHashMap<>(Map.of("database", "sit_orders")));
        DataSourceConnectionDto existing = named("ORDERS_PG", new LinkedHashMap<>());

        Map<String, String> vault = new LinkedHashMap<>();
        vault.put("ORDERS_PG_DSN", "tapuser@pg.prod.internal:5432");

        String preserved = ResourceHandler.restoreDatabaseNameWhenDsnOmitsIt(
                incoming, existing, definitionWithDatabaseName(), vault);

        assertEquals("sit_orders", incoming.getConfig().get("database"),
                "目标没有可保留的值时，绝不能反过来把包里那个抹空");
        assertNull(preserved);
    }

    @Test
    @DisplayName("目标环境还没有这条连接（首次部署）⇒ 不报错，包里的库名照用")
    void noExistingConnection_noop() {
        DataSourceConnectionDto incoming = named("ORDERS_PG",
                new LinkedHashMap<>(Map.of("database", "sit_orders")));

        Map<String, String> vault = new LinkedHashMap<>();
        vault.put("ORDERS_PG_DSN", "tapuser@pg.prod.internal:5432");

        String preserved = ResourceHandler.restoreDatabaseNameWhenDsnOmitsIt(
                incoming, null, definitionWithDatabaseName(), vault);

        assertEquals("sit_orders", incoming.getConfig().get("database"));
        assertNull(preserved);
    }
}
