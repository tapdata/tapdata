package com.tapdata.tm.group.handler;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ResourceHandler Vault Injection Tests")
public class ResourceHandlerVaultTest {

    @Nested
    @DisplayName("truncateName")
    class TruncateNameTest {

        @Test
        void twoUnderscores_returnsBeforeSecond() {
            assertEquals("TMH_PG", ResourceHandler.truncateName("TMH_PG_HPI"));
        }

        @Test
        void threeUnderscores_returnsBeforeSecond() {
            assertEquals("A_B", ResourceHandler.truncateName("A_B_C_D"));
        }

        @Test
        void oneUnderscore_returnsNull() {
            assertNull(ResourceHandler.truncateName("ABC_DEF"));
        }

        @Test
        void noUnderscore_returnsNull() {
            assertNull(ResourceHandler.truncateName("ABCDEF"));
        }

        @Test
        void null_returnsNull() {
            assertNull(ResourceHandler.truncateName(null));
        }
    }

    @Nested
    @DisplayName("injectVaultSecretsToConnection")
    class InjectTest {

        private DataSourceConnectionDto makeConn(String name) {
            DataSourceConnectionDto conn = new DataSourceConnectionDto();
            conn.setName(name);
            conn.setConfig(new LinkedHashMap<>());
            return conn;
        }

        @Test
        @DisplayName("优先级1：connectionName_uri 直接命中")
        void priority1_uri() {
            DataSourceConnectionDto conn = makeConn("MY_CONN");
            Map<String, String> vault = new LinkedHashMap<>();
            vault.put("MY_CONN_uri", "mongodb://user:pass@host1:27017/db");
            // Also put url/user/password to ensure uri takes precedence
            vault.put("MY_CONN_url", "host2:5432/user2");
            vault.put("MY_CONN_user", "user2");
            vault.put("MY_CONN_password", "pass2");

            // definition=null → no apiKeyToConfigPath, uri will be parsed
            // With database_uri not in schema, it would try to parse the URI
            // Since definition is null, no database_uri key → will call injectFromUriString
            ResourceHandler.injectVaultSecretsToConnection(conn, vault, null);

            Map<String, Object> config = conn.getConfig();
            // URI parsed → host, port, username injected
            assertEquals("host1", config.get("host"));
            assertEquals(27017, config.get("port"));
            assertEquals("user", config.get("username"));
        }

        @Test
        @DisplayName("优先级2：connectionName_url + _user + _password")
        void priority2_connectionName() {
            DataSourceConnectionDto conn = makeConn("MY_CONN");
            Map<String, String> vault = new LinkedHashMap<>();
            vault.put("MY_CONN_url", "dbhost:5432");
            vault.put("MY_CONN_user", "myuser");
            vault.put("MY_CONN_password", "mypass");

            ResourceHandler.injectVaultSecretsToConnection(conn, vault, null);

            Map<String, Object> config = conn.getConfig();
            assertEquals("mypass", config.get("password"));
            assertEquals("myuser", config.get("username"));
            assertEquals("dbhost", config.get("host"));
            assertEquals(5432, config.get("port"));
        }

        @Test
        @DisplayName("优先级3：截取连接名后查找")
        void priority3_truncatedName() {
            DataSourceConnectionDto conn = makeConn("TMH_PG_HPI");
            Map<String, String> vault = new LinkedHashMap<>();
            vault.put("TMH_PG_url", "pghost:5433");
            vault.put("TMH_PG_user", "pguser");
            vault.put("TMH_PG_password", "pgpass");

            ResourceHandler.injectVaultSecretsToConnection(conn, vault, null);

            Map<String, Object> config = conn.getConfig();
            assertEquals("pgpass", config.get("password"));
            assertEquals("pguser", config.get("username"));
            assertEquals("pghost", config.get("host"));
            assertEquals(5433, config.get("port"));
        }

        @Test
        @DisplayName("优先级4：default 前缀查找")
        void priority4_default() {
            DataSourceConnectionDto conn = makeConn("SOME_RANDOM_CONN");
            Map<String, String> vault = new LinkedHashMap<>();
            vault.put("default_url", "defaulthost:3306");
            vault.put("default_user", "defaultuser");
            vault.put("default_password", "defaultpass");

            ResourceHandler.injectVaultSecretsToConnection(conn, vault, null);

            Map<String, Object> config = conn.getConfig();
            assertEquals("defaultpass", config.get("password"));
            assertEquals("defaultuser", config.get("username"));
            assertEquals("defaulthost", config.get("host"));
            assertEquals(3306, config.get("port"));
        }

        @Test
        @DisplayName("优先级5：所有策略未命中，抛异常")
        void priority5_throwsException() {
            DataSourceConnectionDto conn = makeConn("MY_CONN");
            Map<String, String> vault = new LinkedHashMap<>();
            vault.put("OTHER_url", "host:1234");
            vault.put("OTHER_user", "user");
            vault.put("OTHER_password", "pass");

            assertThrows(IllegalArgumentException.class,
                    () -> ResourceHandler.injectVaultSecretsToConnection(conn, vault, null));
        }

        @Test
        @DisplayName("大小写不敏感匹配")
        void caseInsensitive() {
            DataSourceConnectionDto conn = makeConn("MY_CONN");
            Map<String, String> vault = new LinkedHashMap<>();
            vault.put("my_conn_url", "myhost:9999");
            vault.put("my_conn_user", "loweruser");
            vault.put("my_conn_password", "lowerpass");

            ResourceHandler.injectVaultSecretsToConnection(conn, vault, null);

            Map<String, Object> config = conn.getConfig();
            assertEquals("lowerpass", config.get("password"));
            assertEquals("loweruser", config.get("username"));
        }

        @Test
        @DisplayName("conn 或 vaultSecrets 为空时直接返回")
        void nullInputs() {
            // Should not throw
            ResourceHandler.injectVaultSecretsToConnection(null, Map.of("k", "v"), null);
            DataSourceConnectionDto conn = makeConn("X");
            ResourceHandler.injectVaultSecretsToConnection(conn, null, null);
            ResourceHandler.injectVaultSecretsToConnection(conn, Map.of(), null);
        }

        @Test
        @DisplayName("精确匹配：不再后缀匹配")
        void exactMatch_noSuffixMatch() {
            DataSourceConnectionDto conn = makeConn("PG");
            Map<String, String> vault = new LinkedHashMap<>();
            // 旧逻辑下 TMH_PG_url 会通过后缀匹配命中，新逻辑要求精确匹配 PG_url
            vault.put("TMH_PG_url", "host:1234");
            vault.put("TMH_PG_user", "user");
            vault.put("TMH_PG_password", "pass");

            // 无精确匹配，也无截取匹配（PG 没有两个下划线），也无 default → 应抛异常
            assertThrows(IllegalArgumentException.class,
                    () -> ResourceHandler.injectVaultSecretsToConnection(conn, vault, null));
        }
    }

    @Nested
    @DisplayName("格式 3：{CONN}_DSN(Variables) + {CONN}_PASSWORD(Secrets)")
    class DsnFormatTest {

        private DataSourceConnectionDto makeConn(String name) {
            DataSourceConnectionDto conn = new DataSourceConnectionDto();
            conn.setName(name);
            conn.setConfig(new LinkedHashMap<>());
            return conn;
        }

        /** 造一份只声明 configPath -> apiServerKey 的 definition，喂 buildConfigPathToApiKeyMap 的 BFS。 */
        private DataSourceDefinitionDto defWith(Map<String, String> configPathToApiKey) {
            LinkedHashMap<String, Object> props = new LinkedHashMap<>();
            configPathToApiKey.forEach((path, apiKey) -> {
                LinkedHashMap<String, Object> meta = new LinkedHashMap<>();
                meta.put("apiServerKey", apiKey);
                props.put(path, meta);
            });
            LinkedHashMap<String, Object> connection = new LinkedHashMap<>();
            connection.put("properties", props);
            LinkedHashMap<String, Object> properties = new LinkedHashMap<>();
            properties.put("connection", connection);

            DataSourceDefinitionDto def = new DataSourceDefinitionDto();
            def.setProperties(properties);
            return def;
        }

        @Test
        @DisplayName("T7-7 MongoDB(L5)：DSN 整串直写 database_uri，{CONN}_PASSWORD splice 回 userinfo")
        void t7_7_mongoDsnDirectWriteWithPasswordSplice() {
            DataSourceConnectionDto conn = makeConn("ORDERS_MONGO");
            // 包里带来的是源环境旧值，应被 DSN 整串覆盖
            conn.getConfig().put("uri", "mongodb://tapuser:oldpw@oldhost:27017/olddb");

            Map<String, String> vault = new LinkedHashMap<>();
            vault.put("ORDERS_MONGO_DSN", "mongodb://tapuser:@h:27017/orders?authSource=admin"); // L5
            vault.put("ORDERS_MONGO_PASSWORD", "p");

            ResourceHandler.injectVaultSecretsToConnection(conn, vault,
                    defWith(Map.of("uri", "database_uri")));

            assertEquals("mongodb://tapuser:p@h:27017/orders?authSource=admin",
                    conn.getConfig().get("uri"));
        }

        @Test
        @DisplayName("T7-7a MongoDB(L6) 副本集：两个 host 与 replicaSet 保真，密码已填回")
        void t7_7a_replicaSetSeedListSurvives() {
            DataSourceConnectionDto conn = makeConn("ORDERS_MONGO");
            Map<String, String> vault = new LinkedHashMap<>();
            vault.put("ORDERS_MONGO_DSN",
                    "mongodb://tapuser:@h1:27017,h2:27017/orders?replicaSet=rs0"); // L6
            vault.put("ORDERS_MONGO_PASSWORD", "p");

            ResourceHandler.injectVaultSecretsToConnection(conn, vault,
                    defWith(Map.of("uri", "database_uri")));

            String written = (String) conn.getConfig().get("uri");
            assertEquals("mongodb://tapuser:p@h1:27017,h2:27017/orders?replicaSet=rs0", written);
            // 反解确认不是巧合的字符串相等：两个 host + replicaSet 真的还在
            com.mongodb.ConnectionString parsed = new com.mongodb.ConnectionString(written);
            assertEquals(java.util.List.of("h1:27017", "h2:27017"), parsed.getHosts());
            assertEquals("rs0", parsed.getRequiredReplicaSetName());
            assertEquals("orders", parsed.getDatabase());
            // D11 的对照：MongoDB 整串直写、不受 query 丢弃之限。把丢弃写成与类型无关的
            // 实现会在这里误丢 replicaSet 并误报告警——而 HA 的常态就是副本集。
            assertTrue(loggedAt(Level.WARN).stream().noneMatch(m -> m.contains("discarded")),
                    "MongoDB 侧不得发 query 丢弃告警，实际 WARN=" + loggedAt(Level.WARN));
        }

        @Test
        @DisplayName("T7-7b MongoDB(L5+L12)：含 @ : / 的密码 splice 时 percent-encode，反解等于原密码")
        void t7_7b_specialCharPasswordIsPercentEncoded() {
            DataSourceConnectionDto conn = makeConn("ORDERS_MONGO");
            Map<String, String> vault = new LinkedHashMap<>();
            vault.put("ORDERS_MONGO_DSN", "mongodb://tapuser:@h:27017/orders?authSource=admin"); // L5
            vault.put("ORDERS_MONGO_PASSWORD", "p@ss:w/rd");                                     // L12

            ResourceHandler.injectVaultSecretsToConnection(conn, vault,
                    defWith(Map.of("uri", "database_uri")));

            String written = (String) conn.getConfig().get("uri");
            assertEquals("mongodb://tapuser:p%40ss%3Aw%2Frd@h:27017/orders?authSource=admin", written);
            // 真正的判据：驱动能把它反解回原密码
            com.mongodb.ConnectionString parsed = new com.mongodb.ConnectionString(written);
            assertEquals("p@ss:w/rd", new String(parsed.getPassword()));
            assertEquals("tapuser", parsed.getUsername());
        }

        /** JDBC 侧 schema：不含 database_uri，含四项 + database_name。 */
        private DataSourceDefinitionDto jdbcDef() {
            LinkedHashMap<String, String> m = new LinkedHashMap<>();
            m.put("host", "database_host");
            m.put("port", "database_port");
            m.put("user", "database_username");
            m.put("password", "database_password");
            m.put("database", "database_name");
            return defWith(m);
        }

        @Test
        @DisplayName("T7-8a JDBC(L1) 裸写形态：解析出 (user, localhost, 3306, test)，库名没被当用户名")
        void t7_8a_bareFormNormalizes() {
            DataSourceConnectionDto conn = makeConn("ORDERS_PG");
            conn.getConfig().put("database", "olddb");

            Map<String, String> vault = new LinkedHashMap<>();
            vault.put("ORDERS_PG_DSN", "user@localhost:3306/test"); // L1
            vault.put("ORDERS_PG_PASSWORD", "p");

            ResourceHandler.injectVaultSecretsToConnection(conn, vault, jdbcDef());

            Map<String, Object> config = conn.getConfig();
            assertEquals("localhost", config.get("host"));
            assertEquals(3306, config.get("port"));
            assertEquals("test", config.get("database"), "库名必须来自 DSN，覆盖包里的 olddb");
            assertEquals("user", config.get("user"), "用户名取自 DSN 的 userinfo");
            assertEquals("p", config.get("password"));
            assertNotEquals("test", config.get("user"), "库名绝不能被当成用户名");
        }

        @Test
        @DisplayName("T7-8 JDBC(L2) jdbc: 形态：库名跨环境覆盖 olddb -> test，path 首段没被当用户名")
        void t7_8_jdbcPrefixedForm() {
            DataSourceConnectionDto conn = makeConn("ORDERS_PG");
            conn.getConfig().put("database", "olddb");

            Map<String, String> vault = new LinkedHashMap<>();
            vault.put("ORDERS_PG_DSN", "jdbc:mysql://user@localhost:3306/test"); // L2
            vault.put("ORDERS_PG_PASSWORD", "p");

            ResourceHandler.injectVaultSecretsToConnection(conn, vault, jdbcDef());

            Map<String, Object> config = conn.getConfig();
            assertEquals("localhost", config.get("host"));
            assertEquals(3306, config.get("port"));
            assertEquals("test", config.get("database"));
            assertEquals("user", config.get("user"));
            assertNotEquals("test", config.get("user"));
        }

        @Test
        @DisplayName("T7-8d JDBC(L3) 只有 scheme 无 jdbc:：与 L1/L2 得到同一组四元组")
        void t7_8d_schemeOnlyForm() {
            DataSourceConnectionDto conn = makeConn("ORDERS_PG");
            Map<String, String> vault = new LinkedHashMap<>();
            vault.put("ORDERS_PG_DSN", "mysql://user@localhost:3306/test"); // L3
            vault.put("ORDERS_PG_PASSWORD", "p");

            ResourceHandler.injectVaultSecretsToConnection(conn, vault, jdbcDef());

            Map<String, Object> config = conn.getConfig();
            assertEquals("localhost", config.get("host"));
            assertEquals(3306, config.get("port"));
            assertEquals("test", config.get("database"));
            assertEquals("user", config.get("user"));
        }

        @Test
        @DisplayName("T7-8c JDBC(L11) 漏写 userinfo：库名是 test、用户名不动，且不去读 {CONN}_USER")
        void t7_8c_missingUserInfoDoesNotReadFormat2Key() {
            DataSourceConnectionDto conn = makeConn("ORDERS_PG");
            conn.getConfig().put("user", "existing-user"); // 目标既有值，缺值规则应保留

            Map<String, String> vault = new LinkedHashMap<>();
            vault.put("ORDERS_PG_DSN", "localhost:3306/test"); // L11
            vault.put("ORDERS_PG_USER", "format2-user");       // 格式 2 的键，格式 3 不认
            vault.put("ORDERS_PG_PASSWORD", "p");

            ResourceHandler.injectVaultSecretsToConnection(conn, vault, jdbcDef());

            Map<String, Object> config = conn.getConfig();
            assertEquals("test", config.get("database"), "path 首段是库名，不是用户名");
            assertEquals("existing-user", config.get("user"), "缺 userinfo 时保留目标既有用户名");
            assertNotEquals("test", config.get("user"));
            assertNotEquals("format2-user", config.get("user"), "格式 3 不得混读 {CONN}_USER");
        }

        @Test
        @DisplayName("T7-8e JDBC query 串被丢弃，additionalString 原封不动")
        void t7_8e_queryStringDiscarded() {
            DataSourceConnectionDto conn = makeConn("ORDERS_PG");
            conn.getConfig().put("additionalString", "SENTINEL-UNTOUCHED");

            Map<String, String> vault = new LinkedHashMap<>();
            vault.put("ORDERS_PG_DSN", "jdbc:postgresql://h:5432/newdb?currentSchema=other");
            vault.put("ORDERS_PG_PASSWORD", "p");

            ResourceHandler.injectVaultSecretsToConnection(conn, vault, jdbcDef());

            Map<String, Object> config = conn.getConfig();
            assertEquals("newdb", config.get("database"), "query 串不得混进库名");
            assertEquals("SENTINEL-UNTOUCHED", config.get("additionalString"),
                    "query 串必须丢弃，不得塞进 additionalString");
        }

        @Test
        @DisplayName("T7-8b 顶层镜像字段与 config 一起写：顶层 database_name = DSN 的库名")
        void t7_8b_topLevelMirrorFieldsWritten() {
            DataSourceConnectionDto conn = makeConn("ORDERS_PG");
            conn.getConfig().put("database", "olddb");
            // 源环境带来的顶层值：全部指向旧环境
            conn.setDatabase_name("olddb");
            conn.setDatabase_host("oldhost");
            conn.setDatabase_port(5432);
            conn.setDatabase_username("olduser");

            Map<String, String> vault = new LinkedHashMap<>();
            vault.put("ORDERS_PG_DSN", "jdbc:mysql://user@localhost:3306/test"); // L2
            vault.put("ORDERS_PG_PASSWORD", "p");

            ResourceHandler.injectVaultSecretsToConnection(conn, vault, jdbcDef());

            assertEquals("test", conn.getConfig().get("database"));
            assertEquals("test", conn.getDatabase_name(),
                    "顶层 database_name 决定元数据限定名，只写 config 会造成半改状态");
            // 同样的半改危险适用于其余镜像字段：不写 ⇒ preserveExistingSecrets 用目标旧值填回
            assertEquals("localhost", conn.getDatabase_host());
            assertEquals(3306, conn.getDatabase_port());
            assertEquals("user", conn.getDatabase_username());
        }

        @Test
        @DisplayName("T7-8b(mongo) 顶层 database_uri 同样被更新")
        void t7_8b_mongoTopLevelUriWritten() {
            DataSourceConnectionDto conn = makeConn("ORDERS_MONGO");
            conn.setDatabase_uri("mongodb://tapuser:oldpw@oldhost:27017/olddb");

            Map<String, String> vault = new LinkedHashMap<>();
            vault.put("ORDERS_MONGO_DSN", "mongodb://tapuser:@h:27017/orders?authSource=admin"); // L5
            vault.put("ORDERS_MONGO_PASSWORD", "p");

            ResourceHandler.injectVaultSecretsToConnection(conn, vault,
                    defWith(Map.of("uri", "database_uri")));

            assertEquals("mongodb://tapuser:p@h:27017/orders?authSource=admin",
                    conn.getDatabase_uri());
        }

        /** 「响亮」是有级别的契约，故真去接日志，不靠肉眼看控制台。 */
        private ListAppender<ILoggingEvent> logs;

        @BeforeEach
        void attachAppender() {
            logs = new ListAppender<>();
            logs.start();
            handlerLogger().addAppender(logs);
        }

        @AfterEach
        void detachAppender() {
            handlerLogger().detachAppender(logs);
        }

        private ch.qos.logback.classic.Logger handlerLogger() {
            return (ch.qos.logback.classic.Logger)
                    org.slf4j.LoggerFactory.getLogger(ResourceHandler.class);
        }

        private List<String> loggedAt(Level level) {
            List<String> messages = new ArrayList<>();
            for (ILoggingEvent event : logs.list) {
                if (event.getLevel() == level) messages.add(event.getFormattedMessage());
            }
            return messages;
        }

        private void assertWarnContains(String needle) {
            assertTrue(loggedAt(Level.WARN).stream().anyMatch(m -> m.contains(needle)),
                    "期望一条 WARN 逐字包含 '" + needle + "'，实际 WARN=" + loggedAt(Level.WARN));
        }

        @Test
        @DisplayName("T7-8e query 串被丢弃时，告警逐字点名被丢掉的那一段")
        void t7_8e_queryDiscardWarningNamesIt() {
            DataSourceConnectionDto conn = makeConn("ORDERS_PG");
            Map<String, String> vault = new LinkedHashMap<>();
            vault.put("ORDERS_PG_DSN", "jdbc:postgresql://h:5432/newdb?currentSchema=other");
            vault.put("ORDERS_PG_PASSWORD", "p");

            ResourceHandler.injectVaultSecretsToConnection(conn, vault, jdbcDef());

            assertWarnContains("currentSchema=other");
        }

        @Test
        @DisplayName("D10(L10) DSN 无库名：沿用目标旧库名并告警，不报错")
        void d10_missingDatabaseNameWarnsAndKeepsOld() {
            DataSourceConnectionDto conn = makeConn("ORDERS_PG");
            conn.getConfig().put("database", "olddb");

            Map<String, String> vault = new LinkedHashMap<>();
            vault.put("ORDERS_PG_DSN", "localhost:3306"); // L10
            vault.put("ORDERS_PG_PASSWORD", "p");

            ResourceHandler.injectVaultSecretsToConnection(conn, vault, jdbcDef());

            assertEquals("olddb", conn.getConfig().get("database"), "无库名时保留目标既有库名");
            assertWarnContains("database name");
        }

        @Test
        @DisplayName("D10(L11) DSN 漏写 userinfo：保留目标既有用户名并告警")
        void d10_missingUserInfoWarns() {
            DataSourceConnectionDto conn = makeConn("ORDERS_PG");
            conn.getConfig().put("user", "existing-user");

            Map<String, String> vault = new LinkedHashMap<>();
            vault.put("ORDERS_PG_DSN", "localhost:3306/test"); // L11
            vault.put("ORDERS_PG_PASSWORD", "p");

            ResourceHandler.injectVaultSecretsToConnection(conn, vault, jdbcDef());

            assertWarnContains("username");
        }

        @Test
        @DisplayName("D10 无 {CONN}_PASSWORD：不报错，告警逐字点名那个键名")
        void d10_missingPasswordWarnsNamingTheKeyVerbatim() {
            DataSourceConnectionDto conn = makeConn("ORDERS_PG");
            Map<String, String> vault = new LinkedHashMap<>();
            vault.put("ORDERS_PG_DSN", "user@localhost:3306/test"); // L1，无 _PASSWORD

            ResourceHandler.injectVaultSecretsToConnection(conn, vault, jdbcDef());

            assertEquals("test", conn.getConfig().get("database"), "缺密码不影响其余成分照常写入");
            // 泛化的 "no matching vault keys" 不算——操作者要能一眼看出该去配哪个键
            assertWarnContains("ORDERS_PG_PASSWORD");
        }

        @Test
        @DisplayName("T7-12 无 {CONN}_PASSWORD 时 password 路径原封不动，不写空串也不写 null")
        void t7_12_noPasswordLeavesPasswordPathUntouched() {
            DataSourceConnectionDto conn = makeConn("ORDERS_PG");
            conn.getConfig().put("password", "SENTINEL-TARGET-PASSWORD");

            Map<String, String> vault = new LinkedHashMap<>();
            vault.put("ORDERS_PG_DSN", "user@localhost:3306/test"); // L1，无 _PASSWORD

            ResourceHandler.injectVaultSecretsToConnection(conn, vault, jdbcDef());

            assertEquals("SENTINEL-TARGET-PASSWORD", conn.getConfig().get("password"),
                    "写空值会让 [ADR-0034] D5 认为包里有值而放行，目标密码当场被抹空");
            assertEquals("localhost", conn.getConfig().get("host"), "其余成分照常写入");
        }

        @Test
        @DisplayName("D8 先拼后验：splice 结果非法时报错，且消息不回显拼出来的串")
        void d8_splicedResultIsValidatedAndErrorDoesNotEchoIt() {
            DataSourceConnectionDto conn = makeConn("ORDERS_MONGO");
            Map<String, String> vault = new LinkedHashMap<>();
            // 作者漏编码的用户名：raw '@' 在 userinfo 里，ConnectionString 会拒
            vault.put("ORDERS_MONGO_DSN", "mongodb://us@er:@h:27017/db");
            vault.put("ORDERS_MONGO_PASSWORD", "secret-pw");

            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> ResourceHandler.injectVaultSecretsToConnection(conn, vault,
                            defWith(Map.of("uri", "database_uri"))));

            assertTrue(ex.getMessage().contains("ORDERS_MONGO"), "消息要点名是哪条连接");
            assertFalse(ex.getMessage().contains("secret-pw"),
                    "splice 之后串里带着密码，消息绝不能回显它");
        }

        @Test
        @DisplayName("优先级6 无命中报错文案提到 DSN，让老 TM 撞上时自解释")
        void priority6_errorMessageMentionsDsn() {
            DataSourceConnectionDto conn = makeConn("ORDERS_PG");
            Map<String, String> vault = new LinkedHashMap<>();
            vault.put("OTHER_url", "h:1");
            vault.put("OTHER_user", "u");
            vault.put("OTHER_password", "p");

            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> ResourceHandler.injectVaultSecretsToConnection(conn, vault, jdbcDef()));

            assertTrue(ex.getMessage().toLowerCase().contains("dsn"),
                    "老 TM 遇到只含 _DSN 的 vault 会走到这里中止整批导入，文案不提 DSN 就读成租户配错了；实际=" + ex.getMessage());
        }

        @Test
        @DisplayName("T7-9 老 vault 回归：只有 {CONN}_URI 时行为逐字不变，password 字段不被碰")
        void t7_9_legacyUriPathUnchanged() {
            DataSourceConnectionDto conn = makeConn("ORDERS_MONGO");
            conn.getConfig().put("password", "SENTINEL-TARGET-PASSWORD");

            Map<String, String> vault = new LinkedHashMap<>();
            vault.put("ORDERS_MONGO_uri", "mongodb://u:pw@h:27017/db");
            // ⚠ vault 里**必须**同时放一个 password 键，否则这条断言是假闸：
            // 没有这个键时，任何"多去找一次 password"的实现都因查不到而跳过，哨兵照样活着。
            vault.put("ORDERS_MONGO_password", "SHOULD-NOT-BE-INJECTED");

            ResourceHandler.injectVaultSecretsToConnection(conn, vault,
                    defWith(Map.of("uri", "database_uri")));

            assertEquals("mongodb://u:pw@h:27017/db", conn.getConfig().get("uri"), "整串直写，与今天一致");
            assertEquals("SENTINEL-TARGET-PASSWORD", conn.getConfig().get("password"),
                    "老 uri 路径从不注入 password；新逻辑若无条件去找 password，这里会多写一个字段");
        }

        @Test
        @DisplayName("T7-10 老格式回归：{CONN}_URL/_USER/_PASSWORD 结果不变，且库名不被新逻辑碰到")
        void t7_10_legacyUrlPathDoesNotTouchDatabaseName() {
            DataSourceConnectionDto conn = makeConn("ORDERS_PG");
            conn.getConfig().put("database", "olddb");
            conn.setDatabase_name("olddb");

            Map<String, String> vault = new LinkedHashMap<>();
            vault.put("ORDERS_PG_URL", "pghost:5432");
            vault.put("ORDERS_PG_USER", "pguser");
            vault.put("ORDERS_PG_PASSWORD", "pgpass");

            ResourceHandler.injectVaultSecretsToConnection(conn, vault, jdbcDef());

            Map<String, Object> config = conn.getConfig();
            assertEquals("pghost", config.get("host"));
            assertEquals(5432, config.get("port"));
            assertEquals("pguser", config.get("user"));
            assertEquals("pgpass", config.get("password"));
            // 「已定口径 3」：库名覆盖是格式 3 独有的能力，老格式一个字都不改
            assertEquals("olddb", config.get("database"), "老格式不得获得库名覆盖能力");
            assertEquals("olddb", conn.getDatabase_name(), "顶层同理");
        }
    }
}
