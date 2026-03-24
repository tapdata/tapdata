package com.tapdata.tm.group.handler;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
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
}
