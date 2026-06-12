package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import java.util.*;
import java.util.regex.*;

import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

class SqlAliasResolverTest {

    @Test
    void testSimpleAliasReplacement() {
        String querySql = "SELECT t1.id FROM t1";
        Map<String, String> aliasMap = Map.of("t1", "node_mysql_1__users");
        
        String resolved = resolveWithBoundaryDetection(querySql, aliasMap);
        
        assertEquals("SELECT node_mysql_1__users.id FROM node_mysql_1__users", resolved);
    }

    @Test
    void testJoinQueryReplacement() {
        String querySql = "SELECT t1.id, t2.name FROM t1 JOIN t2 ON t1.id = t2.ref_id";
        Map<String, String> aliasMap = Map.of(
            "t1", "node_mysql_1__users",
            "t2", "node_pg_1__orders"
        );
        
        String resolved = resolveWithBoundaryDetection(querySql, aliasMap);
        
        assertTrue(resolved.contains("node_mysql_1__users"));
        assertTrue(resolved.contains("node_pg_1__orders"));
        assertFalse(resolved.matches(".*\\bt1\\b.*"));
        assertFalse(resolved.matches(".*\\bt2\\b.*"));
    }

    @Test
    void testNoPartialReplacementInStrings() {
        String querySql = "SELECT t1.id, 't1_is_alias' AS comment FROM t1 WHERE name LIKE '%t1%'";
        Map<String, String> aliasMap = Map.of("t1", "target_table");
        
        String resolved = resolveWithBoundaryDetection(querySql, aliasMap);
        
        assertTrue(resolved.contains("'t1_is_alias'"), "String literal should not be modified");
        assertTrue(resolved.contains("'%t1%'"), "LIKE pattern should not be modified");
        assertEquals(3, countOccurrences(resolved, "target_table"), 
                     "Only 3 standalone t1 occurrences should be replaced");
    }

    @Test
    void testComplexQueryWithSubquery() {
        String querySql = """
            SELECT t1.id, t2.amount,
                   (SELECT COUNT(*) FROM t3 WHERE t3.t1_id = t1.id) AS sub_count
            FROM t1
            JOIN t2 ON t1.id = t2.user_id
            """;
        Map<String, String> aliasMap = Map.of(
            "t1", "table_a",
            "t2", "table_b",
            "t3", "table_c"
        );
        
        String resolved = resolveWithBoundaryDetection(querySql, aliasMap);
        
        assertTrue(resolved.contains("table_a"));
        assertTrue(resolved.contains("table_b"));
        assertTrue(resolved.contains("table_c"));
        assertFalse(resolved.matches(".*\\bt1\\b.*"));
    }

    @Test
    void testEmptyAliasMapReturnsOriginal() {
        String querySql = "SELECT id FROM users";
        Map<String, String> aliasMap = Collections.emptyMap();
        
        String resolved = resolveWithBoundaryDetection(querySql, aliasMap);
        
        assertEquals(querySql, resolved);
    }

    @Test
    void testPreserveCaseSensitivity() {
        String querySql = "SELECT T1.id, t1.name FROM T1 LEFT JOIN t2 ON T1.id = t2.ref";
        Map<String, String> aliasMap = Map.of(
            "T1", "UPPER_TABLE",
            "t2", "lower_table"
        );
        
        String resolved = resolveWithBoundaryDetection(querySql, aliasMap);
        
        assertTrue(resolved.contains("UPPER_TABLE"));
        assertTrue(resolved.contains("lower_table"));
        assertFalse(resolved.contains("T1"));
        assertFalse(resolved.contains("t2"));
    }
    
    // Helper method (will be extracted to production code later)
    private String resolveWithBoundaryDetection(String sql, Map<String, String> aliasMap) {
        String currentSql = sql;
        
        for (Map.Entry<String, String> entry : aliasMap.entrySet()) {
            String alias = entry.getKey();
            String targetTableName = entry.getValue();
            
            Pattern pattern = Pattern.compile("\\b" + Pattern.quote(alias) + "\\b");
            Matcher matcher = pattern.matcher(currentSql);
            
            StringBuffer sb = new StringBuffer();
            while (matcher.find()) {
                matcher.appendReplacement(sb, Matcher.quoteReplacement(targetTableName));
            }
            matcher.appendTail(sb);
            
            currentSql = sb.toString();
        }
        
        return currentSql;
    }

    private long countOccurrences(String str, String sub) {
        return str.split(Pattern.quote(sub), -1).length - 1;
    }
}
