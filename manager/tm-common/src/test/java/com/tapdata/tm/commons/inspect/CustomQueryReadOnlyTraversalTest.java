package com.tapdata.tm.commons.inspect;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class CustomQueryReadOnlyTraversalTest {
    @TestFactory
    Stream<DynamicTest> rejectsUnsafeExpressionsInEveryQueryClause() {
        return Stream.of(
                "select 1 from t limit (select count(*) from dblink_exec('c','delete from x') q)",
                "select 1 from t limit 1 offset (select 1 from dblink_exec('c','delete from x') q)",
                "select 1 union select 2 limit (select nextval('s'))",
                "select 1 union select 2 offset (select nextval('s'))",
                "select top (nextval('s')) v from t",
                "select distinct on (nextval('s')) v from t",
                "select count(*) filter (where nextval('s') > 0) from t",
                "select percentile_cont(0.5) within group (order by nextval('s')) from t",
                "select sum(v) over (order by nextval('s')) from t",
                "select sum(v) over (partition by nextval('s')) from t",
                "select lag(v,nextval('s'),0) over (order by v) from t",
                "select lag(v,1,nextval('s')) over (order by v) from t",
                "select sum(v) over (order by v rows nextval('s') preceding) from t",
                "select sum(v) over (order by v rows between nextval('s') preceding and 1 following) from t",
                "select sum(v) over (order by v rows between 1 preceding and nextval('s') following) from t",
                "select nextval('s') over () from t",
                "select max(v) keep (dense_rank first order by nextval('s')) from t",
                "select group_concat(nextval('s')) from t",
                "select group_concat(v order by nextval('s')) from t",
                "select sum(v) from t group by grouping sets ((nextval('s')), ())"
        ).map(sql -> DynamicTest.dynamicTest(sql, () -> {
            // Prove these reach the AST guard rather than passing because of a parser error.
            assertDoesNotThrow(() -> CCJSqlParserUtil.parseStatements(sql));
            IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                    () -> CustomQueryReadOnly.validateReadOnlyQuery(sql, "test"));
            assertTrue(error.getMessage().contains("Side-effecting SQL function"), error.getMessage());
        }));
    }

    @TestFactory
    Stream<DynamicTest> rejectsExecutableComments() {
        return Stream.of(
                "select 1 /*! into outfile '/tmp/query-output' */",
                "select 1 /*M! into outfile '/tmp/query-output' */",
                "select 1 /*M!100100 , 2 */",
                "select 1 /* ordinary comment */ /*M! , 2 */",
                "select '/*!' as v /*M! , 2 */"
        ).map(sql -> DynamicTest.dynamicTest(sql, () -> {
            IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                    () -> CustomQueryReadOnly.validateReadOnlyQuery(sql, "test"));
            assertTrue(error.getMessage().contains("Executable SQL comments"), error.getMessage());
        }));
    }

    @TestFactory
    Stream<DynamicTest> rejectsExternalAccessAndAdministrativeFunctions() {
        return Stream.of(
                "select * from file('data.csv', 'CSV')",
                "select * from url('http://example.test/data', 'CSV', 'v String')",
                "select * from s3('https://example.test/data', 'CSV')",
                "select * from remote('example.test', 'db.t')",
                "select * from remoteSecure('example.test', 'db.t')",
                "select * from mysql('example.test', 'db', 't', 'u', 'p')",
                "select * from postgresql('example.test', 'db', 't', 'u', 'p')",
                "select * from mongodb('example.test', 'db', 't', 'u', 'p')",
                "select * from jdbc('jdbc:test', 'select 1')",
                "select * from odbc('DSN=test', 'db', 't')",
                "select * from hdfs('hdfs://example.test/data', 'CSV')",
                "select * from input('v String')",
                "select pg_drop_replication_slot('s')",
                "select pg_create_physical_replication_slot('s')",
                "select pg_create_logical_replication_slot('s','test_decoding')",
                "select pg_replication_slot_advance('s','0/1')",
                "select pg_switch_wal()",
                "select pg_wal_replay_pause()",
                "select pg_wal_replay_resume()",
                "select pg_backup_start('label')",
                "select pg_backup_stop()",
                "select pg_start_backup('label')",
                "select pg_stop_backup()",
                "select pg_ls_waldir()",
                "select pg_catalog.\"pg_ls_logdir\"()"
        ).map(sql -> DynamicTest.dynamicTest(sql, () -> {
            assertDoesNotThrow(() -> CCJSqlParserUtil.parseStatements(sql));
            IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                    () -> CustomQueryReadOnly.validateReadOnlyQuery(sql, "test"));
            assertTrue(error.getMessage().contains("Side-effecting SQL function"), error.getMessage());
        }));
    }

    @TestFactory
    Stream<DynamicTest> acceptsReadOnlyQueriesAndCommentMarkersInsideValues() {
        return Stream.of(
                "select '/*!' as v", "select '/*M!' as v", "select 'it''s /*!' as v",
                "select 1 as \"/*!\"", "select $$/*!$$ as v",
                "select 1 /* ordinary comment mentioning /*! and /*M! */",
                "select 1 -- /*! and /*M! are only comment text\n",
                "select count(*) filter (where v > 0) from t",
                "select percentile_cont(0.5) within group (order by v) from t",
                "select sum(v) over (partition by k order by v rows between 2 preceding and current row) from t",
                "select lag(v,1,0) over (order by v) from t",
                "select max(v) keep (dense_rank first order by k) from t",
                "select group_concat(v order by k) from t",
                "select sum(v) from t group by grouping sets ((k), ())",
                "select 1 from t limit (select count(*) from x)",
                "select 1 from t limit 1 offset (select count(*) from x)",
                "select 1 union select 2 limit 1 offset 1",
                "select v from t offset 1 rows fetch next 2 rows only",
                "select top (2) v from t",
                "select distinct on (k) v from t",
                "select 'nextval' as v, pg_typeof(1)")
                .map(sql -> DynamicTest.dynamicTest(sql,
                        () -> assertDoesNotThrow(() -> CustomQueryReadOnly.validateReadOnlyQuery(sql, "test"))));
    }
}
