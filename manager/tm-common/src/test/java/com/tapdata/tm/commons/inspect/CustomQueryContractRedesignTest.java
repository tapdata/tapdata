package com.tapdata.tm.commons.inspect;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class CustomQueryContractRedesignTest {
    CustomQueryRequest request(String sql) { CustomQueryRequest r=new CustomQueryRequest();r.setConnectionId("connection");r.setQuery(sql);r.setTimezone("UTC");return r; }
    @Test void readOnlySqlIncludesCteJoinAndSubqueryButRejectsWrites() {
        for (String sql:List.of("select count(*) c from t", "with c as (select id from t) select c.id from c join t on c.id=t.id", "select * from (select id from t) q")) request(sql).validate();
        for (String sql:List.of("delete from t", "select * into newtable from t", "select * from t for update", "select 1;delete from t", "with c as (delete from t returning id) select * from c")) assertThrows(IllegalArgumentException.class,()->request(sql).validate(),sql);
    }
    @Test void mongoNestedWriteStagesAndScriptsAreRejected() {
        for (String json:List.of("[{\"$match\":{\"id\":1}}]","[{\"$group\":{\"_id\":\"$id\",\"total\":{\"$sum\":\"$amount\"}}}]")) { var r=request(json);r.setKind("mongoPipeline");r.setCollection("rows");r.validate(); }
        for (String json:List.of("[{\"$out\":\"write\"}]", "[{\"$lookup\":{\"from\":\"t\",\"pipeline\":[{\"$merge\":\"t\"}],\"as\":\"rows\"}}]", "[{\"$facet\":{\"x\":[{\"$out\":\"t\"}]}}]", "[{\"$match\":{\"$where\":\"evil()\"}}]")) { var r=request(json);r.setKind("mongoPipeline");r.setCollection("rows");assertThrows(IllegalArgumentException.class,r::validate); }
    }
    @Test void snapshotFingerprintChangesWithEveryExecutionInputAndRoundTrips() throws Exception {
        var r=request("select 1 v");String fingerprint=r.fingerprint();
        r.setCollection("c");assertNotEquals(fingerprint,r.fingerprint());r.setCollection(null);
        r.setConnectionId("other");assertNotEquals(fingerprint,r.fingerprint());r.setConnectionId("connection");
        r.setQuery("select 2 v");assertNotEquals(fingerprint,r.fingerprint());r.setQuery("select 1 v");
        r.setTimezone("Asia/Shanghai");assertNotEquals(fingerprint,r.fingerprint());r.setTimezone("UTC");
        assertEquals(fingerprint,r.fingerprint());assertEquals(fingerprint,new ObjectMapper().readValue(new ObjectMapper().writeValueAsString(r),CustomQueryRequest.class).fingerprint());
    }
    @Test void limitsAndDuplicateAliasesAreValidatedBeforeExecution() {
        var config=new CustomQueryConfig();var snapshot=new CustomQueryConfig.Snapshot();var c=new CustomQueryConfig.Column();c.setName("id");snapshot.setColumns(Arrays.asList(c,c));
        assertThrows(IllegalArgumentException.class,()->config.columnNames(snapshot));
        config.setMaxRows(10001);assertThrows(IllegalArgumentException.class,()->config.validate(snapshot,snapshot));
        config.setMaxRows(1000);config.setTimeoutSeconds(0);assertThrows(IllegalArgumentException.class,()->config.validate(snapshot,snapshot));
    }
    @Test void requestRejectsZeroTimeoutAndAcceptsBlankUtcTimezone() {
        var r=request("select 1 v");r.setTimeoutSeconds(0);
        assertThrows(IllegalArgumentException.class,r::validate);
        r.setTimeoutSeconds(1);r.setTimezone(" ");assertDoesNotThrow(r::validate);
        assertEquals(java.time.ZoneId.of("UTC"),CustomQueryRequest.zoneId(r.getTimezone()));
    }

    @Test void selectFunctionsCannotMutateSequencesSessionsOrRemoteDatabases() {
        for (String sql : List.of("select nextval('s')", "select pg_catalog.\"setval\"('s',1)",
                "select pg_terminate_backend(1)", "select lo_export(1,'/tmp/file')", "select dblink_exec('c','delete from t')",
                "select next value for s", "select s.nextval from dual", "select dbms_lock.request(1) from dual",
                "select 1 from t order by nextval('s')", "select count(*) from t group by nextval('s')",
                "with c as (select set_config('x','y',false)) select * from c",
                "select coalesce((select nextval('s')),0)", "select sum(nextval('s')) from t")) {
            assertThrows(IllegalArgumentException.class, () -> request(sql).validate(), sql);
        }
        for (String sql : List.of("select sum(v),count(*) from t", "select coalesce(v,0) from t", "select 'nextval' as v"))
            assertDoesNotThrow(() -> request(sql).validate(), sql);
    }

    @Test void resolvedPreviewTimezoneIsFallbackButExplicitUtcWins() {
        CustomQueryConfig.Snapshot snapshot = new CustomQueryConfig.Snapshot(); snapshot.setTimezone("Asia/Shanghai");
        assertEquals("Asia/Shanghai",CustomQueryRequest.effectiveTimezone(null,snapshot));
        assertEquals("Asia/Shanghai",CustomQueryRequest.effectiveTimezone("  ",snapshot));
        assertEquals("UTC",CustomQueryRequest.effectiveTimezone("UTC",snapshot));
        assertEquals("UTC",CustomQueryRequest.effectiveTimezone(null,null));
    }

}
