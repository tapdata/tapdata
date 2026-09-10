package io.tapdata.dql.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DqlEventIdentityTest {

    @Test
    @DisplayName("identity metadata can be applied to the shared event report")
    void appliesToEventReport() {
        DqlEventIdentity identity = new DqlEventIdentity();
        identity.setEventKey(Map.of("id", 1001));
        identity.setEventKeyMissing(false);
        identity.setPayloadHash("sha256:payload");
        identity.setRecordIdentity("key:orders:sha256:record");
        identity.setRecordIdentityType(DqlRecordIdentityType.PRIMARY_KEY);
        identity.setRecordIdentityFields(List.of("id"));
        identity.setEventIdentity("eo:event-1");

        DqlEventReport report = new DqlEventReport();
        identity.applyTo(report);

        assertEquals(identity.getEventKey(), report.getEventKey());
        assertEquals(identity.isEventKeyMissing(), report.getEventKeyMissing());
        assertEquals(identity.getPayloadHash(), report.getPayload().getPayloadHash());
        assertEquals(identity.getRecordIdentity(), report.getRecordIdentity());
        assertEquals("PRIMARY_KEY", report.getRecordIdentityType());
        assertEquals(identity.getRecordIdentityFields(), report.getRecordIdentityFields());
        assertEquals(identity.getEventIdentity(), report.getEventIdentity());
    }
}
