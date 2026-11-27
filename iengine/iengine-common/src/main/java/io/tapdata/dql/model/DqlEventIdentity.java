package io.tapdata.dql.model;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * Engine-generated event and business-record identity metadata.
 */
@Data
public class DqlEventIdentity {
    private Map<String, Object> eventKey;
    private boolean eventKeyMissing;
    private String payloadHash;
    private String recordIdentity;
    private DqlRecordIdentityType recordIdentityType;
    private List<String> recordIdentityFields;
    private String eventIdentity;

    /**
     * Copies generated metadata to the shared TM report model.
     */
    public void applyTo(DqlEventReport report) {
        if (report == null) {
            throw new IllegalArgumentException("report must not be null");
        }
        report.setEventKey(eventKey);
        report.setEventKeyMissing(eventKeyMissing);
        report.setEventIdentity(eventIdentity);
        report.setRecordIdentity(recordIdentity);
        report.setRecordIdentityType(recordIdentityType == null ? null : recordIdentityType.name());
        report.setRecordIdentityFields(recordIdentityFields);
        if (report.getPayload() == null) {
            report.setPayload(new DqlPayloadSnapshot());
        }
        report.getPayload().setPayloadHash(payloadHash);
    }

    /**
     * Copies the same identity metadata to a later-success callback.
     */
    public void applyTo(DqlRecordSuccessReport report) {
        if (report == null) {
            throw new IllegalArgumentException("report must not be null");
        }
        report.setEventKey(eventKey);
        report.setRecordIdentity(recordIdentity);
        report.setRecordIdentityType(recordIdentityType == null ? null : recordIdentityType.name());
        report.setRecordIdentityFields(recordIdentityFields);
        report.setPayloadHash(payloadHash);
    }
}
