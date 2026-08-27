package io.tapdata.dql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.mongo.RestTemplateOperator;
import io.tapdata.dql.client.DqlTmClient;
import io.tapdata.dql.model.DqlClassificationConfidence;
import io.tapdata.dql.model.DqlClassificationResult;
import io.tapdata.dql.model.DqlErrorType;
import io.tapdata.dql.model.DqlEventReport;
import io.tapdata.dql.model.DqlEventReportResult;
import io.tapdata.dql.model.DqlExceptionScope;
import io.tapdata.dql.model.DqlPayloadSnapshot;
import io.tapdata.dql.model.DqlRecordSuccessReport;
import io.tapdata.dql.model.DqlRecordSuccessReportResult;
import io.tapdata.dql.model.DqlRecoveryReport;
import io.tapdata.dql.model.DqlRouteDecision;
import io.tapdata.exception.ManagementException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
class DqlTmClientTest {
    private static final String TASK_ID = "task-1";

    @Test
    @DisplayName("classification and payload snapshot flatten into the TM report contract")
    void reportModelUsesContractFields() {
        DqlEventReport report = new DqlEventReport();
        report.setTaskRecordId("record-1");
        report.setTaskVersion(7L);
        report.setDmlType("U");
        report.setPayload(new DqlPayloadSnapshot());
        report.getPayload().setPayloadFormat("tap-record-event-json-v1");
        report.getPayload().setPayloadData(Map.of("after", Map.of("id", 1001)));
        report.getPayload().setPayloadHash("sha256:payload");
        report.getPayload().setPayloadSize(128L);
        report.getPayload().setPayloadComplete(true);
        DqlClassificationResult classification = new DqlClassificationResult();
        classification.setExceptionScope(DqlExceptionScope.RECORD);
        classification.setRouteDecision(DqlRouteDecision.RECORD_DLQ);
        classification.setErrorType(DqlErrorType.TARGET_WRITE_ERROR);
        classification.setClassificationReason("single record constraint violation");
        classification.setClassificationConfidence(DqlClassificationConfidence.RULE);

        classification.applyTo(report);

        assertEquals(DqlExceptionScope.RECORD, report.getExceptionScope());
        assertEquals(DqlRouteDecision.RECORD_DLQ, report.getRouteDecision());
        assertEquals(DqlErrorType.TARGET_WRITE_ERROR, report.getErrorType());
        assertEquals("single record constraint violation", report.getClassificationReason());
        assertEquals(DqlClassificationConfidence.RULE, report.getClassificationConfidence());
        assertEquals("tap-record-event-json-v1", report.getPayload().getPayloadFormat());
        assertEquals(Map.of("after", Map.of("id", 1001)), report.getPayload().getPayloadData());
        try {
            String json = new ObjectMapper().writeValueAsString(report);
            assertTrue(json.contains("\"payloadFormat\":\"tap-record-event-json-v1\""));
            assertFalse(json.contains("\"payload\""));
        } catch (Exception exception) {
            throw new AssertionError(exception);
        }
    }

    @Test
    @DisplayName("TM client posts reports to the contract endpoints and preserves duplicate responses")
    void reportsUseContractPaths() {
        RecordingMongoOperator operator = new RecordingMongoOperator();
        DqlTmClient client = new DqlTmClient(operator);
        DqlEventReportResult eventResult = new DqlEventReportResult();
        eventResult.setEventId("DQL-1");
        eventResult.setStatus("PENDING");
        eventResult.setDuplicate(true);
        operator.respond("task/task-1/dql-events/report", eventResult);
        DqlRecordSuccessReportResult successResult = new DqlRecordSuccessReportResult();
        successResult.setMarked(true);
        successResult.setEventId("DQL-1");
        operator.respond("task/task-1/dql-events/record-success/report", successResult);

        DqlEventReport report = new DqlEventReport();
        report.setTaskRecordId("record-1");
        report.setRouteDecision(DqlRouteDecision.RECORD_DLQ);
        report.getPayload().setPayloadFormat("tap-record-event-json-v1");
        DqlEventReportResult actualEvent = client.reportEvent(TASK_ID, report);
        DqlRecordSuccessReport success = new DqlRecordSuccessReport();
        success.setTaskRecordId("record-1");
        DqlRecordSuccessReportResult actualSuccess = client.reportRecordSuccess(TASK_ID, success);

        assertTrue(actualEvent.isDuplicate());
        assertEquals("DQL-1", actualEvent.getEventId());
        assertTrue(actualSuccess.isMarked());
        assertEquals("task/task-1/dql-events/report", operator.calls.get(0).resource());
        assertEquals("record-1", operator.calls.get(0).payload().get("taskRecordId"));
        assertEquals("RECORD_DLQ", operator.calls.get(0).payload().get("routeDecision"));
        assertEquals("tap-record-event-json-v1", operator.calls.get(0).payload().get("payloadFormat"));
        assertFalse(operator.calls.get(0).payload().containsKey("payload"));
        assertEquals("task/task-1/dql-events/record-success/report", operator.calls.get(1).resource());
    }

    @Test
    @DisplayName("recovery callbacks use the dedicated TM endpoint and keep the batch contract")
    void recoveryReportUsesContractPath() {
        RecordingMongoOperator operator = new RecordingMongoOperator();
        DqlTmClient client = new DqlTmClient(operator);
        operator.respond("task/task-1/dql-events/recovery/report", Boolean.TRUE);

        Boolean acknowledged = client.reportRecovery(TASK_ID, DqlRecoveryReport.batchStarted("batch-1", 100L));

        assertTrue(acknowledged);
        assertEquals("task/task-1/dql-events/recovery/report", operator.calls.get(0).resource());
        assertEquals("batch-1", operator.calls.get(0).payload().get("batchId"));
        assertEquals("BATCH_STARTED", operator.calls.get(0).payload().get("type"));
        assertEquals(100L, operator.calls.get(0).payload().get("startedAt"));
    }

    @Test
    @DisplayName("TM client rejects null responses so callers cannot skip without a TM acknowledgement")
    void emptyResponseFailsAsManagementException() {
        RecordingMongoOperator operator = new RecordingMongoOperator();
        DqlTmClient client = new DqlTmClient(operator);

        ManagementException exception = assertThrows(ManagementException.class,
                () -> client.reportEvent(TASK_ID, new DqlEventReport()));

        assertTrue(exception.getMessage().contains("empty"));
    }

    @Test
    @DisplayName("TM client rejects incomplete input before constructing an endpoint")
    void invalidInputIsRejected() {
        DqlTmClient client = new DqlTmClient(new RecordingMongoOperator());

        assertThrows(IllegalArgumentException.class, () -> client.reportEvent(" ", new DqlEventReport()));
        assertThrows(IllegalArgumentException.class, () -> client.reportRecordSuccess(TASK_ID, null));
    }

    private record Call(String resource, Map<String, Object> payload) {
    }

    private static class RecordingMongoOperator extends HttpClientMongoOperator {
        private final List<Call> calls = new ArrayList<>();
        private final Map<String, Object> responses = new HashMap<>();

        private RecordingMongoOperator() {
            super(null, null, new RestTemplateOperator(Collections.singletonList("http://localhost"), 0), new ConfigurationCenter());
        }

        private void respond(String resource, Object response) {
            responses.put(resource, response);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T postOne(Map<String, Object> payload, String resource, Class<T> responseType) {
            calls.add(new Call(resource, payload));
            return (T) responses.get(resource);
        }
    }
}
