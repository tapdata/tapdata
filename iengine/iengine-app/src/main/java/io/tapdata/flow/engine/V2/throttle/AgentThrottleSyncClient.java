package io.tapdata.flow.engine.V2.throttle;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.throttle.AgentThrottleCapabilities;
import com.tapdata.tm.commons.throttle.AgentThrottleDeltaResponse;
import com.tapdata.tm.commons.throttle.TaskThrottleApplyReceipt;
import com.tapdata.tm.commons.throttle.TaskThrottleStopEvidenceDto;
import com.tapdata.tm.commons.throttle.TaskThrottleTelemetrySample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Control-plane synchronization client between Agent {@link TaskThrottleRuntimeRegistry} and TM:
 * - registers Agent metering/throttling capabilities,
 * - pulls incremental policy/runEpoch/sample-config deltas every 5 seconds,
 * - uploads apply receipts, 5s telemetry windows, and graceful stop evidences.
 * Control-plane transient failures never interrupt already-authorized running tasks.
 */
@Component
public class AgentThrottleSyncClient {

    private static final Logger logger = LoggerFactory.getLogger(AgentThrottleSyncClient.class);

    public static final String RESOURCE_CAPABILITIES = "agent-throttle/capabilities";
    public static final String RESOURCE_DELTA = "agent-throttle/delta";
    public static final String RESOURCE_RECEIPTS = "agent-throttle/receipts";
    public static final String RESOURCE_TELEMETRY = "agent-throttle/telemetry";
    public static final String RESOURCE_STOP_EVIDENCE = "agent-throttle/stop-evidence";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    @Autowired(required = false)
    private ClientMongoOperator clientMongoOperator;
    @Autowired(required = false)
    private TaskThrottleRuntimeRegistry registry = TaskThrottleRuntimeRegistry.getInstance();

    public AgentThrottleSyncClient() {
    }

    public AgentThrottleSyncClient(
            ClientMongoOperator clientMongoOperator,
            TaskThrottleRuntimeRegistry registry) {
        this.clientMongoOperator = clientMongoOperator;
        this.registry = registry != null ? registry : TaskThrottleRuntimeRegistry.getInstance();
    }

    @Scheduled(fixedDelay = 5000L, initialDelay = 3000L)
    public void scheduledSyncCycle() {
        pullDeltaAndApply();
        flushPendingReceipts();
        flushTelemetrySamples();
        flushPendingStopEvidences();
    }

    public boolean registerCapabilities() {
        if (clientMongoOperator == null) {
            return false;
        }
        try {
            AgentThrottleCapabilities capabilities = registry.buildCapabilities();
            Map<String, Object> payload = OBJECT_MAPPER.convertValue(capabilities, MAP_TYPE);
            clientMongoOperator.postOne(payload, RESOURCE_CAPABILITIES, Object.class);
            return true;
        } catch (Exception e) {
            logger.warn("Register agent throttle capabilities failed: {}", e.getMessage());
            return false;
        }
    }

    public boolean pullDeltaAndApply() {
        if (clientMongoOperator == null) {
            return false;
        }
        try {
            Map<String, Object> params = new HashMap<>(4);
            params.put("agentId", registry.getAgentId());
            params.put("cursor", registry.getLastDeltaCursor());
            AgentThrottleDeltaResponse delta = clientMongoOperator.findOne(params, RESOURCE_DELTA, AgentThrottleDeltaResponse.class);
            if (delta != null) {
                registry.applyDelta(delta);
            }
            return true;
        } catch (Exception e) {
            logger.warn("Pull agent throttle delta failed (keeping last applied snapshot): {}", e.getMessage());
            return false;
        }
    }

    public boolean flushPendingReceipts() {
        if (clientMongoOperator == null) {
            return false;
        }
        List<TaskThrottleApplyReceipt> receipts = registry.drainPendingReceipts();
        if (receipts.isEmpty()) {
            return true;
        }
        try {
            Map<String, Object> payload = new HashMap<>(4);
            payload.put("agentId", registry.getAgentId());
            payload.put("receipts", OBJECT_MAPPER.convertValue(receipts, Object.class));
            clientMongoOperator.postOne(payload, RESOURCE_RECEIPTS, Object.class);
            return true;
        } catch (Exception e) {
            logger.warn("Upload throttle apply receipts failed: {}", e.getMessage());
            return false;
        }
    }

    public boolean flushTelemetrySamples() {
        if (clientMongoOperator == null) {
            return false;
        }
        Map<String, List<TaskThrottleTelemetrySample>> telemetry = registry.snapshotTelemetryAll();
        if (telemetry.isEmpty()) {
            return true;
        }
        try {
            Map<String, Object> payload = new HashMap<>(4);
            payload.put("agentId", registry.getAgentId());
            payload.put("samplesByTask", OBJECT_MAPPER.convertValue(telemetry, Object.class));
            clientMongoOperator.postOne(payload, RESOURCE_TELEMETRY, Object.class);
            return true;
        } catch (Exception e) {
            logger.warn("Upload throttle telemetry samples failed: {}", e.getMessage());
            return false;
        }
    }

    public boolean reportStopEvidence(TaskThrottleStopEvidenceDto evidence) {
        if (evidence == null) {
            return true;
        }
        if (clientMongoOperator == null) {
            registry.requeueStopEvidence(evidence);
            return false;
        }
        try {
            Map<String, Object> payload = OBJECT_MAPPER.convertValue(evidence, MAP_TYPE);
            clientMongoOperator.postOne(payload, RESOURCE_STOP_EVIDENCE, Object.class);
            registry.removeStopEvidence(evidence);
            return true;
        } catch (Exception e) {
            logger.warn("Report throttle stop evidence failed for task {}: {}", evidence.getTaskId(), e.getMessage());
            registry.requeueStopEvidence(evidence);
            return false;
        }
    }

    public void flushPendingStopEvidences() {
        if (clientMongoOperator == null) {
            return;
        }
        List<TaskThrottleStopEvidenceDto> list = registry.drainPendingStopEvidences();
        for (TaskThrottleStopEvidenceDto evidence : list) {
            reportStopEvidence(evidence);
        }
    }
}
