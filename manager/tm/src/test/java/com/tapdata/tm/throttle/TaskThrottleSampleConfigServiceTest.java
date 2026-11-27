package com.tapdata.tm.throttle;

import com.tapdata.tm.Settings.dto.SettingsDto;
import com.tapdata.tm.Settings.service.SettingsServiceImpl;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.throttle.AgentThrottleCapabilities;
import com.tapdata.tm.commons.throttle.ThrottleConstants;
import com.tapdata.tm.throttle.dto.TaskThrottleSampleConfigView;
import com.tapdata.tm.throttle.service.TaskThrottleExecutionGuardService;
import com.tapdata.tm.throttle.service.TaskThrottleSampleConfigService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TaskThrottleSampleConfigServiceTest {

    private InMemoryMongoOperations mongo;
    private TaskThrottleSampleConfigService sampleConfigService;
    private TaskThrottleExecutionGuardService guardService;

    @BeforeEach
    void setUp() {
        mongo = new InMemoryMongoOperations();
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1_700_000_000_000L), ZoneOffset.UTC);
        sampleConfigService = new TaskThrottleSampleConfigService(mongo.ops(), clock);
        guardService = new TaskThrottleExecutionGuardService(mongo.ops(), clock);
    }

    @Test
    void defaultsTo30sAndUpdatesValidRangeWithMonotonicVersion() {
        TaskThrottleSampleConfigView initial = sampleConfigService.getConfigView();
        assertEquals(ThrottleConstants.SETTING_KEY_CDC_SAMPLE_REFRESH_INTERVAL, initial.getKey());
        assertEquals(30, initial.getIntervalSeconds());
        assertEquals(1L, initial.getConfigVersion());

        TaskThrottleSampleConfigView updated = sampleConfigService.updateInterval(5);
        assertEquals(5, updated.getIntervalSeconds());
        assertEquals(2L, updated.getConfigVersion());

        TaskThrottleSampleConfigView upper = sampleConfigService.updateInterval("300");
        assertEquals(300, upper.getIntervalSeconds());
        assertEquals(3L, upper.getConfigVersion());
    }

    @Test
    void rejectsNullOutOfRangeDecimalAndNonNumericInputs() {
        for (Object bad : new Object[]{null, 4, 301, 15.5d, "abc", "", "12.3"}) {
            BizException ex = assertThrows(BizException.class, () -> sampleConfigService.updateInterval(bad));
            assertEquals(ThrottleConstants.ERR_INVALID_RATE, ex.getErrorCode());
        }
        assertEquals(1L, sampleConfigService.getCurrentConfig().configVersion());
    }

    @Test
    void categorizesAgentRolloutStatusesIntoAppliedApplyingAndUnsupportedLegacy() {
        sampleConfigService.updateInterval(45); // configVersion=2, interval=45

        guardService.registerCapabilities(AgentThrottleCapabilities.builder()
                .agentId("agent-applied")
                .throttleSupported(true)
                .meterVersion(ThrottleConstants.METER_VERSION)
                .cdcSampleConfigVersion(2L)
                .cdcSampleRefreshIntervalSeconds(45)
                .build());
        guardService.registerCapabilities(AgentThrottleCapabilities.builder()
                .agentId("agent-applying")
                .throttleSupported(true)
                .meterVersion(ThrottleConstants.METER_VERSION)
                .cdcSampleConfigVersion(1L)
                .cdcSampleRefreshIntervalSeconds(30)
                .build());
        guardService.registerCapabilities(AgentThrottleCapabilities.builder()
                .agentId("agent-legacy")
                .throttleSupported(false)
                .meterVersion("v0")
                .build());

        TaskThrottleSampleConfigView view = sampleConfigService.getConfigView();
        Map<String, TaskThrottleSampleConfigView.AgentSampleStatus> byAgent = new java.util.HashMap<>();
        for (TaskThrottleSampleConfigView.AgentSampleStatus status : view.getAgentStatuses()) {
            byAgent.put(status.getAgentId(), status);
        }

        assertEquals(TaskThrottleSampleConfigView.STATUS_APPLIED, byAgent.get("agent-applied").getStatus());
        assertEquals(TaskThrottleSampleConfigView.STATUS_APPLYING, byAgent.get("agent-applying").getStatus());
        assertEquals(TaskThrottleSampleConfigView.STATUS_UNSUPPORTED_LEGACY, byAgent.get("agent-legacy").getStatus());
    }

    @Test
    void settingsServiceSaveHookValidatesAndAdvancesConfigVersion() {
        SettingsServiceImpl settingsService = new SettingsServiceImpl();
        MongoTemplate mongoTemplate = mock(MongoTemplate.class);
        BulkOperations bulkOps = mock(BulkOperations.class);
        when(mongoTemplate.bulkOps(eq(BulkOperations.BulkMode.UNORDERED), eq(com.tapdata.tm.Settings.entity.Settings.class)))
                .thenReturn(bulkOps);
        settingsService.setMongoTemplate(mongoTemplate);
        ReflectionTestUtils.setField(settingsService, "taskThrottleSampleConfigService", sampleConfigService);

        SettingsDto invalidDto = new SettingsDto();
        invalidDto.setId("s1");
        invalidDto.setCategory("Job");
        invalidDto.setKey(ThrottleConstants.SETTING_KEY_CDC_SAMPLE_REFRESH_INTERVAL);
        invalidDto.setValue(999);

        BizException ex = assertThrows(BizException.class, () -> settingsService.save(List.of(invalidDto)));
        assertEquals(ThrottleConstants.ERR_INVALID_RATE, ex.getErrorCode());
        verify(bulkOps, never()).execute();

        SettingsDto validDto = new SettingsDto();
        validDto.setId("s1");
        validDto.setCategory("Job");
        validDto.setKey(ThrottleConstants.SETTING_KEY_CDC_SAMPLE_REFRESH_INTERVAL);
        validDto.setValue("90");

        settingsService.save(List.of(validDto));
        verify(bulkOps).execute();
        TaskThrottleSampleConfigService.ConfigSnapshot snap = sampleConfigService.getCurrentConfig();
        assertEquals(90, snap.intervalSeconds());
        assertEquals(2L, snap.configVersion());
    }

    @Test
    void configVersionSurvivesServiceRestartMonotonicallyWithoutRegressing() {
        sampleConfigService.updateInterval(20); // v2
        sampleConfigService.updateInterval(40); // v3

        // Simulate TM restart by constructing a fresh service instance over the same Mongo store
        TaskThrottleSampleConfigService restarted = new TaskThrottleSampleConfigService(
                mongo.ops(), Clock.fixed(Instant.ofEpochMilli(1_700_000_010_000L), ZoneOffset.UTC));
        TaskThrottleSampleConfigService.ConfigSnapshot loaded = restarted.getCurrentConfig();
        assertEquals(40, loaded.intervalSeconds());
        assertEquals(3L, loaded.configVersion());

        TaskThrottleSampleConfigView nextUpdate = restarted.updateInterval(60);
        assertEquals(60, nextUpdate.getIntervalSeconds());
        assertEquals(4L, nextUpdate.getConfigVersion());
    }
}
