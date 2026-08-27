package com.tapdata.tm.init;

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class DqlAlarmTemplateTest {

    private static final Map<String, Set<String>> REQUIRED_PARAMETERS = Map.of(
            "TASK_DQL_EVENT", Set.of("taskName", "eventId", "sourceTable", "targetTable", "dmlType",
                    "errorType", "errorCode", "failedAt", "pageUrl", "alarmDate"),
            "TASK_DQL_SAVE_FAILED", Set.of("taskName", "taskId", "eventId", "errorCode", "errorMessage",
                    "failedAt", "pageUrl", "alarmDate"),
            "TASK_DQL_RECOVERY_FAILED", Set.of("taskName", "taskId", "batchId", "operatorName", "recoveryStatus",
                    "successCount", "failedCount", "skippedCount", "failedAt", "pageUrl", "alarmDate"),
            "TASK_DQL_STORM_GUARD", Set.of("taskName", "taskId", "guardKey", "guardWindowSeconds",
                    "guardThreshold", "suppressedCountEstimate", "routeDecision", "safeReason", "pageUrl", "alarmDate"));

    private static final Pattern PARAMETER = Pattern.compile("#\\{\\[([^]]+)]}");

    @Test
    void allSupportedLanguagesContainSafeDqlTemplates() throws Exception {
        for (String language : new String[]{"zh_CN", "zh_TW", "en_US"}) {
            Properties templates = load(language);
            for (Map.Entry<String, Set<String>> entry : REQUIRED_PARAMETERS.entrySet()) {
                String template = templates.getProperty(entry.getKey());
                assertNotNull(template, language + " is missing " + entry.getKey());
                assertEquals(entry.getValue(), parameters(template), language + " parameters for " + entry.getKey());
                assertFalse(template.contains("payload_data"), language + " exposes payload_data");
                assertFalse(template.contains("recordIdentity"), language + " exposes recordIdentity");
                assertFalse(template.contains("stackTrace"), language + " exposes stackTrace");
            }
        }
    }

    private Properties load(String language) throws Exception {
        String resource = "alarmTemplate_" + language + ".properties";
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream(resource)) {
            assertNotNull(stream, resource);
            Properties properties = new Properties();
            properties.load(new InputStreamReader(stream, StandardCharsets.UTF_8));
            return properties;
        }
    }

    private Set<String> parameters(String template) {
        return PARAMETER.matcher(template).results().map(match -> match.group(1)).collect(java.util.stream.Collectors.toSet());
    }
}
