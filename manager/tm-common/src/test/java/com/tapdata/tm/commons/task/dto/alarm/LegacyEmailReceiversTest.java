package com.tapdata.tm.commons.task.dto.alarm;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LegacyEmailReceiversTest {

    @Test
    void batchNonEmptyBecomesEmailReplace() {
        BatchUpdateAlarmParam param = new BatchUpdateAlarmParam();
        param.setEmailReceivers(Arrays.asList(" a@example.com ", null, " "));

        LegacyEmailReceivers.normalize(param);

        assertEquals(List.of(new AlarmReceiver(AlarmReceiverType.EMAIL, null, "a@example.com")), param.getAlarmReceivers());
        assertEquals(ReceiverBatchMode.REPLACE, param.getReceiverMode());
        assertNull(param.getUseSystemDefaultReceivers());
    }

    @Test
    void batchEmptyRestoresSystemDefault() {
        BatchUpdateAlarmParam param = new BatchUpdateAlarmParam();
        param.setEmailReceivers(List.of(" "));

        LegacyEmailReceivers.normalize(param);

        assertTrue(param.getUseSystemDefaultReceivers());
        assertNull(param.getAlarmReceivers());
    }

    @Test
    void batchWithNewFieldsIsUntouched() {
        BatchUpdateAlarmParam param = new BatchUpdateAlarmParam();
        List<AlarmReceiver> receivers = List.of(new AlarmReceiver(AlarmReceiverType.USER, "u1", null));
        param.setAlarmReceivers(receivers);
        param.setEmailReceivers(List.of());

        LegacyEmailReceivers.normalize(param);

        assertSame(receivers, param.getAlarmReceivers());
        assertNull(param.getUseSystemDefaultReceivers());
        LegacyEmailReceivers.normalize((BatchUpdateAlarmParam) null);
    }

    @Test
    void singleNonEmptyBecomesEmailReceivers() {
        AlarmVO alarm = new AlarmVO();
        alarm.setEmailReceivers(List.of("new@example.com"));

        assertTrue(LegacyEmailReceivers.needsNormalize(alarm));
        LegacyEmailReceivers.normalize(alarm, List.of("old@example.com"));

        assertEquals(List.of(new AlarmReceiver(AlarmReceiverType.EMAIL, null, "new@example.com")), alarm.getAlarmReceivers());
    }

    @Test
    void singleUnchangedSnapshotKeepsCurrentReceivers() {
        AlarmVO alarm = new AlarmVO();
        alarm.setEmailReceivers(List.of("A@example.com", "b@example.com"));

        LegacyEmailReceivers.normalize(alarm, List.of("b@example.com", "a@example.com"));

        assertNull(alarm.getAlarmReceivers());
    }

    @Test
    void singleEmptyDoesNotChangeReceivers() {
        AlarmVO alarm = new AlarmVO();
        alarm.setEmailReceivers(List.of());

        LegacyEmailReceivers.normalize(alarm, null);

        assertNull(alarm.getAlarmReceivers());
        assertNull(alarm.getUseSystemDefaultReceivers());
    }

    @Test
    void singleSkipsNodeLevelAndNewFields() {
        AlarmVO node = new AlarmVO();
        node.setNodeId("n1");
        node.setEmailReceivers(List.of("a@example.com"));
        assertFalse(LegacyEmailReceivers.needsNormalize(node));

        AlarmVO reset = new AlarmVO();
        reset.setUseSystemDefaultReceivers(Boolean.TRUE);
        reset.setEmailReceivers(List.of("a@example.com"));
        assertFalse(LegacyEmailReceivers.needsNormalize(reset));

        AlarmVO noLegacy = new AlarmVO();
        assertFalse(LegacyEmailReceivers.needsNormalize(noLegacy));
        assertFalse(LegacyEmailReceivers.needsNormalize(null));
        LegacyEmailReceivers.normalize(noLegacy, null);
        assertNull(noLegacy.getAlarmReceivers());
    }
}
