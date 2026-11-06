package com.tapdata.tm.commons.task.dto.alarm;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AlarmReceiverScopeSelectionTest {
    @Test
    void inheritedSelectionIgnoresDisplaySnapshotsAndOrder() {
        List<AlarmReceiver> linked = List.of(
                new AlarmReceiver(AlarmReceiverType.USER_GROUP, "group-1", "old@example.com", "old name"),
                new AlarmReceiver(AlarmReceiverType.EMAIL, null, "OPS@example.com"));
        List<AlarmReceiver> draft = List.of(
                new AlarmReceiver(AlarmReceiverType.EMAIL, null, "ops@example.com"),
                new AlarmReceiver(AlarmReceiverType.USER_GROUP, "group-1", null));

        assertTrue(AlarmReceiverScope.sameSelection(draft, linked));
        assertFalse(AlarmReceiverScope.sameSelection(List.of(draft.get(1)), linked));
    }
}
