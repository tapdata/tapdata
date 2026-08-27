package com.tapdata.tm.commons.task.constant;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DqlAlarmKeyTest {

    @Test
    void dqlAlarmKeysRemainAtTheEndOfTaskAlarmSchema() {
        List<AlarmKeyEnum> keys = List.of(
                AlarmKeyEnum.TASK_DQL_EVENT,
                AlarmKeyEnum.TASK_DQL_SAVE_FAILED,
                AlarmKeyEnum.TASK_DQL_RECOVERY_FAILED,
                AlarmKeyEnum.TASK_DQL_STORM_GUARD);

        assertEquals(AlarmKeyEnum.Constant.TYPE_EVENT, AlarmKeyEnum.TASK_DQL_EVENT.getType());
        assertTrue(keys.stream().allMatch(key -> AlarmKeyEnum.Constant.TYPE_EVENT.equals(key.getType())));

        List<String> taskAlarmKeys = AlarmKeyEnum.getTaskAlarmKeys();
        assertEquals(keys.stream().map(Enum::name).toList(),
                taskAlarmKeys.subList(taskAlarmKeys.size() - keys.size(), taskAlarmKeys.size()));
    }
}
