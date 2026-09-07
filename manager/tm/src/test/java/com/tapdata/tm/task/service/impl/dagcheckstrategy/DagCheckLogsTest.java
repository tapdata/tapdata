package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import com.tapdata.tm.commons.alarm.Level;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DagCheckLogsTest {

    @Test
    void ofFillsLogMetadataAndLocalizesDefaultNodeName() {
        Date now = new Date();

        TaskDagCheckLog log = DagCheckLogs.of(
                "task-id",
                "node-id",
                "user-id",
                now,
                Level.INFO,
                DagOutputTemplateEnum.SOURCE_SETTING_CHECK,
                Locale.US,
                "SOURCE_SETTING_INFO",
                "数据源节点");

        assertEquals("task-id", log.getTaskId());
        assertEquals("node-id", log.getNodeId());
        assertEquals("user-id", log.getCreateUser());
        assertEquals(now, log.getCreateAt());
        assertEquals(Level.INFO, log.getGrade());
        assertEquals(DagOutputTemplateEnum.SOURCE_SETTING_CHECK.name(), log.getCheckType());
        assertTrue(log.getLog().contains("Data source node"));
    }
}
