package com.tapdata.tm.task.service.impl;

import com.tapdata.tm.commons.alarm.Level;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.utils.MessageUtil;
import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskDagCheckLogServiceImplLocalizationTest {

    @Test
    void localizesNodeNameWhenCreateLogFormatsAnEnglishTemplate() {
        TaskDagCheckLogServiceImpl service = new TaskDagCheckLogServiceImpl();
        TaskDagCheckLog log = service.createLog(
                "task-id",
                "node-id",
                "user-id",
                Level.INFO,
                DagOutputTemplateEnum.SOURCE_SETTING_CHECK,
                MessageUtil.getDagCheckMsg(Locale.US, "SOURCE_SETTING_INFO"),
                "数据源节点");

        assertTrue(log.getLog().contains("Data source node"));
    }
}
